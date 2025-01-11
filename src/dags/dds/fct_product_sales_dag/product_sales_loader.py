from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductSalesLoader:
    WF_KEY = "dds_product_sales"
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_product_sales(self):
        # запрос работает в одной транзакции.
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                     WITH
                        get_ts as	
                            (
                                SELECT COALESCE((
                                        SELECT (sws.workflow_settings->>%(LAST_LOADED_KEY)s)::timestamp
                                        FROM dds.srv_wf_settings sws 
                                        WHERE sws.workflow_key = %(WF_KEY)s),'-infinity'::timestamp) AS ts
                            )
                        ,src AS 
                            (
                                SELECT
									oot.id AS prod_id
									,oot.name AS prod_name
									,oo.object_id AS order_id
									,oot.quantity
									,oot.price
									,oot.quantity*oot.price AS total_sum
									,(oo.object_value::jsonb->'restaurant')->>'id' AS rest_id
									,bet.bonus_payment
									,bet.bonus_grant
									,(oo.object_value::jsonb->>'date')::timestamp AS update_ts
								FROM stg.ordersystem_orders oo
								CROSS JOIN jsonb_to_recordset(oo.object_value::jsonb -> 'order_items') AS oot(id TEXT,name TEXT,price NUMERIC,quantity NUMERIC)
								JOIN stg.bonussystem_events be
									ON be.event_value::jsonb->>'order_id' = oo.object_id  AND be.event_type = 'bonus_transaction'
								CROSS JOIN jsonb_to_recordset(be.event_value::jsonb -> 'product_payments') AS bet(product_id TEXT,bonus_payment NUMERIC(19,5),bonus_grant NUMERIC(19,5))
								WHERE bet.product_id = oot.id 
									AND oo.update_ts >= (SELECT ts FROM get_ts)
									AND be.event_ts  >= (SELECT ts FROM get_ts)
									AND (oo.object_value::jsonb->>'date') = (be.event_value::jsonb->>'order_date')
								ORDER BY oo.update_ts 
								LIMIT %(limit)s
                            )
                        ,src_refer AS 
                            (
                                SELECT
									dp.id AS prod_id
									,o.id AS order_id
									,s.quantity
									,s.price
									,s.total_sum
									,s.bonus_payment
									,s.bonus_grant
								FROM src s
								JOIN dds.dm_products dp
									ON s.prod_id=dp.product_id 
									AND s.update_ts BETWEEN dp.active_from AND dp.active_to
								JOIN dds.dm_restaurants dr 
									ON dr.restaurant_id=s.rest_id
								JOIN dds.dm_orders o
									ON s.order_id=o.order_key
								JOIN dds.dm_timestamps dt 
									ON dt.id=o.timestamp_id 
								WHERE s.update_ts = dt.ts
									AND dr.id=dp.restaurant_id
									
                            )
                        , ins AS 
                            (
                            INSERT INTO dds.fct_product_sales (product_id,order_id,count,price,total_sum,bonus_payment,bonus_grant)
                            SELECT prod_id,order_id,quantity,price,total_sum,bonus_payment,bonus_grant
                            FROM src_refer
                            ON CONFLICT (product_id,order_id)
                            DO NOTHING
                            RETURNING id
                            )
                        ,upd_ts AS 
                            (
                                UPDATE dds.srv_wf_settings SET 		
                                    workflow_settings = s.w_settings
                                FROM (SELECT
                                            %(WF_KEY)s AS w_key
                                            ,jsonb_build_object(%(LAST_LOADED_KEY)s,max(update_ts)) AS w_settings 
                                        FROM src) s
                                WHERE workflow_key=s.w_key
                                RETURNING s.w_key
                            )
                        ,ins_ts AS 
                            (
                                INSERT INTO dds.srv_wf_settings(workflow_key,workflow_settings)
                                SELECT 
                                    s.w_key
                                    ,s.w_settings 
                                FROM (SELECT
                                            %(WF_KEY)s AS w_key
                                            ,jsonb_build_object(%(LAST_LOADED_KEY)s,max(update_ts)) AS w_settings 
                                        FROM src) s
                                LEFT JOIN upd_ts u
                                    ON s.w_key=u.w_key
                                WHERE u.w_key IS NULL
                            )
                        SELECT 
                        (SELECT count(*) FROM src) AS src
                        ,(SELECT count(*) FROM ins) AS ins;
                    """,
                    {
                        "limit": self.BATCH_LIMIT,
                        "WF_KEY": self.WF_KEY,
                        "LAST_LOADED_KEY": self.LAST_LOADED_KEY,

                    },
                )
                res = cur.fetchone()
                # Сохраняем прогресс.
                if res[0]==0:
                        self.log.info("Quitting.")
                        return
            self.log.info(f"Load {res[0]}, inserted {res[1]} rows")
            conn.commit()
