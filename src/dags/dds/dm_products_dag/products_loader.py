from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductLoader:
    WF_KEY = "dds_products"
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_products(self):
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
									oo.id
                                    ,oo.update_ts
								FROM stg.ordersystem_orders oo
								WHERE oo.update_ts >= (SELECT ts FROM get_ts)
                                ORDER BY oo.update_ts ASC
                                LIMIT %(limit)s
                            )
                        ,src_rest AS 
                            (
                                SELECT
									t.id AS prod_id
									,t.name AS prod_name
									,t.price
									,r.id AS rest_id
									,min(oo.update_ts) min_update_ts
								FROM stg.ordersystem_orders oo
                                JOIN src s
                                    ON s.id = oo.id
								JOIN dds.dm_restaurants r 
									ON (oo.object_value::jsonb->'restaurant')->>'id' = r.restaurant_id
								CROSS JOIN jsonb_to_recordset(oo.object_value::jsonb -> 'order_items') AS t(id TEXT,name TEXT,price numeric)
								GROUP BY t.id,t.name,t.price,r.id
                            )
                        ,src_dest AS 
                            (
                                SELECT
									s.prod_id
									,s.prod_name
									,s.price
									,s.rest_id
									,s.min_update_ts
									,dp.product_id AS curr_prod_id
									,dp.product_name AS curr_prod_name
									,dp.product_price AS curr_price
									,(s.prod_name!=dp.product_name OR s.price!=dp.product_price) is_need_upd
								FROM src_rest s
								LEFT JOIN dds.dm_products dp
									ON s.prod_id=dp.product_id 
									AND s.rest_id=dp.restaurant_id
                                    AND dp.active_to >= s.min_update_ts
                                	AND dp.active_from <= s.min_update_ts
                            )
                        ,upd_exists AS 
                            (
                                UPDATE dds.dm_products SET 		
                                    active_to = s.min_update_ts
                                FROM src_dest s
                                WHERE s.prod_id=product_id
                                	AND s.is_need_upd IS true
                                RETURNING s.prod_id
                            )
                        ,ins_exists AS 
                            (
                                INSERT INTO dds.dm_products(restaurant_id,product_id,product_name,product_price,active_from,active_to)
                                SELECT 
                                    s.rest_id
                                    ,s.prod_id
                                    ,s.prod_name
                                    ,s.price
                                    ,s.min_update_ts
                                    ,'2099-12-31 00:00:00.000'
                                FROM src_dest s
                                WHERE s.curr_prod_id IS NOT NULL
                                	AND s.is_need_upd IS true
                                RETURNING restaurant_id
                            )
                        ,ins_new AS 
                            (
                                INSERT INTO dds.dm_products(restaurant_id,product_id,product_name,product_price,active_from,active_to)
                                SELECT 
                                    s.rest_id
                                    ,s.prod_id
                                    ,s.prod_name
                                    ,s.price
                                    ,'2022-01-01 00:00:00.000'
                                    ,'2099-12-31 00:00:00.000'
                                FROM src_dest s
                                WHERE s.curr_prod_id IS NULL
                                RETURNING restaurant_id
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
                        (SELECT count(*) FROM src_rest) AS src_rest
                        ,(SELECT count(*) FROM upd_exists) AS upd_exists
                        ,(SELECT count(*) FROM ins_new) AS ins_new;
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
            self.log.info(f"Load {res[0]}, updated {res[1]}, inserted {res[2]} rows")
            conn.commit()
