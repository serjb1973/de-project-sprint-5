from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderDeliveriesLoader:
    WF_KEY = "dds_order_deliveries"
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_order_deliveries(self):
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
									d.id as delivery_id
									,o.id as order_id
									,(dd.object_value::jsonb->>'tip_sum')::numeric AS tip_sum
									,(dd.object_value::jsonb->>'rate')::numeric AS rate
                                    ,dd.update_ts
								FROM stg.deliverysystem_deliveries dd
								JOIN dds.dm_deliveries d
									ON dd.object_id = d.delivery_key 
								JOIN dds.dm_orders o
									ON dd.object_value::jsonb->>'order_id' = o.order_key 
								WHERE dd.update_ts >= (SELECT ts FROM get_ts)
								ORDER BY dd.update_ts 
								LIMIT %(limit)s
                            )
                        , ins AS 
                            (
                            INSERT INTO dds.fct_order_deliveries (delivery_id,order_id,tip_sum,rate)
                            SELECT delivery_id,order_id,tip_sum,rate
                            FROM src
                            ON CONFLICT (delivery_id,order_id)
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
