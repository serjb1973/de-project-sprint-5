from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderLoader:
    WF_KEY = "dds_orders"
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_orders(self):
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
                                object_id
                                ,(oo.object_value::jsonb->'user')->>'id' AS user_id
                                ,(oo.object_value::jsonb->'restaurant')->>'id' AS rest_id
                                ,(oo.object_value::jsonb->>'date')::timestamp AS dt
                                ,oo.object_value::jsonb->>'final_status' AS status
                                ,oo.update_ts 
                            FROM stg.ordersystem_orders oo
                            WHERE oo.update_ts >= (SELECT ts FROM get_ts)
                            ORDER BY oo.update_ts
                            LIMIT %(limit)s
                            )
                        ,src_refer AS 
                            (
                            SELECT 
                                du.id AS user_id
                                ,dr.id AS rest_id	
                                ,dt.id AS dt_id
                                ,s.object_id AS order_id
                                ,s.status as order_status
                            FROM src s
                            JOIN dds.dm_users du 
                                ON du.user_id = s.user_id
                            JOIN dds.dm_restaurants dr 
                                ON dr.restaurant_id = s.rest_id
                                AND s.dt between dr.active_from AND dr.active_to 
                            JOIN dds.dm_timestamps dt 
                                ON dt.ts = s.dt
                            )
                        , ins AS 
                            (
                            INSERT INTO dds.dm_orders (user_id,restaurant_id,timestamp_id,order_key,order_status)
                            SELECT user_id,rest_id,dt_id,order_id,order_status
                            FROM src_refer
                            ON CONFLICT (order_key)
                            DO UPDATE
                            SET order_status = EXCLUDED.order_status
                            RETURNING user_id
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
                        (SELECT count(*) FROM src_refer) AS src_refer
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
