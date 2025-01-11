from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveriyLoader:
    WF_KEY = "dds_deliveries"
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_deliveries(self):
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
                                ,(oo.object_value::jsonb->>'courier_id') AS courier_id
                                ,(oo.object_value::jsonb->>'delivery_id') AS delivery_id
                                ,(oo.object_value::jsonb->>'delivery_ts')::timestamp AS delivery_ts
                            FROM stg.deliverysystem_deliveries oo
                            WHERE oo.update_ts >= (SELECT ts FROM get_ts)
                            ORDER BY oo.update_ts
                            LIMIT %(limit)s
                            )
                        ,src_refer AS 
                            (
                            SELECT 
                                c.id AS courier_id	
                                ,dt.id AS dt_id
                                ,s.delivery_id AS delivery_id
                            FROM src s
                            JOIN dds.dm_couriers c 
                                ON c.courier_id = s.courier_id
                                AND s.delivery_ts between c.active_from AND c.active_to
                            JOIN dds.dm_timestamps dt 
                                ON dt.ts = s.delivery_ts
                            )
                        , ins AS 
                            (
                            INSERT INTO dds.dm_deliveries (courier_id,timestamp_id,delivery_key)
                            SELECT courier_id,dt_id,delivery_id
                            FROM src_refer
                            ON CONFLICT (delivery_key)
                            DO NOTHING
                            RETURNING courier_id
                            )
                        ,upd_ts AS 
                            (
                                UPDATE dds.srv_wf_settings SET 		
                                    workflow_settings = s.w_settings
                                FROM (SELECT
                                            %(WF_KEY)s AS w_key
                                            ,jsonb_build_object(%(LAST_LOADED_KEY)s,max(delivery_ts)) AS w_settings 
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
                                            ,jsonb_build_object(%(LAST_LOADED_KEY)s,max(delivery_ts)) AS w_settings 
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
