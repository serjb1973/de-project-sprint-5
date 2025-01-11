from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserLoader:
    WF_KEY = "dds_users"
    LAST_LOADED_KEY = "last_loaded_ts"
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_users(self):
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
                                SELECT ou.object_id 
                                    ,ou.object_value::jsonb ->> 'login' AS login
                                    ,ou.object_value::jsonb ->> 'name' AS name
                                    ,ou.update_ts
                                    ,u.user_id as curr_user_id
                                    ,u.user_login as curr_login
                                    ,u.user_name as curr_name
                                    ,(u.user_login!=ou.object_value::jsonb ->> 'login' OR u.user_name!=ou.object_value::jsonb ->> 'name') as is_need_upd
                                FROM stg.ordersystem_users ou
                                LEFT JOIN dds.dm_users u
                                    ON ou.object_id=u.user_id
                                WHERE ou.update_ts >= (SELECT ts FROM get_ts)
                                ORDER BY ou.update_ts ASC
                                LIMIT %(limit)s
                            )
                        ,upd AS 
                            (
                                UPDATE dds.dm_users SET 		
                                    user_login = s.login, user_name = s.name
                                FROM src s
                                WHERE s.object_id=user_id
                                    AND s.is_need_upd is true
                                RETURNING s.object_id
                            )
                        ,ins AS 
                            (
                                INSERT INTO dds.dm_users(user_id,user_login,user_name)
                                SELECT 
                                    s.object_id
                                    ,s.login
                                    ,s.name
                                FROM src s
                                WHERE s.curr_user_id IS NULL
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
                        (SELECT count(*) FROM src) AS src
                        ,(SELECT count(*) FROM ins) AS ins
                        ,(SELECT count(*) FROM upd) AS upd;
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
            self.log.info(f"Load  {res[0]}, updated {res[2]}, inserted {res[1]} rows")
            conn.commit()
