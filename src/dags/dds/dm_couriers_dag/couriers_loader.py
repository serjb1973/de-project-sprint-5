from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_couriers(self):
        # запрос работает в одной транзакции.
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH 
                        src AS 
                            (
                                SELECT sc.object_id
                                    ,sc.object_value::jsonb ->> 'name' AS courier_name
                                    ,dc.courier_id AS curr_object_id
                                    ,dc.courier_name AS curr_courier_name
                                    ,(dc.courier_name!=sc.object_value::jsonb ->> 'name') is_need_upd
                                FROM stg.deliverysystem_couriers sc
                                LEFT JOIN dds.dm_couriers dc  
                                	ON sc.object_id=dc.courier_id
                                	AND dc.active_to > now()
                                	AND dc.active_from < now()
                            )
                        ,upd_exists AS 
                            (
                                UPDATE dds.dm_couriers SET 		
                                    active_to = now()
                                FROM src s
                                WHERE s.object_id=courier_id
                                	AND s.is_need_upd IS TRUE
                                RETURNING id
                            )
                        ,ins_exists AS 
                            (
                                INSERT INTO dds.dm_couriers(courier_id,courier_name,active_from,active_to)
                                SELECT 
                                    s.object_id
                                    ,s.courier_name
                                    ,now()
                                    ,'2099-12-31 00:00:00.000'
                                FROM src s
                                WHERE s.curr_object_id IS NOT NULL
                                	AND s.is_need_upd IS TRUE
                                RETURNING id
                            )
                        ,ins_new AS 
                            (
                                INSERT INTO dds.dm_couriers(courier_id,courier_name,active_from,active_to)
                                SELECT 
                                    s.object_id
                                    ,s.courier_name
                                    ,'2022-01-01 00:00:00.000'
                                    ,'2099-12-31 00:00:00.000'
                                FROM src s
                                WHERE s.curr_object_id IS NULL
                                RETURNING id
                            )
                        SELECT 
                        (SELECT count(*) FROM src) AS src
                        ,(SELECT count(*) FROM upd_exists) AS upd_exists
                        ,(SELECT count(*) FROM ins_new) AS ins_new;
                    """,
                )
                res = cur.fetchone()
                # Сохраняем прогресс.
                if res[0]==0:
                        self.log.info("Quitting.")
                        return
            self.log.info(f"Load {res[0]}, updated {res[1]}, inserted {res[2]} rows")
            conn.commit()
