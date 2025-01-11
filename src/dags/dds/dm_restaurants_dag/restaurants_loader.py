from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RestaurantLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_restaurants(self):
        # запрос работает в одной транзакции.
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH 
                        src AS 
                            (
                                SELECT sr.object_id
                                    ,sr.object_value::jsonb ->> 'name' AS restaurant_name
                                    ,sr.update_ts
                                    ,LEAST(update_ts,(SELECT min((oo.object_value::jsonb->>'date')::timestamp) FROM stg.ordersystem_orders oo where (oo.object_value::jsonb->'restaurant')->>'id'=sr.object_id)) AS min_update_ts
                                    ,dr.restaurant_id AS curr_object_id
                                    ,dr.restaurant_name AS curr_restaurant_name
                                    ,(dr.restaurant_name!=sr.object_value::jsonb ->> 'name') is_need_upd
                                FROM stg.ordersystem_restaurants sr
                                LEFT JOIN dds.dm_restaurants dr 
                                	ON sr.object_id=dr.restaurant_id
                                    AND dr.active_to >= sr.update_ts
                                	AND dr.active_from <= sr.update_ts
                            )
                        ,upd_exists AS 
                            (
                                UPDATE dds.dm_restaurants SET 		
                                    active_to = update_ts
                                FROM src s
                                WHERE s.object_id=restaurant_id
                                	AND s.is_need_upd IS TRUE
                                RETURNING s.object_id
                            )
                        ,ins_exists AS 
                            (
                                INSERT INTO dds.dm_restaurants(restaurant_id,restaurant_name,active_from,active_to)
                                SELECT 
                                    s.object_id
                                    ,s.restaurant_name
                                    ,s.update_ts
                                    ,'2099-12-31 00:00:00.000'
                                FROM src s
                                WHERE s.curr_object_id IS NOT NULL
                                	AND s.is_need_upd IS TRUE
                                RETURNING restaurant_id
                            )
                        ,ins_new AS 
                            (
                                INSERT INTO dds.dm_restaurants(restaurant_id,restaurant_name,active_from,active_to)
                                SELECT 
                                    s.object_id
                                    ,s.restaurant_name
                                    ,s.min_update_ts
                                    ,'2099-12-31 00:00:00.000'
                                FROM src s
                                WHERE s.curr_object_id IS NULL
                                RETURNING restaurant_id
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
