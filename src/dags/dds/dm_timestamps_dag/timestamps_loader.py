from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestampLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_timestamps(self):
        # запрос работает в одной транзакции.
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH ts AS 
                        (
                        SELECT (oo.object_value::jsonb->>'date')::timestamp AS dt
                        FROM stg.ordersystem_orders oo 
                        WHERE oo.object_value::jsonb->>'final_status' IN ('CLOSED','CANCELLED')
                        UNION
                        SELECT (oo.object_value::jsonb->>'delivery_ts')::timestamp AS dt
                        FROM stg.deliverysystem_deliveries oo 
                        )
                    ,ins AS
                        (
                        INSERT INTO dds.dm_timestamps(ts,"year","month","day","date","time")
                        SELECT t.dt AS fact_date
                            ,to_char(t.dt,'YYYY')::int as year_num
                            ,to_char(t.dt,'MM')::int as month_num
                            ,to_char(t.dt,'DD')::int as day_num
                            ,t.dt::date AS ddate
                            ,t.dt::time AS ttime
                        FROM ts t
                        ON CONFLICT (ts) DO NOTHING
                        RETURNING id
                        )
                    SELECT count(*) FROM ins;
                    """,
                )
                res = cur.fetchone()
                # Сохраняем прогресс.
                if res[0]==0:
                        self.log.info("Quitting.")
                        return
            self.log.info(f"Load finished on {res[0]}")
            conn.commit()
