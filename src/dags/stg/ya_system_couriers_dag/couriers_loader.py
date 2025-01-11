from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from lib.ya_rest_connect import YacloudRestReader
from psycopg import Connection
from psycopg.rows import class_row
import json


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(object_id, object_value)
                    VALUES (%(object_id)s, %(object_value)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "object_id": courier["_id"],
                    "object_value": str(json.dumps(courier, ensure_ascii=False))

                },
            )


class CourierLoader:
    BATCH_LIMIT = 50
    OBJ_NAME = "couriers"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = YacloudRestReader(self.OBJ_NAME)
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Вычитываем очередную пачку объектов.
            offset = 0
            limit = self.BATCH_LIMIT
            count = 1
            while (count > 0):
                params = {'sort_field': 'id', 'sort_direction': 'asc', 'limit': limit, 'offset' : offset}
                load_queue = self.origin.get_json(params=params)
                offset += self.origin.count
                count = self.origin.count
                if offset == 0:
                    self.log.info("Quitting.")
                    return
                # Сохраняем объекты в базу dwh.
                for courier in load_queue:
                    self.stg.insert_courier(conn, courier)

            self.log.info(f"Load {offset} rows")
