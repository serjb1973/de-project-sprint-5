from logging import Logger
from typing import List
from datetime import datetime

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from lib.ya_rest_connect import YacloudRestReader
from psycopg import Connection
from psycopg.rows import class_row
import json


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(object_id, object_value,update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id,update_ts) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "object_id": delivery["delivery_id"],
                    "object_value": str(json.dumps(delivery, ensure_ascii=False)),
                    "update_ts": delivery["order_ts"]
                },
            )


class DeliveryLoader:
    BATCH_LIMIT = 50
    OBJ_NAME = "deliveries"
    WF_KEY = "deliveries_origin_to_stg"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = YacloudRestReader(self.OBJ_NAME)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")
            # Вычитываем очередную пачку объектов.
            offset = 0
            limit = self.BATCH_LIMIT
            count = 1
            while (count > 0):
                params = {'sort_field': 'date', 'sort_direction': 'asc', 'limit': limit, 'offset' : offset, 'from' : last_loaded_ts.strftime('%Y-%m-%d %H:%M:%S')}
                load_queue = self.origin.get_json(params=params)
                offset += self.origin.count
                count = self.origin.count
                if offset == 0:
                    self.log.info("Quitting.")
                    return
                # Сохраняем объекты в базу dwh.
                for delivery in load_queue:
                    self.stg.insert_delivery(conn, delivery)
                # Сохраняем последнее значение order_ts
                if self.origin.count > 0:
                    last_ts = datetime.fromisoformat(load_queue[-1]['order_ts'])
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_ts
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
            self.log.info(f"Load {offset} rows")
