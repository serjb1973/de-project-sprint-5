from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SettlementReportLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_settlement_report(self):
        # запрос работает в одной транзакции.
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                   """
                    WITH ins AS (
                    INSERT INTO cdm.dm_settlement_report
                        (restaurant_id
                        , restaurant_name
                        , settlement_date
                        , orders_count
                        , orders_total_sum
                        , orders_bonus_payment_sum
                        , orders_bonus_granted_sum
                        , order_processing_fee
                        , restaurant_reward_sum)
                    SELECT dm_res.restaurant_id
                        , dm_res.restaurant_name
                        , dm_t."date" AS settlement_date
                        , count(DISTINCT dm_o.id) AS orders_count
                        , sum(fct_ps.total_sum) AS orders_total_sum
                        , sum(fct_ps.bonus_payment) AS orders_bonus_payment_sum
                        , sum(fct_ps.bonus_grant) AS orders_bonus_granted_sum
                        , sum(fct_ps.total_sum*0.25) AS order_processing_fee
                        , sum(fct_ps.total_sum-fct_ps.total_sum*0.25-fct_ps.bonus_payment) AS restaurant_reward_sum
                    FROM dds.fct_product_sales fct_ps
                    JOIN dds.dm_orders dm_o ON dm_o.id = fct_ps.order_id
                    JOIN dds.dm_products dm_p ON dm_p.id = fct_ps.product_id
                    JOIN dds.dm_restaurants dm_res ON dm_res.id = dm_o.restaurant_id
                    JOIN dds.dm_timestamps dm_t ON dm_t.id = dm_o.timestamp_id
                    WHERE dm_o.order_status ='CLOSED'
                    GROUP BY dm_res.restaurant_id, dm_res.restaurant_name, dm_t."date"
                    ON CONFLICT (restaurant_id, settlement_date)
                    DO UPDATE 
                    SET 
                        restaurant_name=EXCLUDED.restaurant_name
                        ,orders_count=EXCLUDED.orders_count
                        ,orders_total_sum=EXCLUDED.orders_total_sum
                        ,orders_bonus_payment_sum=EXCLUDED.orders_bonus_payment_sum
                        ,orders_bonus_granted_sum=EXCLUDED.orders_bonus_granted_sum
                        ,order_processing_fee=EXCLUDED.order_processing_fee
                        ,restaurant_reward_sum=EXCLUDED.restaurant_reward_sum
                    RETURNING id)
                    SELECT count(*) FROM ins
                    ;
                    """,
                )
                res = cur.fetchone()
                # Сохраняем прогресс.
                if res[0]==0:
                        self.log.info("Quitting.")
                        return
            self.log.info(f"Load {res[0]} rows")
            conn.commit()
