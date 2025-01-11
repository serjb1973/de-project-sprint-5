from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierLedgerReportLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_courier_ledger_report(self):
        # запрос работает в одной транзакции.
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                   """
                   WITH orders AS (
                        SELECT 
                            fps.order_id 
                            ,dt."year" AS settlement_year
                            ,dt."month" AS settlement_month
                            ,sum(fps.total_sum) AS orders_total_sum
                        FROM dds.fct_product_sales fps
                        JOIN dds.dm_orders do2 ON fps.order_id = do2.id 
                        JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id 
                        GROUP BY fps.order_id, dt."year",dt."month"
                        )
                    , raw AS (
                        SELECT 
                            dc.courier_id 
                            ,dc.courier_name 
                            ,o.settlement_year
                            ,o.settlement_month
                            ,fod.order_id
                            ,fod.delivery_id
                            ,o.orders_total_sum
                            ,avg(fod.rate) OVER (PARTITION BY dc.id,o.settlement_year,o.settlement_month) AS rate_avg
                            ,o.orders_total_sum * 0.25 AS order_processing_fee
                            ,fod.tip_sum AS courier_tips_sum
                        FROM dds.fct_order_deliveries fod 
                        JOIN orders o ON fod.order_id = o.order_id 
                        JOIN dds.dm_deliveries dd ON dd.id = fod.delivery_id 
                        JOIN dds.dm_timestamps dt ON dt.id = dd.timestamp_id 
                        JOIN dds.dm_couriers dc ON dc.id = dd.courier_id AND dt.ts BETWEEN dc.active_from AND dc.active_to 
                        )
                    ,ins AS (
                        INSERT INTO cdm.dm_courier_ledger
                            (courier_id
                            , courier_name
                            , settlement_year
                            , settlement_month
                            , orders_count
                            , orders_total_sum
                            , rate_avg
                            , order_processing_fee
                            , courier_order_sum
                            , courier_tips_sum
                            , courier_reward_sum)
                        SELECT 
                            courier_id
                            ,courier_name
                            ,settlement_year
                            ,settlement_month
                            ,count(*) AS orders_count
                            ,sum(orders_total_sum) AS orders_total_sum
                            ,rate_avg
                            ,sum(order_processing_fee) AS order_processing_fee
                            ,sum((CASE 
                                    WHEN rate_avg>=4.9 
                                        THEN GREATEST(0.1*orders_total_sum,200)
                                    WHEN rate_avg>=4.5 AND rate_avg<4.9 	
                                        THEN GREATEST(0.08*orders_total_sum,175)
                                    WHEN rate_avg>=4 AND rate_avg<4.5 	
                                        THEN GREATEST(0.07*orders_total_sum,150)
                                    WHEN rate_avg<4 	
                                        THEN GREATEST(0.05*orders_total_sum,100)
                                    ELSE 0 END)) AS courier_order_sum
                            ,sum(courier_tips_sum) AS courier_tips_sum
                            ,sum(courier_tips_sum * 0.95 +
                                (CASE 
                                    WHEN rate_avg>=4.9 
                                        THEN GREATEST(0.1*orders_total_sum,200)
                                    WHEN rate_avg>=4.5 AND rate_avg<4.9 	
                                        THEN GREATEST(0.08*orders_total_sum,175)
                                    WHEN rate_avg>=4 AND rate_avg<4.5 	
                                        THEN GREATEST(0.07*orders_total_sum,150)
                                    WHEN rate_avg<4 	
                                        THEN GREATEST(0.05*orders_total_sum,100)
                                    ELSE 0 END)
                                ) AS courier_reward_sum
                        FROM raw
                        GROUP BY courier_id,courier_name,settlement_year,settlement_month,rate_avg
                        ON CONFLICT (courier_id, settlement_year, settlement_month)
                        DO UPDATE 
                        SET 
                            courier_name=EXCLUDED.courier_name
                            ,orders_count=EXCLUDED.orders_count
                            ,orders_total_sum=EXCLUDED.orders_total_sum
                            ,rate_avg=EXCLUDED.rate_avg
                            ,order_processing_fee=EXCLUDED.order_processing_fee
                            ,courier_order_sum=EXCLUDED.courier_order_sum
                            ,courier_tips_sum=EXCLUDED.courier_tips_sum
                            ,courier_reward_sum=EXCLUDED.courier_reward_sum
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
