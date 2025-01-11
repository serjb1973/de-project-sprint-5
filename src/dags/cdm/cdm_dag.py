import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from cdm.dm_settlement_report.settlement_report_loader import SettlementReportLoader
from cdm.dm_courier_ledger_report.courier_ledger_report_loader import CourierLedgerReportLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'origin', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_cdm():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    tstart = EmptyOperator(task_id="start")
    tend = EmptyOperator(task_id="end")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_settlement_report():
        # создаем экземпляр класса, в котором реализована логика.
        loader = SettlementReportLoader(dwh_pg_connect, log)
        loader.load_settlement_report()  # Вызываем функцию, которая перельет данные.

    @task(task_id="courier_ledger")
    def load_courier_ledger_report():
        # создаем экземпляр класса, в котором реализована логика.
        loader = CourierLedgerReportLoader(dwh_pg_connect, log)
        loader.load_courier_ledger_report()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    settlement_report = load_settlement_report()
    courier_ledger_report = load_courier_ledger_report()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    tstart >> [ settlement_report, courier_ledger_report ] >> tend


cdm_dm = sprint5_cdm()
