import logging

import pendulum
from airflow.decorators import dag, task
from stg.ya_system_couriers_dag.couriers_loader import CourierLoader
from stg.ya_system_deliveries_dag.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_from_rest():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    tstart = EmptyOperator(task_id="start")
    tend = EmptyOperator(task_id="end")

    # Объявляем таск, который загружает данные.
    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        loader = DeliveryLoader(dwh_pg_connect, log)
        loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        loader = CourierLoader(dwh_pg_connect, log)
        loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    deliveries_dict = load_deliveries()
    couriers_dict = load_couriers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    tstart >> [ couriers_dict, deliveries_dict ] >> tend


stg_from_rest = sprint5_stg_from_rest()
