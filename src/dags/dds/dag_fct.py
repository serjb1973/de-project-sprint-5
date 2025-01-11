import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from dds.fct_product_sales_dag.product_sales_loader import ProductSalesLoader
from dds.fct_order_deliveries_dag.order_deliveries_loader import OrderDeliveriesLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds','fact', 'origin', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_fact():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    tstart = EmptyOperator(task_id="start")
    tend = EmptyOperator(task_id="end")

    # Объявляем таск, который загружает данные.
    @task(task_id="product_sales")
    def load_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        loader = ProductSalesLoader(dwh_pg_connect, log)
        loader.load_product_sales()  # Вызываем функцию, которая перельет данные.

    @task(task_id="order_deliveries")
    def load_order_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        loader = OrderDeliveriesLoader(dwh_pg_connect, log)
        loader.load_order_deliveries()  # Вызываем функцию, которая перельет данные.


    # Инициализируем объявленные таски.
    product_sales_dict = load_product_sales()
    order_deliveries_dict = load_order_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    tstart >> [ product_sales_dict, order_deliveries_dict ] >> tend


dds_dag = sprint5_dds_fact()
