import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from dds.dm_users_dag.users_loader import UserLoader
from dds.dm_restaurants_dag.restaurants_loader import RestaurantLoader
from dds.dm_timestamps_dag.timestamps_loader import TimestampLoader
from dds.dm_products_dag.products_loader import ProductLoader
from dds.dm_orders_dag.orders_loader import OrderLoader
from dds.dm_couriers_dag.couriers_loader import CourierLoader
from dds.dm_deliveries_dag.deliveries_loader import DeliveriyLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_dm():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    tstart = EmptyOperator(task_id="start")
    tend = EmptyOperator(task_id="end")
    tchk = EmptyOperator(task_id="checkpoint")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        loader = UserLoader(dwh_pg_connect, log)
        loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        loader = RestaurantLoader(dwh_pg_connect, log)
        loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        loader = TimestampLoader(dwh_pg_connect, log)
        loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        loader = ProductLoader(dwh_pg_connect, log)
        loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        loader = OrderLoader(dwh_pg_connect, log)
        loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="couriers_loader")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        loader = CourierLoader(dwh_pg_connect, log)
        loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="deliveries_loader")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        loader = DeliveriyLoader(dwh_pg_connect, log)
        loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    couriers_dict = load_couriers()
    deliveries_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    tstart >> [ users_dict, restaurants_dict, timestamps_dict, couriers_dict ] >> tchk >> [ products_dict, orders_dict, deliveries_dict ] >> tend


dds_dag = sprint5_dds_dm()
