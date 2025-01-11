import logging

import pendulum
from airflow.decorators import dag, task
from stg.bonus_system_events_dag.events_loader import EventLoader
from stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from stg.bonus_system_users_dag.users_loader import UserLoader
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
def sprint5_stg_from_postgres():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    tstart = EmptyOperator(task_id="start")
    tend = EmptyOperator(task_id="end")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="events_load")
    def load_events():
        # создаем экземпляр класса, в котором реализована логика.
        loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_events()  # Вызываем функцию, которая перельет данные.

    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_users()  # Вызываем функцию, которая перельет данные.


    # Инициализируем объявленные таски.
    events_dict = load_events()
    ranks_dict = load_ranks()
    users_dict = load_users()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    tstart >> [ events_dict, ranks_dict, users_dict ] >> tend


stg_from_postgres = sprint5_stg_from_postgres()
