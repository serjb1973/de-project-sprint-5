import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.order_system_users_dag.pg_saver import PgSaverOrdersystemUsers
from stg.order_system_users_dag.user_loader import UserLoader
from stg.order_system_users_dag.user_reader import UserReader
from stg.order_system_restaurants_dag.pg_saver import PgSaverRestaurantLoader
from stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader
from stg.order_system_orders_dag.pg_saver import PgSaverOrderReader
from stg.order_system_orders_dag.order_loader import OrderLoader
from stg.order_system_orders_dag.order_reader import OrderReader
from lib import ConnectionBuilder, MongoConnect
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_from_mongo():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    tstart = EmptyOperator(task_id="start")
    tend = EmptyOperator(task_id="end")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverOrdersystemUsers()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UserReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverRestaurantLoader()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverOrderReader()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrderReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    # Создание экземпляра task-и.
    user_loader = load_users()
    restaurant_loader = load_restaurants()
    order_loader = load_orders()


    # Задаем порядок выполнения.
    tstart >> [ user_loader, restaurant_loader, order_loader ] >> tend


order_stg_dag = sprint5_stg_from_mongo()  # noqa
