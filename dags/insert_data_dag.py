import json

from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from pendulum import datetime

from include.etl import insert_many


@dag(
    dag_id="load_data_to_mongodb",
    schedule=None,
    start_date=datetime(2022, 10, 28),
    catchup=False,
    default_args={
        "retries": 0,
    },
)
def dag_mongodb():

    @task(task_id="check-connection")
    def check_connection_task():
        hook = MongoHook(mongo_conn_id="mongo_default")
        with hook.get_conn() as client:
            _ = insert_many([{"zango": "dozo"}], client, "locations", "cards")
            print(f"Connected to MongoDB - {client.server_info()}")
            for db_info in client.list_database_names():
                print(db_info)
            collections = client.locations.list_collection_names()
            for collection in collections:
                print(collection)
            print(client.achats.cards)
            client.close()

    check_connection_task()


dag_mongodb()
