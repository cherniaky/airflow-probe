from datetime import datetime, timedelta
import requests
import xmltodict
import os

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from airflow.providers.sqlite.operators.sqlite import SqliteOperator

with DAG(
    "podcasts",
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # create sqllite table 
    create_table = SqliteOperator(
        task_id='create_table',
        sql='CREATE TABLE IF NOT EXISTS podcasts (filename TEXT PRIMARY KEY, link TEXT, title TEXT)',
        sqlite_conn_id='podcasts',
    )

    PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

    @task()    
    def fetch_podcasts():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)
        episodes = feed['rss']['channel']['item']
        print(f"Found {len(episodes)} episodes")
        return episodes

    podcasts = fetch_podcasts()
    create_table.set_downstream(podcasts)

    @task()
    def insert_podcasts(episodes):
        hook = SqliteHook(sqlite_conn_id='podcasts')
        rows = []
        for episode in episodes:
            rows.append((f"{episode['link'].split('/')[-1]}.mp3", episode['link'], episode['title']))
        target_fields = ['filename', 'link', 'title']
        hook.insert_rows(table="podcasts", rows=rows, target_fields=target_fields, replace=True)

    insert_podcasts(podcasts)

    @task()
    def download_podcasts(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            response = requests.get(episode['link'])
            audio_path = os.path.join("podcasts", filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                with open(audio_path, "wb") as f:
                    f.write(response.content)

    download_podcasts(podcasts) 
