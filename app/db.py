import os

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv


load_dotenv()


def get_db():

    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        row_factory=dict_row
    ) as connection:

        yield connection