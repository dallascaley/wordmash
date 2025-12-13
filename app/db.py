import os
import pymysql

def get_conn():
    return pymysql.connect(
            host=os.environ["DB_HOST"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ.get("DB_NAME", "wordmash"),
            cursorclass=pymysql.cursors.DictCursor
        )
