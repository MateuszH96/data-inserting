from .DownloadList import downloadList
from configure import *
import pandas as pd
import os
import psycopg2 as db


def inserData():
    downloadList()
    cities = pd.read_csv(CSV_CORRECT, header=None, encoding='UTF-8')
    os.remove(CSV_FILENAME)
    os.remove(CSV_CORRECT)

    con = db.connect(host="195.150.230.208", port=5432, database="2022_hamera_mateusz",
                     user="2022_hamera_mateusz", password="BD2022/mh")
    cursor = con.cursor()
    QUERY_INSERT_CITIES = "INSERT INTO weather.city VALUES(%s,%s,%s)"
    count = 0
    print("Adding cities...")
    for i in cities.index:
        value_to_insert = (str(cities[0][i]), str(
            cities[3][i]), str(cities[2][i]))
        cursor.execute(QUERY_INSERT_CITIES, value_to_insert)
        con.commit()
        count += cursor.rowcount
    con.close()
