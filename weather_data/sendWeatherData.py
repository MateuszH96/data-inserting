import wget
from configure import *
import json
import os
import psycopg2 as db
from zipfile import ZipFile
import pandas as pd
from database import *
import logging
pd.options.mode.chained_assignment = None


def prepareAndSend(con, cursor, df, dataFrameRow):
    JSON_TO_SEND["id_stacji"] = str(df[STATION_INDEX][dataFrameRow])
    JSON_TO_SEND["stacja"] = str(df[CITY_INDEX][dataFrameRow])
    month = f'{df[MONTH_INDEX][dataFrameRow]}'
    if len(month) == 1:
        month = '0'+month

    day = f'{df[DAY_INDEX][dataFrameRow]}'
    if len(day) == 1:
        day = '0'+day
    JSON_TO_SEND["data_pomiaru"] = f'{df[YEAR_INDEX][dataFrameRow]}-{month}-{day}'
    JSON_TO_SEND["godzina_pomiaru"] = str(df[HOUR_INDEX][dataFrameRow])
    JSON_TO_SEND["temperatura"] = str(df[TEMPERATURE_INDEX][dataFrameRow])
    JSON_TO_SEND["predkosc_wiatru"] = str(df[WIND_SPEED_INDEX][dataFrameRow])
    JSON_TO_SEND["kierunek_wiatru"] = str(df[WIND_DIR_INDEX][dataFrameRow])
    JSON_TO_SEND["wilgotnosc_wzgledna"] = str(df[HUMIDITY_INDEX][dataFrameRow])
    JSON_TO_SEND["suma_opadu"] = str(df[PRECIPITATION_INDEX][dataFrameRow])
    JSON_TO_SEND["cisnienie"] = str(df[PRESSURE_INDEX][dataFrameRow])
    jsonSend = json.dumps(JSON_TO_SEND)
    QUERY = f"INSERT INTO weather.json_data_weather (weather_data) VALUES('{jsonSend}')"
    cursor.execute(QUERY)
    con.commit()


def sendDataToDatabase(filename):
    con = db.connect(host=HOST, port=PORT, database=DATABASE,
                     user=USER, password=PASSWORD)
    cursor = con.cursor()
    with open(filename, "rt") as fin:
        with open(OUT_FILENAME, "wt") as fout:
            for line in fin:
                fout.write(line.replace('""', '" "'))
    df = pd.read_csv(OUT_FILENAME, header=None, encoding=ENCODING, on_bad_lines='skip')
    os.remove(OUT_FILENAME)
    os.remove(filename)
    for row in df.index:
        prepareAndSend(con, cursor, df, row)
    con.close()


def fillDatabase():
    logging.basicConfig(filename=f'send_data_to_database.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    START_YEAR=1960
    for yearOffset in range(0, 40, 5):
        if yearOffset == 5:
            START_YEAR += 1
        for stationNum in range(100, 1000):
            urlFilename=''
            try:
                urlPath = f'{START_YEAR+yearOffset}_{END_YEAR+yearOffset}'
                urlFilename = f'{START_YEAR+yearOffset}_{END_YEAR+yearOffset}_{stationNum}_s.zip'
                wget.download(URL_ARCHIVE_DATA+f'{urlPath}/{urlFilename}')
                with ZipFile(urlFilename, 'r') as zip:
                    zip.extractall()
                os.remove(urlFilename)
                sendDataToDatabase(f's_t_{stationNum}_{START_YEAR+yearOffset}_{END_YEAR+yearOffset}.csv')

            except Exception as err:
                if urlFilename != '':
                    os.remove(urlFilename)
                logging.error(str(err)+f' s_t_{stationNum}_{START_YEAR+yearOffset}_{END_YEAR+yearOffset}.csv')
    lastYearMonth = 1
    for rok in range(2001, CURRENT_YEAR+1):
        if lastYearMonth > 12:
            break
        for stationNum in range(100, 1000):
            zipFilename=''
            if lastYearMonth > 12:
                break
            try:
                zipFilename = f'{rok}_{stationNum}_s.zip'
                if rok == CURRENT_YEAR:
                    zipFilename = f'{rok}_0{lastYearMonth}_s.zip'
                    lastYearMonth += 1
                urlPath = f'{URL_ARCHIVE_DATA}/{rok}/{zipFilename}'
                wget.download(urlPath)
                with ZipFile(zipFilename, 'r') as zip:
                    zip.extractall()
                os.remove(zipFilename)
                sendDataToDatabase(f's_t_{stationNum}_{rok}.csv')

            except Exception as err:
                if zipFilename != '':
                    os.remove(zipFilename)
                logging.error(str(err)+f' s_t_{stationNum}_{START_YEAR+yearOffset}_{END_YEAR+yearOffset}.csv')
