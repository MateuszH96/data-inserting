YEAR_INDEX = 2
MONTH_INDEX = 3
DAY_INDEX = 4
HOUR_INDEX = 5
STATION_INDEX = 0
TEMPERATURE_INDEX = 29
WIND_SPEED_INDEX = 25
WIND_DIR_INDEX = 23
HUMIDITY_INDEX = 37
PRESSURE_INDEX = 41
PRECIPITATION_INDEX = 48
CITY_INDEX = 1

START_YEAR = 1960
END_YEAR = 1965

OUT_FILENAME = 'out.csv'
ENCODING = 'iso-8859-1'

CURRENT_YEAR = 2023
URL_ARCHIVE_DATA = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/'
JSON_TO_SEND = {
    "id_stacji": "X",
    "stacja": "X",
    "data_pomiaru": "X",
    "godzina_pomiaru": "X",
    "temperatura": "X",
    "predkosc_wiatru": "X",
    "kierunek_wiatru": "X",
    "wilgotnosc_wzgledna": "X",
    "suma_opadu": "X",
    "cisnienie": "X"
}

DOWNLOADED_FILE = 'download.zip'
CSV_FILENAME = 'incorrect_data.csv'
CSV_CORRECT = 'correct_data.csv'
ENCODING = 'iso-8859-1'
URL_STATIONS = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/wykaz_stacji.csv'
symbols = ['£', 'Ê', '¯', 'Ñ', '¥', 'Æ', '\x8f', '\x8c']
corretSymbols = {
    '£': 'Ł',
    'Ê': 'Ę',
    '¯': 'Ż',
    'Ñ': 'Ń',
    '¥': 'Ą',
    'Æ': 'Ć',
    '\x8f': 'Ź',
    '\x8c': 'Ś'
}
