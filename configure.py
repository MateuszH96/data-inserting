import password
import datetime
#DATABASE
HOST = "195.150.230.208"
PORT = "5432"
DATABASE = "2022_hamera_mateusz"
USERNAME = "2022_hamera_mateusz"
PASSWORD = password.PASSWD

#IMGW WEATHER DATA
URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/klimat/"
START_YEAR = 1951
END_YEAR = int(datetime.date.today().strftime("%Y"))
STEP_YEAR = 5
MONTHS = 12