DOWNLOADED_FILE='download.zip'
CSV_FILENAME='incorrect_data.csv'
CSV_CORRECT='correct_data.csv'
URL='https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/wykaz_stacji.csv'
symbols=['£','Ê','¯','Ñ','¥','Æ','\x8f','\x8c']
corretSymbols={
    '£':'Ł',
    'Ê':'Ę',
    '¯':'Ż',
    'Ñ':'Ń',
    '¥':'Ą',
    'Æ':'Ć',
    '\x8f':'Ź',
    '\x8c':'Ś'
}