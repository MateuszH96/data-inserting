import pandas as pd
import wget
from .configure import *

def downloadList():
    wget.download(URL,CSV_FILENAME)

    data = pd.read_csv(CSV_FILENAME,encoding='iso-8859-1',header=None)
    data[len(data.columns)]=''
    for i in range(len(data[1])):
        data[3][i]=data[1][i] 
        for j in symbols:
            if j in data[3][i]:
                print(data[3][i])
                data[3][i].replace(j,corretSymbols[j])

    data.to_csv(CSV_CORRECT)