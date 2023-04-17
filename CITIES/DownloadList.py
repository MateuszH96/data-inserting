from configure import *
import pandas as pd
import wget

def downloadList():
    wget.download(URL_STATIONS,CSV_FILENAME)

    data = pd.read_csv(CSV_FILENAME,encoding=ENCODING,header=None)
    #add column at end
    data[len(data.columns)]=''
    
    for row in range(len(data[1])):
        data[3][row]=data[1][row] 
        for symbol in symbols:
            if symbol in data[3][row]:
                data[3][row]=data[3][row].replace(symbol,corretSymbols[symbol])

    data.to_csv(CSV_CORRECT,header=False,index=False)