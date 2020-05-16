
import operator
import os, feather
import ta

import pandas_datareader as pdr
import pandas as pd

from multiprocessing.pool import ThreadPool

from datetime import date, timedelta

## TA Filter Conditions
BBandsPeriod = 20
BBandsStdDev = 2

RSIUpper = 75
RSILower = 25

RSIPeriod = 14

## Assume args[0] always refer to ticker
def tryExcept(f):
    def wrapper(*args, **kwargs):
        try: 
            return f(*args, **kwargs) 
        except Exception as e:
            print(f'{args[0]} EXCEPTION: {e}')
            return None
    return wrapper

def getDate(inter=0):
    return date.today() - timedelta(days=1+inter)

def getFilteredNASDAQTickers():
    
    data = pdr.nasdaq_trader.get_nasdaq_symbols()

    ticks = set(data[data['Nasdaq Traded']].index.values)   
    errorTicks = set(getErrorTicks()['ErrorTicks'])
    ticks -= errorTicks    
    
    return list(ticks)

def writeToDB(data, file, resetIndex=True):
    if not os.path.exists("db"):
        os.makedirs('db')
    print(f"Writing to db/{file}.file")
    feather.write_dataframe(data.reset_index() if resetIndex else data, f'db/{file}.file')        

def filterDF(data, dt, oper=operator.ge):
    return data[[oper(i.date(), dt) for i in pd.to_datetime(data.index)]]

def findDateIndex(data, idx, oper=operator.add):
    return oper(pd.to_datetime(data.index.values[idx]).date(), timedelta(days=1)) 

## Only extract data that doesn't exist so avoid repetitive extraction
@tryExcept
def extractYahooData(tick, startDate, endDate):
    
    ## If exists, take only dates that one needs
    if os.path.exists(f'db/{tick}.file'):
        
        df = pd.read_feather(f'db/{tick}.file').set_index('Date')
        df = filterDF(df, startDate)

        ## Get original length to determine if it has been appended with new data 
        origLen = len(df) 

        ## If len is zero, instantly go to reading the entire database and rewriting it
        if 0 == origLen:
            df = pdr.data.get_data_yahoo(tick, startDate, endDate)
            writeToDB(df, tick)
            return df

        ## Get first and last date
        firstdt = findDateIndex(df, 0, operator.sub)  
        lastdt = findDateIndex(df, -1)      

        ## Account for missing earlier dates (Try-except to avoid date-violation if for e.g. data doesnt exist)
        if firstdt >= startDate:
            try: 
                add_df = pdr.data.get_data_yahoo(tick, startDate, firstdt)
                add_df = filterDF(add_df, firstdt, operator.le) 
                df = df.combine_first(add_df)             
            except:
                pass

        ## Account for missing latest dates 
        if lastdt <= endDate:
            try: 
                add_df = pdr.data.get_data_yahoo(tick, lastdt, endDate)
                add_df = filterDF(add_df, lastdt, operator.ge) 
                df = add_df.combine_first(df) 
            except:
                pass
                    
        ## Only write to feather.file if length changes
        if origLen != len(df):
            writeToDB(df, tick) 

    else: 
        df = pdr.data.get_data_yahoo(tick, startDate, endDate)
        writeToDB(df, tick)          

    return df

def getErrorTicks():
    file = 'db/error.file'
    return pd.read_feather(file) if os.path.exists(file) else pd.DataFrame(columns=['ErrorTicks'])

## Multiprocessing mechanism
def multiproc(iterList, errorList, f, args, procs=8, storeRes=False):
    
    pool = ThreadPool(processes=procs)

    async_result, result = [], []

    for i in iterList:
        async_result.append(pool.apply_async(f, [i] + args))

    for k,v in enumerate(async_result):
        v.wait()
        res = v.get()
        if res is None:
            errorList.append(iterList[k])
        elif storeRes: 
            result.append(res)

    return result, errorList

## Parallel download of ticker symbols - Default 8 processes
def populateDB(procs=8):
    
    print("Initiating population of DB for last 250 days")
    
    ticks = getFilteredNASDAQTickers()  
    errList = list(getErrorTicks()['ErrorTicks'])
    origLen = len(errList)
    
    dtRange = [getDate(250), getDate()]
    res, errList = multiproc(ticks, errList, extractYahooData, dtRange, procs)  

    ## Update error.file
    if origLen != len(errList):
        writeToDB(pd.DataFrame({'ErrorTicks': errList}), 'error', False)

    print("Completion of population of DB")

## Check for Bollinger Bands and RSI Violations
def checkTAFilter(tick, startDate, endDate):
    
    df = extractYahooData(tick, startDate, endDate)

    if df is None:
        return None

    indicator_rsi = ta.momentum.RSIIndicator(close=df['Adj Close'], n=RSIPeriod, fillna=False)
    df['RSI'] = indicator_rsi.rsi()
    
    indicator_bb = ta.volatility.BollingerBands(close=df['Adj Close'], n=BBandsPeriod, ndev=BBandsStdDev)
    df['bb_High'] = indicator_bb.bollinger_hband_indicator() 
    df['bb_Low'] = indicator_bb.bollinger_lband_indicator()
    
    ## Get last row of dataframe
    row = df.iloc[-1]

    ## Identify overbought/oversold tickers
    if (row['RSI'] >= RSIUpper) and bool(row['bb_High']):
        return [tick, True] 

    if (row['RSI'] <= RSILower) and bool(row['bb_Low']):
        return [tick, False]  
    
    return None

## Multi-processing wrapper for checkTAFilter  
def getFilteredTicks(procs=8):
    
    print("Identifying Overbought/Oversold Tick Symbols")

    ticks = getFilteredNASDAQTickers()  
    errList = list(getErrorTicks()['ErrorTicks'])
    origLen = len(errList)  
    
    dtRange = [getDate(250), getDate()]
    filteredTicks, errList = multiproc(ticks, errList, checkTAFilter, dtRange, procs, True)  

    ## Update error.file
    if origLen != len(errList):
        writeToDB(pd.DataFrame({'ErrorTicks': errList}), 'error', False)

    df = pd.DataFrame(filteredTicks, columns=['Symbol','Type'])
    df['Type'] = df['Type'].map({True:'Overbought', False:'Oversold'})

    print("Completion of Identification")

    return df

def getAndStoreFilteredTicks(file='filteredTicks'):
    writeToDB(getFilteredTicks(), file, False)

def readFilteredTicks(file='filteredTicks'):
    file = f'db/{file}.file'
    return pd.read_feather(file) if os.path.exists(file) else pd.DataFrame(columns=['Symbol','Type'])    