#ch09/09_17.py
from pyupbit import WebSocketManager
import sys
import traceback2 as traceback
import requests
import redis
from datetime import datetime
from upbitpy import Upbitpy
import time
import logging
import pandas as pd
import json
import itertools

def wait(min):
    now = datetime.now()
    remain_second = 60 - now.second
    remain_second += 60 * (min - (now.minute % min + 1))
    time.sleep(remain_second)
    
def calcSMA (df, values, window):
	df['sma'+str(window)] = values.rolling(window).mean()
	return df['sma'+str(window)]

def calcEMA (df, values, window):
        d = values.sort_index()
        
        a = d.ewm(span=window, min_periods=0, adjust=False, ignore_na=False).mean()
        df['ema'+str(window)] = a.iloc[::-1]
        return df['ema'+str(window)]

def calcBollBand(x, w=20, k=2):

        for item in x:
                x['BolBandMA'] = x['c'].rolling(window=w).mean()
                x['BolBandSTD'] = x['c'].rolling(window=w).std(ddof=0)
                x['UpperBand'] = x['BolBandMA'] + (k * (x['BolBandSTD']))
                x['LowerBand'] = x['BolBandMA'] - (k * (x['BolBandSTD']))
        return x

def computeRSI (df, data, time_window):
    diff = data.diff(1).dropna()        # diff in one field(one day)

    up_chg = 0 * diff
    down_chg = 0 * diff
    
    up_chg[diff > 0] = diff[ diff>0 ]
    
    down_chg[diff < 0] = diff[ diff < 0 ]
    
    up_chg_avg   = up_chg.ewm(com=time_window-1 , min_periods=time_window).mean()
    down_chg_avg = down_chg.ewm(com=time_window-1 , min_periods=time_window).mean()
    
    rs = abs(up_chg_avg/down_chg_avg)
    rsi = 100 - 100/(1+rs)
    df['rsi'] = rsi
    return rsi



timespan_1 = '5'
timespan_4 = '60'

if __name__ == '__main__':
    try:
        conn = redis.StrictRedis(
        host='localhost',
        port=6384,
        charset="utf-8", decode_responses=True,
        db=1)

    except Exception as e:
            print('Error occured: ', e, traceback.format_exc())

##    cur = json.loads(conn.hgetall('upbit:buyList:ADXKRW')['ADXKRW'])
######    
##    ddd = len(cur['t'])
##    print(ddd)
##    print(cur['t'][str(0)])
##    print(cur['ema200'][str(0)])
##    cur['t'] = dict(itertools.islice(cur['t'].items(),1500))
##    ddd = len(cur['t'])
##    print(ddd)
####    
##    exit()
            
##    wait(5)
    
    date_str = '2021-03-11 00:00:00'
    date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    from_ts = int(time.mktime(date_obj.timetuple()))
##    date_str = '2021-03-15 16:00:00'
##    date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
##    to_ts = int(time.mktime(date_obj.timetuple()))
    
    to_ts = int(time.mktime(datetime.today().timetuple()))-300
##    from_ts = to_ts-300

    print("to:",to_ts)
    print("from:",from_ts)
    
    req = requests.get('https://s3.ap-northeast-2.amazonaws.com/crix-production/crix_master?nonce=1614317584555')
    ret = req.json()

    start = time.time()

    for r in ret:
            try:
                if r['isTradingSuspended'] == True or r['tradeStatus'] != 'ACTIVE' or r['exchange'] != 'UPBIT' or r['quoteCurrencyCode'] != 'KRW':
                    continue
                
                ticker = str(r['baseCurrencyCode'])+str(r['quoteCurrencyCode'])
                saveName = str(r['quoteCurrencyCode'])+'-'+str(r['baseCurrencyCode'])

                req_ticker = requests.get('https://crix-api-tv.upbit.com/v1/crix/tradingview/history?symbol='+ticker+'&resolution='+timespan_1+'&from='+str(from_ts)+'&to='+str(to_ts))
                ret_ticker = req_ticker.json()

                if ret_ticker['s'] == 'no_data':
                    continue

                t = [ str(datetime.fromtimestamp(ret_ticker['t'][i] // 1000)) for i in range(len(ret_ticker['t'])) ]
                o = [ ret_ticker['o'][i] for i in range(len(ret_ticker['o'])) ]
                h = [ ret_ticker['h'][i] for i in range(len(ret_ticker['h'])) ]
                l = [ ret_ticker['l'][i] for i in range(len(ret_ticker['l'])) ]
                c = [ ret_ticker['c'][i] for i in range(len(ret_ticker['c'])) ]
                timestamp = [ str(ret_ticker['t'][i] // 1000) for i in range(len(ret_ticker['t'])) ]
                                        
                data = {'t':t,
                        'o':o,
                        'c':c,
                        'h':h,
                        'l':l,
                        'timestamp': timestamp
                        }
                

                df_reverse_1 = pd.DataFrame(data)

                        
                sma_df_1 = df_reverse_1['c'].copy()

                calcSMA(df_reverse_1, sma_df_1, 5)
                calcSMA(df_reverse_1, sma_df_1, 10)
                calcSMA(df_reverse_1, sma_df_1, 20)

                calcEMA(df_reverse_1, sma_df_1, 60)
                calcEMA(df_reverse_1, sma_df_1, 120)
                calcEMA(df_reverse_1, sma_df_1, 200)

                calcBollBand(df_reverse_1)
                computeRSI(df_reverse_1, sma_df_1, 14)

                df_redis = json.dumps(df_reverse_1.iloc[::-1].to_dict())
                
                conn.hset('upbit:chartData:'+saveName, saveName, df_redis)

                
                
            except Exception as e:
                print('Error occured: ', e, traceback.format_exc())


    print("time:", time.time()-start)
                
##    wm = WebSocketManager("ticker", tickers)
##    while True:
##        data = wm.get()
##        print(data.get("code"), data.get("trade_price"))
##        
####        try:
####            conn = redis.StrictRedis(
####            host='localhost',
####            port=6384,
####            charset="utf-8", decode_responses=True,
####            db=1)
####
####            now = datetime.now()
####            now_string = now.strftime("%d/%m/%Y %H:%M:%S")
####
####            conn.hset(str(data.get("code")), {'trade_price': data.get("trade_price"), 'update_time': now_string})
####            print("DB Update", conn.hgetall('current_price')['code'], conn.hgetall('current_price')['trade_price'])
####            
####
####        except Exception as ex:
####                print ('Error:', ex)

