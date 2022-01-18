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
import pyupbit

#dotenv
import os
from os.path import join, dirname
from dotenv import load_dotenv

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
    
def calcPPO(p, s20):
        return p/s20*100.0
    
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

def isRightOrder(e1,e2,e3):
        return e1 > e2 and e2 > e3


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

##    cur = json.loads(conn.hgetall('upbit:buyListADXKRW')['ADXKRW'])
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

    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    access_key = os.environ.get("ACCESS_KEY")
    secret_key = os.environ.get("SECRET_KEY")

    upbit = pyupbit.Upbit(access_key, secret_key)

    
        
    
    



    while(True):
        
##        date_str = '2021-03-09 16:00:00'
##        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
##        from_ts = int(time.mktime(date_obj.timetuple()))
##        date_str = '2021-03-18 16:00:00'
##        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
##        to_ts = int(time.mktime(date_obj.timetuple()))
##        to_ts = int(time.mktime(datetime.today().timetuple()))

        wait(5)
            
        to_ts = int(time.mktime(datetime.today().timetuple()))
        from_ts = to_ts-300
        
        req = requests.get('https://s3.ap-northeast-2.amazonaws.com/crix-production/crix_master?nonce=1614317584555')
        ret = req.json()

        start = time.time()

        for r in ret:
            
            try:
                if r['isTradingSuspended'] == True or r['tradeStatus'] != 'ACTIVE' or r['exchange'] != 'UPBIT' or r['quoteCurrencyCode'] != 'KRW':
                    continue
                
                ticker = str(r['baseCurrencyCode'])+str(r['quoteCurrencyCode'])
                saveName = str(r['quoteCurrencyCode'])+'-'+str(r['baseCurrencyCode'])
                
                data = json.loads(conn.hgetall('upbit:chartData:'+saveName)[saveName])

                df_reverse_1 = pd.DataFrame(data)
                
                

                req_ticker = requests.get('https://crix-api-tv.upbit.com/v1/crix/tradingview/history?symbol='+ticker+'&resolution='+timespan_1+'&from='+str(from_ts)+'&to='+str(to_ts))
                ret_ticker = req_ticker.json()

                if ret_ticker['s'] == 'no_data':
                    continue

                t = [str(datetime.fromtimestamp(ret_ticker['t'][0] // 1000))]
                o = [ret_ticker['o'][0]]
                h = [ret_ticker['h'][0]]
                l = [ret_ticker['l'][0]]
                c = [ret_ticker['c'][0]]
                timestamp = str(ret_ticker['t'][0] // 1000)
                                        
                data = {'t':t,
                        'o':o,
                        'c':c,
                        'h':h,
                        'l':l,
                        'timestamp': timestamp
                        }
                

                df_reverse_2 = pd.DataFrame(data, index=[str(len(df_reverse_1))])

                df = pd.concat([df_reverse_2, df_reverse_1])
                df.index=[i for i in range(len(df)-1, -1, -1)]
                df=df.iloc[::-1]

                sma_df_1 = df['c'].copy()

                calcSMA(df, sma_df_1, 5)
                calcSMA(df, sma_df_1, 10)
                
                calcSMA(df, sma_df_1, 20)

                calcEMA(df, sma_df_1, 60)
                calcEMA(df, sma_df_1, 120)
                calcEMA(df, sma_df_1, 200)

                calcBollBand(df)
                computeRSI(df, sma_df_1, 14)
                
    ##                print(df['ema60'])
    ##                print(df['ema120'])
    ##                print(df['ema200'])
    ##                print(df['t'])
    ##                print(ticker)
    ##                exit()
                df = df.iloc[::-1]
                df = df.iloc[:1500:]
                df_bb = df.iloc[:19:]
                

                df_len = len(df)
                df_bb_len = len(df_bb)
                df.index=[i for i in range(df_len-1, -1, -1)]
                df_bb.index=[i for i in range(df_bb_len-1, -1, -1)]
                df_bb_dict = {'c':[a for a in df_bb['c']]}
                
                df_dict = df.to_dict()
##                df_bb_dict = df_bb.to_dict()

                df_redis = json.dumps(df_dict)
                df_redis_prev = json.dumps(df.loc[df_len-1].to_dict())
                df_redis_bb = json.dumps(df_bb_dict)

                df_i = df_len-1
                buySignal = False
                if df['rsi'][df_i] < 45.0 and isRightOrder(df['ema60'][df_i], df['ema120'][df_i], df['ema200'][df_i]) and df['sma20'][df_i] > df['ema60'][df_i] and df['LowerBand'][df_i] >  df['ema200'][df_i] and calcPPO(df["c"][df_i], df['sma5'][df_i]) < 101.0 and calcPPO(df["c"][df_i], df['sma10'][df_i]) < 101.0 and calcPPO(df["c"][df_i], df['sma20'][df_i]) < 101.0:
                    buySignal = True
                
                conn.hset('upbit:chartData:'+saveName, saveName, df_redis)
                conn.hset('upbit:prev_cur:'+saveName, 'prev', df_redis_prev)
                conn.hset('upbit:prev_cur:'+saveName, 'buySignal', str(buySignal))
                conn.hset('upbit:bbCalc:'+saveName, 'bb', df_redis_bb)
                
            except Exception as e:
                print('Error occured: ', e, traceback.format_exc())
                
        print("time consumed: "+ str(time.time()-start)[:5]+'sec')
        
        bCount = int(conn.hgetall('upbit:buyList:COUNT')['count'])
        if bCount > 0:
            for key in conn.scan_iter("upbit:buyList:*"):
                k = key[14:]
                if k != "COUNT":
                    history = upbit.get_order(k, state = 'wait', kind = 'normal', contain_req=False)
                    if len(history) > 0:
                        uuid = history[0]['uuid']
                        order = upbit.cancel_order(uuid)
                        print(order)
                        ## 주문 취소가 되기전에 지정가로 조금의 물량이라도 샀다면, 시장가로 바로 팔아버림.
                        if order['executed_volume'] > 0.0:
                            upbit.sell_market_order(k, order['executed_volume'])
                        conn.hdel('upbit:buyList:'+k, k)
                        bCount = int(conn.hgetall('upbit:buyList:COUNT')['count'])
                        bCount -= 1
                        conn.hset('upbit:buyList:COUNT', 'count', bCount)


    
                
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

