import json
import math
#dotenv
import os
import time
from datetime import datetime
from os.path import dirname, join
from threading import Thread

import pandas as pd
import pyupbit
import redis
import requests
from dotenv import load_dotenv
from websocket import WebSocketApp


class UpbitReal:
    def __init__(self, request, callback=print):
        self.request = request
        self.callback = callback
        self.ws = WebSocketApp(
            url="wss://api.upbit.com/websocket/v1",
            on_message=lambda ws, msg: self.on_message(ws, msg),
            on_error=lambda ws, msg: self.on_error(ws, msg),
            on_close=lambda ws:     self.on_close(ws),
            on_open=lambda ws:     self.on_open(ws))
        self.running = False

        self.conn = redis.StrictRedis(
        host='localhost',
        port=6384,
        charset="utf-8", decode_responses=True,
        db=1)

        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)

        access_key = os.environ.get("ACCESS_KEY")
        secret_key = os.environ.get("SECRET_KEY")
        telegram_key = os.environ.get("TELEGRAM_KEY")

        self.upbit = pyupbit.Upbit(access_key, secret_key)

        self.MAX_COUNT = 10

    
    def isRightOrder(self, e1,e2,e3):
        return e1 > e2 and e2 > e3

    def calcQuantity(self, balance, bCount, price):
        if self.MAX_COUNT == bCount:
            return -1
        elif self.MAX_COUNT-bCount == 1:
            self.callback("ddd", float(balance/price))
            return float((balance-(balance*0.006))/price)
        
        availBalance = balance/(self.MAX_COUNT-bCount)
        
        if availBalance < 7000:
            return -1
        
        quant = float(availBalance/price)
        
        return quant
        

    def calcBollBand(self, x, w=20, k=2):

        for item in x:
                x['BolBandMA'] = x['c'].rolling(window=w).mean()
                x['BolBandSTD'] = x['c'].rolling(window=w).std(ddof=0)
                x['UpperBand'] = x['BolBandMA'] + (k * (x['BolBandSTD']))
                x['LowerBand'] = x['BolBandMA'] - (k * (x['BolBandSTD']))
        return x

    
    
    def priceCutting(self, aprice, stype, n):
        remove_price = 0
        stype = stype if stype else "R"
        remove_price = aprice / n
        if stype == "F":
            remove_price = math.floor(remove_price)
        elif stype == "R":
            remove_price = round(remove_price)
        elif stype == "C":
            remove_price = math.ceil(remove_price)
        
        remove_price = remove_price * n
        return remove_price

    def ovs(self, aprice):
        price = float(aprice);
        if price < 10:
            return 0.01
        elif price >= 10 and price < 100:
            return 0.1
        elif price >= 100 and price < 1000:
            return 1
        elif price >= 1000 and price < 10000:
            return 5
        elif price >= 10000 and price < 100000:
            return 10
        elif price >= 100000 and price < 500000:
            return 50
        elif price >= 500000 and price < 1000000:
            return 100
        elif price >= 1000000 and price < 2000000:
            return 500
        elif price >= 2000000:
            return 1000
        
    def on_message(self, ws, msg):
        data = json.loads(msg.decode('utf-8'))
##        self.callback(msg)

        code = data["code"]
        price = data["trade_price"]
        rd = json.loads(self.conn.hgetall('upbit:prev_cur:'+code)['prev'])
        
        if not self.conn.exists('upbit:buyList:'+code):
##        print(code, price)
            buySignal = self.conn.hgetall('upbit:prev_cur:'+code)['buySignal']
            
            if buySignal == 'True':
                dec = self.ovs(price)
                tp = self.priceCutting(rd['ema200'],'F',dec)
                tp = round(tp, 2)
                body = code+'- Price: '+str(price) +' '+ str(tp)
                self.callback(body)

##                bids_asks = ob[0]['orderbook_units'][0] # for market order
##                ask_price = bids_asks['ask_price'] # for market order
                if tp == price: # for limit order
##                if ask_price < tp: # for market order
     
                    balance = float(self.upbit.get_balances()[0]['balance'])
                    bCount = int(self.conn.hgetall('upbit:buyList:COUNT')['count'])
                    
                    quantity = self.calcQuantity(balance, bCount, price)
                    availBalance = balance/(self.MAX_COUNT-bCount)
                    
                    if quantity > -1: # for limit order
##                    if quantity > -1 and bids_asks['ask_size'] >= quantity: # for market order
                        order = self.upbit.buy_limit_order(code, price, quantity)
##                        order = self.upbit.buy_market_order(code, availBalance) # for market order
                        
                        if 'error' in order:
                            self.callback(order['error'])
                        
                        now = datetime.now()
                        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                        
                        self.callback(body)
                        
                        dict_upload = {'uuid':order['uuid'], 'date':dt_string, 'price': price, 'quantity': order['volume']}
                        upload_str = json.dumps(dict_upload)
                        url = 'https://api.telegram.org/bot1568259234:'+self.telegram_key+'/sendMessage?chat_id=-443945140&text=B U Y\n'+code+'\n'+str(price)+'&disable_web_page_preview=true'
                        requests.get(url)

                        self.conn.hset('upbit:buyList:'+code, code, upload_str)

                        # BUY Count setting
                        bCount = int(self.conn.hgetall('upbit:buyList:COUNT')['count'])
                        bCount += 1
                        self.conn.hset('upbit:buyList:COUNT', 'count', bCount)
                    
                    
        else:
                
            data = json.loads(self.conn.hgetall('upbit:bbCalc:'+code)['bb'])
            df_reverse_1 = pd.DataFrame(data)
            
            data = {'c':price}
            df_reverse_2 = pd.DataFrame(data, index=[str(len(df_reverse_1))])

            df = pd.concat([df_reverse_2, df_reverse_1])
            df_len = len(df)
            df.index=[i for i in range(df_len-1, -1, -1)]
            df=df.iloc[::-1]
            
            self.calcBollBand(df)
            dec = self.ovs(price)
            tp = self.priceCutting(df['UpperBand'][df_len-1],'R',dec)
            data = json.loads(self.conn.hgetall('upbit:buyList:'+code)[code])
            buyP = float(data['price'])
            buyQ = float(data['quantity'])

            ob = pyupbit.get_orderbook(code)
            bids_asks = ob[0]['orderbook_units'][0]
            bid_price = bids_asks['bid_price']

##            if tp == price and tp != buyP:
            if (bids_asks['bid_size'] >= buyQ and bid_price > tp and bid_price != buyP): #or (isRightOrder(rd['ema200'], rd['ema120'], rd['ema60']) and rd['rsi'] > 47.0):
                self.callback(tp, bids_asks['bid_price'])
                
##                order = self.upbit.sell_limit_order(code, price, buyQ)
                order = self.upbit.sell_market_order(code, buyQ)
                self.callback('sell order: ', order)
                
                profit = ((bid_price-buyP)/buyP)*100.0
                if profit < 0.0:
                    numNeg = int(self.conn.hgetall('upbit:sellList:COUNT')['numNeg'])
                    numNeg += 1
                    self.conn.hset('upbit:sellList:COUNT', 'numNeg', numNeg)
                    
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

                if self.conn.exists('upbit:sellList:'+code):
                    dict_upload = {'uuid': order['uuid'], 'buy_date':data['date'], 'buyPrice': buyP, 'buyQuantity': buyQ,'sell_date':dt_string, 'sellPrice': bid_price, 'sellQuantity': buyQ, 'profit': profit}

                    data = json.loads(self.conn.hgetall('upbit:sellList:'+code)[code])
                    df_reverse_1 = pd.DataFrame(data)
                    df_reverse_2 = pd.DataFrame(dict_upload, index=[str(len(df_reverse_1))])
                    
                    df = pd.concat([df_reverse_1, df_reverse_2])
                    df_dict = df.to_dict()
                    df_redis = json.dumps(df_dict)

                    self.conn.hset('upbit:sellList:'+code, code, df_redis)
                else:
                    dict_upload = {'uuid': [order['uuid']], 'buy_date':[data['date']], 'buyPrice': [buyP], 'buyQuantity': [buyQ], 'sell_date':[dt_string], 'sellPrice': [bid_price], 'sellQuantity': [buyQ], 'profit': [profit]}
                    upload_str = json.dumps(dict_upload)
                    self.conn.hset('upbit:sellList:'+code, code, upload_str)

                url = 'https://api.telegram.org/bot1568259234:'+self.telegram_key+'/sendMessage?chat_id=-443945140&text=S E L L\n'+code+'\n'+str(bid_price)+'\n'+str(profit)+'&disable_web_page_preview=true'
                requests.get(url)


                sCount = int(self.conn.hgetall('upbit:sellList:COUNT')['count'])
                sCount += 1
                self.conn.hset('upbit:sellList:COUNT', 'count', sCount)
                
                self.conn.hdel('upbit:buyList:'+code, code)

                # BUY Count setting
                bCount = int(self.conn.hgetall('upbit:buyList:COUNT')['count'])
                bCount -= 1
                self.conn.hset('upbit:buyList:COUNT', 'count', bCount)


    def on_error(self, ws, msg):
        self.callback('sk Error: ',msg)
##        if msg == 'Connection is already closed.':
##            print("Yes")
##            self.restart = True
            
        
    def on_close(self, ws):
        self.callback("closed")
        self.running = False


        time.sleep(5)

        ## =================================================
        ## R E S T A R T
        ## =================================================
        req = requests.get('https://s3.ap-northeast-2.amazonaws.com/crix-production/crix_master?nonce=1614317584555')
        ret = req.json()

        tickers = []
        
        for r in ret:            
            if r['isTradingSuspended'] == True or r['tradeStatus'] != 'ACTIVE' or r['exchange'] != 'UPBIT' or r['quoteCurrencyCode'] != 'KRW':
                continue
            
            tickers.append(str(r['quoteCurrencyCode'])+'-'+str(r['baseCurrencyCode']))

        dict_req = {"type":"ticker","codes":tickers}
        dict_ret = json.dumps(dict_req)

        request='[{"ticket":"test"},'+dict_ret+']'
        self.restart = False
        real = UpbitReal(request=request)     
        real.start()
            

        
    def on_open(self, ws):
        th = Thread(target=self.activate, daemon=True)
        th.start()
        
    def activate(self):
        self.ws.send(self.request)
        while self.running:
            time.sleep(1)
        self.ws.close()
        
        
    def start(self):
        self.running = True
        self.ws.run_forever()
    
if __name__ == "__main__":
    req = requests.get('https://s3.ap-northeast-2.amazonaws.com/crix-production/crix_master?nonce=1614317584555')
    ret = req.json()

    tickers = []
    
    for r in ret:            
        if r['isTradingSuspended'] == True or r['tradeStatus'] != 'ACTIVE' or r['exchange'] != 'UPBIT' or r['quoteCurrencyCode'] != 'KRW':
            continue
        
        tickers.append(str(r['quoteCurrencyCode'])+'-'+str(r['baseCurrencyCode']))

    dict_req = {"type":"ticker","codes":tickers}
    dict_ret = json.dumps(dict_req)

    request='[{"ticket":"test"},'+dict_ret+']'
    real = UpbitReal(request=request)     
    real.start()
