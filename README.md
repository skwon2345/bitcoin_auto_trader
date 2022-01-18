# bitcoin_auto_trader
## Intro
- Language: Python
- Database: Redis

This is a bitcoin auto trading program, with the buy and sell algorithm that I developed. Not only the Bitcoin, but other cryptocurrencies can be traded with this program automatically.

It uses several indicators such as SMA, EMA and Bollinger Bands.
Also update indicators information every five minutes, and buy after comparing with the current price.
Current price is retrieved by socket from Upbit, which is a live current price.

All the transaction information and chart data are saved in Redis.

## Instruction
### Install Requirements.txt
```bash
$ pip install -r requirements.txt
```

### Collect Chart Data
```bash
$ python chart_collect.py
```

### Update chart data every 5 minutes
```docker
$ python update.py
```

### Run buy-sell program
```bash
$ python run.py
```
