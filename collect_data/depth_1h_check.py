import datetime
import json
import sys
import requests


if __name__ == '__main__':
    """
        Получает стакан. 
        Стоит через cron запускается каждый час.
        Пример запуска python test0.py <symbol>
            <symbol> - lowercase
    """
    TRADE_SYMBOL = sys.argv[1]
    
    PRICE_URL = f"https://api.binance.com/api/v3/depth?symbol={TRADE_SYMBOL.upper()}&limit=1000"
    
    
    now = datetime.datetime.utcnow()
    response = requests.get(PRICE_URL)

    line = {
        "timestamp": str(now),
        "payload": str(response.json())
    }

    with open(f'depth_1h_check_{TRADE_SYMBOL}.log', 'a') as f:
        f.write(json.dumps(line) + '\n')
