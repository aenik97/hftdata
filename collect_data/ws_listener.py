import json
import datetime
import rel
import requests
import websocket
import sys

TRADE_SYMBOL = sys.argv[1]

TRADE_SYMBOL = "usdcusdt"
STREAM_DEPTH = "depth"
STREAM_DEPTH_100 = "depth@100ms"
STREAM_DEPTH_10_LEVELS_100 = "depth10@100ms"
STREAM_DEPTH_20_LEVELS_100 = "depth20@100ms"
STREAM_BOOK_TICKER = "bookTicker"

_BASE_SOCKET = f"wss://stream.binance.com:9443/ws/{TRADE_SYMBOL}"
SOCKET_DEPTH = f"{_BASE_SOCKET}@{STREAM_DEPTH}"
SOCKET_DEPTH_100 = f"{_BASE_SOCKET}@{STREAM_DEPTH_100}"
SOCKET_DEPTH_10_LEVELS_100 = f"{_BASE_SOCKET}@{STREAM_DEPTH_10_LEVELS_100}"
SOCKET_DEPTH_20_LEVELS_100 = f"{_BASE_SOCKET}@{STREAM_DEPTH_20_LEVELS_100}"
SOCKET_BOOK_TICKER = f"{_BASE_SOCKET}@{STREAM_BOOK_TICKER}"
PRICE_URL = f"https://api.binance.com/api/v3/depth?symbol={TRADE_SYMBOL.upper()}&limit=1000"


class LogWriter:
    def __init__(self, stream_name, log_file_base_name):
        self.stream_name = stream_name

        self.log_file_base_name = log_file_base_name
        self.log_file_date = None
        self.log_file_obj = None

        self.websocket = None

    def get_current_logfile(self, now):
        date_time = now.strftime("%d_%m_%Y")
        log_path = f"{self.log_file_base_name}_{date_time}.log"

        if self.log_file_date is None:
            self.log_file_date = date_time
            self.log_file_obj = open(log_path, "a")
            self.add_log_header(now)

            return None

        if self.log_file_date != date_time:
            self.log_file_obj.close()

            self.log_file_date = date_time
            self.log_file_obj = open(log_path, "a")
            self.add_log_header(now)
            return None

        return None

    def add_log_header(self, now):
        response = requests.get(PRICE_URL)
        self.log_file_obj.write(
            f"Stream: {self.stream_name} started at {str(now)} {str(response.json())} \n"
        )

    def write_to_file(self, message):
        now = datetime.datetime.utcnow() # + datetime.timedelta(hours=4, minutes=44)
        self.get_current_logfile(now)

        line = {
            "timestamp": str(now),
            "payload": message
        }

        self.log_file_obj.write(json.dumps(line) + '\n')

    def on_message(self, ws, message):
        json_message = json.loads(message)
        self.write_to_file(json_message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def create_websocket(self):
        self.websocket = websocket.WebSocketApp(
            self.stream_name,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
         )


if __name__ == "__main__":
    lw_array = [
        LogWriter(
            stream_name=SOCKET_DEPTH,
            log_file_base_name=STREAM_DEPTH
        ),
        LogWriter(
            stream_name=SOCKET_DEPTH_100,
            log_file_base_name=STREAM_DEPTH_100
        ),
        LogWriter(
            stream_name=SOCKET_DEPTH_10_LEVELS_100,
            log_file_base_name=STREAM_DEPTH_10_LEVELS_100
        ),
        LogWriter(
            stream_name=SOCKET_DEPTH_20_LEVELS_100,
            log_file_base_name=STREAM_DEPTH_20_LEVELS_100
        ),
        LogWriter(
            stream_name=SOCKET_BOOK_TICKER,
            log_file_base_name=STREAM_BOOK_TICKER
        )
    ]

    # ws_clients
    for lw in lw_array:
        lw.create_websocket()
        lw.websocket.run_forever(dispatcher=rel, reconnect=3)

    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
