import websocket
import json
import threading
import time
from datetime import datetime

ALL_SYMBOLS = [
	"ETHUSDT",
	"SOLUSDT",
	"TRXUSDT",
	"DOGEUSDT",
	"XRPUSDT",
	"LTCUSDT",
	"SUIUSDT",
	"ZKUSDT",
	"AAVEUSDT",
	"AVAXUSDT",
	"ZECUSDT",
	"1000PEPEUSDT",
	"OPUSDT",
	"ADAUSDT",
	"LINKUSDT",
	"UNIUSDT",
	"TONUSDT",
]

class KlineWebSocketClient:
    """
    WebSocket K线数据监听器
    """

    def __init__(self):
        """
        初始化 WebSocket 客户端

        Args:
            symbol: 交易对 (如 "BTCUSDT", "BTC-USDT")
            interval: K线周期 ("1m", "5m", "15m", "1h", "4h", "1d" 等)
        """
        self.exchange = "joyful"
        self.interval = "1m"
        self.ws = None
        self.running = False
        self.reconnect_count = 0
        self.max_reconnect = 5
        self._setup_url()

    def _setup_url(self):
        stream_name = ""
        for symbol in ALL_SYMBOLS:
            stream_name += f"{symbol.lower()}@volatility_{self.interval}/"
        stream_name = stream_name.rstrip("/")
        self.url = f"ws://154.86.24.27:8081/stream?streams={stream_name}"

    # ==================== WebSocket 回调函数 ====================

    def on_message(self, ws, message):
        """收到消息时的回调"""
        try:
            data = json.loads(message)
            self._process_kline(data)
        except Exception as e:
            print(f"❌ 消息解析错误: {e}")
            print(f"原始消息: {message[:200]}")

    def on_error(self, ws, error):
        """发生错误时的回调"""
        print(f"⚠️ WebSocket 错误: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """连接关闭时的回调"""
        print(f"🔌 连接已关闭 | 状态码: {close_status_code} | 原因: {close_msg}")
        self.running = False

        # 自动重连
        if self.reconnect_count < self.max_reconnect:
            self.reconnect_count += 1
            wait_time = min(2 ** self.reconnect_count, 60)  # 指数退避
            print(f"🔄 {wait_time}秒后尝试第 {self.reconnect_count} 次重连...")
            time.sleep(wait_time)
            self.start()
        else:
            print("❌ 达到最大重连次数，停止重连")

    def on_open(self, ws):
        """连接成功时的回调"""
        print(f"✅ WebSocket 连接成功!")
        self.reconnect_count = 0
        self.running = True

        # OKX 需要发送订阅消息
        if self.exchange == "okx" and hasattr(self, 'subscribe_msg'):
            ws.send(json.dumps(self.subscribe_msg))
            print("📤 已发送订阅消息")

    # ==================== K线数据处理 ====================

    def _process_kline(self, data):
        """
        解析并处理 K线数据
        """
        try:
            kline = {
                "symbol": data["symbol"],
                "start_time": int(data["start_time"]),
                "close_time": int(data["close_time"]),
                "open": float(data["open"]),
                "high": float(data["high"]),
                "low": float(data["low"]),
                "close": float(data["close"]),
                "volume": float(data["volume"]),
                "taker_buy_base_asset_volume": float(data["taker_buy_base_asset_volume"]),
                "taker_buy_quote_asset_volume": float(data["taker_buy_quote_asset_volume"]),
                "trades": int(data["trades"]),
                "vd": float(data["vd"]),
                "ma10": data["ma10"],
                "ratio": data["ratio"]
            }
            self.on_kline(kline)

        except Exception as e:
            print(f"❌ K线处理错误: {e}")
            print(f"数据: {json.dumps(data, indent=2)[:500]}")

    def on_kline(self, kline):
        """
        K线数据回调 - 子类可重写此方法实现自定义逻辑
        """
        print(kline)

    # ==================== 连接管理 ====================

    def start(self):
        """启动 WebSocket 连接"""
        print(f"\n🚀 正在连接 {self.exchange} WebSocket...")
        print(f"   URL: {self.url}")

        # 配置 WebSocket
        websocket.enableTrace(False)  # 设为 True 可查看详细日志

        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        # 在后台线程运行，避免阻塞主线程
        self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={
            "ping_interval": 20,      # 每20秒发送ping
            "ping_timeout": 10      # ping超时10秒
        })
        self.ws_thread.daemon = True
        self.ws_thread.start()

    def stop(self):
        """停止 WebSocket 连接"""
        print("\n🛑 正在关闭 WebSocket 连接...")
        self.running = False
        if self.ws:
            self.ws.close()
        print("✅ 已关闭")

    def is_alive(self):
        """检查连接状态"""
        return self.running and self.ws_thread.is_alive()


# ==================== 使用示例 ====================

def main():
    """主函数 - 演示如何使用"""

    # 示例1: 监听 BTC/USDT 1分钟K线
    print("=" * 60)
    print("示例: BTC/USDT 1分钟K线")
    print("=" * 60)

    client = KlineWebSocketClient()

    # 启动连接
    client.start()

    try:
        # 主线程保持运行
        print("\n⏳ 监听中... 按 Ctrl+C 停止")
        client.ws_thread.join()
    except KeyboardInterrupt:
        print("\n👋 用户中断")


if __name__ == "__main__":
    # 运行基础示例
    main()
