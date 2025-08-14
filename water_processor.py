# water_processor.py (Phiên bản cuối cùng, thông minh và độc lập)

import json
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import argparse
import os
import queue
import configparser
import threading
from collections import deque
import paho.mqtt.client as mqtt

# ======================================================================
# LỚP 0: CẤU HÌNH VÀ LOGGING
# ======================================================================

LOG_DIRECTORY = "water_processor_logs"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
logger = logging.getLogger("water_processor")
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - WATER-PROCESSOR - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
log_file_path = os.path.join(LOG_DIRECTORY, "water_processor.log")
file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=7, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class FatalWatchdogError(Exception):
    """Lỗi nghiêm trọng khi watchdog bị kích hoạt."""
    pass

# ======================================================================
# LỚP 1: BỘ XỬ LÝ DỮ LIỆU CỐT LÕI (WATER ENGINE)
# ======================================================================

class DataEngine:
    def __init__(self, config):
        self.config = config
        self.history = deque(maxlen=self.config.history_size)
        logger.info(f"Data Engine initialized. History size: {self.config.history_size} points.")

    def process_payload(self, payload_str):
        try:
            payload = json.loads(payload_str)
            timestamp = payload.get("timestamp", time.time())
            value = payload.get("value")

            if value is None:
                logger.warning(f"Payload không chứa trường 'value': {payload_str}")
                return None

            value = float(value)

            if not (self.config.valid_range_min <= value <= self.config.valid_range_max):
                logger.warning(f"REJECTED: Giá trị {value:.2f}m nằm ngoài phạm vi hợp lệ ({self.config.valid_range_min}m - {self.config.valid_range_max}m).")
                return None

            if len(self.history) >= 5:
                past_values = [item[1] for item in self.history]
                mean = sum(past_values) / len(past_values)
                std_dev = (sum([(x - mean) ** 2 for x in past_values]) / len(past_values)) ** 0.5
                
                if std_dev > 0.01 and abs(value - mean) > self.config.spike_threshold_std_dev * std_dev:
                     logger.warning(f"REJECTED (SPIKE): Giá trị {value:.2f}m bị coi là nhiễu đột biến so với trung bình {mean:.2f}m.")
                     return None
            
            self.history.append((timestamp, value))

            rate_of_change_mm_per_min = 0.0
            if len(self.history) >= self.config.trend_window_size:
                trend_data = list(self.history)[-self.config.trend_window_size:]
                start_point, end_point = trend_data[0], trend_data[-1]
                delta_time_sec = end_point[0] - start_point[0]
                delta_value_m = end_point[1] - start_point[1]

                if delta_time_sec > 1:
                    rate_of_change_m_per_sec = delta_value_m / delta_time_sec
                    rate_of_change_mm_per_min = rate_of_change_m_per_sec * 1000 * 60

            report = {
                "type": "processed_water_report",
                "timestamp": timestamp,
                "processed_value": value,
                "analysis": {
                    "rate_of_change_mm_per_min": round(rate_of_change_mm_per_min, 3),
                    "history_points_analyzed": len(self.history)
                },
                "original_payload": payload
            }
            return report

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error(f"Lỗi khi xử lý payload: '{payload_str}'. Lỗi: {e}")
            return None
        except Exception as e:
            logger.critical(f"Lỗi không xác định trong DataEngine: {e}", exc_info=True)
            return None

# ======================================================================
# LỚP 2: BỘ XỬ LÝ MQTT
# ======================================================================

class MqttProcessor:
    def __init__(self, config, topics):
        self.config = config
        self.message_queue = queue.Queue(maxsize=1000)
        self.engine = DataEngine(config)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata={'topics': topics})
        if config.username: self.client.username_pw_set(config.username, config.password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.last_message_timestamp = time.time()
        self.last_reconnect_attempt = 0
        self.watchdog_lock = threading.Lock()

    def on_connect(self, client, userdata, flags, rc, props=None):
        rc_val = getattr(rc, 'value', rc)
        if rc_val == 0:
            logger.info("MQTT broker connected successfully.")
            for t in userdata['topics']:
                client.subscribe(t)
                logger.info(f"Subscribed to topic: {t}")
        else:
            logger.error(f"Failed to connect to MQTT, return code: {rc_val}")

    def on_message(self, client, userdata, msg):
        with self.watchdog_lock:
            self.last_message_timestamp = time.time()
        try:
            self.message_queue.put(msg.payload.decode("utf-8"), block=False)
        except queue.Full:
            logger.warning("Message queue is full. Discarding incoming message.")

    def run(self):
        try:
            logger.info(f"Connecting to {self.config.broker}:{self.config.port}...")
            self.client.connect(self.config.broker, self.config.port, 60)
            self.client.loop_start()

            while True:
                try:
                    payload = self.message_queue.get(timeout=1.0)
                    report = self.engine.process_payload(payload)
                    
                    if report:
                        print(json.dumps(report, ensure_ascii=False), flush=True)

                except queue.Empty:
                    with self.watchdog_lock:
                        time_since_last_msg = time.time() - self.last_message_timestamp
                    
                    if time_since_last_msg > self.config.fatal_watchdog_timeout_sec:
                        raise FatalWatchdogError(f"Không nhận được message trong hơn {self.config.fatal_watchdog_timeout_sec} giây.")
                    
                    if time_since_last_msg > self.config.watchdog_timeout_sec and (time.time() - self.last_reconnect_attempt > 60):
                        logger.warning(f"WATCHDOG: Không có message trong {time_since_last_msg:.0f}s. Đang kết nối lại MQTT...")
                        try:
                            self.client.reconnect()
                        except Exception as e:
                            logger.error(f"Kết nối lại MQTT thất bại: {e}")
                        self.last_reconnect_attempt = time.time()
        
        except FatalWatchdogError as e:
            logger.critical(f"FATAL WATCHDOG: {e}. Đang tắt tiến trình.")
        except KeyboardInterrupt:
            logger.info("Nhận tín hiệu dừng (Ctrl+C). Đang tắt tiến trình.")
        except Exception as e:
            logger.critical(f"Lỗi không xác định trong vòng lặp chính: {e}", exc_info=True)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Dọn dẹp hoàn tất. Tiến trình đã thoát.")

# ======================================================================
# ĐIỂM KHỞI CHẠY CHÍNH
# ======================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone Water Level Data Processor.")
    parser.add_argument('--broker', required=True, help='MQTT Broker address')
    parser.add_argument('--port', type=int, required=True, help='MQTT Broker port')
    parser.add_argument('--username', default='', help='MQTT username (optional)')
    parser.add_argument('--password', default='', help='MQTT password (optional)')
    parser.add_argument('--topic', dest='topics', action='append', required=True, help='MQTT topic to subscribe to (can be specified multiple times)')
    args = parser.parse_args()

    config_parser = configparser.ConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(base_dir, 'water_processor_config.ini')
    
    if not os.path.exists(config_file_path):
        logger.critical(f"LỖI NGHIÊM TRỌNG: Không tìm thấy file cấu hình '{config_file_path}'. Vui lòng tạo file này. Đang thoát.")
        sys.exit(1)
    
    config_parser.read(config_file_path, encoding='utf-8')
    logger.info(f"Đã tải cấu hình thành công từ '{config_file_path}'.")

    try:
        engine_config = config_parser['WaterEngine']
        watchdog_config = config_parser['WatchdogAndPerformance']

        args.history_size = engine_config.getint('history_size', 36)
        args.trend_window_size = engine_config.getint('trend_window_size', 12)
        args.valid_range_min = engine_config.getfloat('valid_range_min', 0.0)
        args.valid_range_max = engine_config.getfloat('valid_range_max', 50.0)
        args.spike_threshold_std_dev = engine_config.getfloat('spike_threshold_std_dev', 4.0)
        
        args.watchdog_timeout_sec = watchdog_config.getint('watchdog_timeout_sec', 60)
        args.fatal_watchdog_timeout_sec = watchdog_config.getint('fatal_watchdog_timeout_sec', 300)

    except (configparser.NoSectionError, KeyError, ValueError) as e:
        logger.critical(f"LỖI NGHIÊM TRỌNG: Lỗi đọc file cấu hình '{config_file_path}': {e}. Đang thoát.")
        sys.exit(1)

    try:
        processor = MqttProcessor(config=args, topics=args.topics)
        processor.run()
    except Exception as e:
        logger.critical(f"Lỗi khi khởi tạo chương trình: {e}", exc_info=True)
        sys.exit(1)
