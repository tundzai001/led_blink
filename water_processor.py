# water_processor.py (Phiên bản cuối cùng, tinh giản, mặc định đầu vào là mét)

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

# Ngăn ghi log trùng lặp nếu file được import nhiều lần
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - WATER-PROCESSOR - %(levelname)s - %(message)s')
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File Handler
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
        logger.info(f"Data Engine initialized. Assuming input unit is METERS. History size: {self.config.history_size} points.")

    def process_payload(self, payload_str):
        try:
            payload = json.loads(payload_str)
            timestamp = payload.get("timestamp", time.time())
            value_in_meters = payload.get("value") # Dữ liệu vào đã là mét

            if value_in_meters is None:
                logger.warning(f"Payload does not contain 'value' field: {payload_str}")
                return None
            
            value_in_meters = float(value_in_meters)

            # 1. Validation: Kiểm tra giá trị có nằm trong phạm vi hợp lệ không
            if not (self.config.valid_range_min <= value_in_meters <= self.config.valid_range_max):
                logger.warning(f"REJECTED: Value {value_in_meters:.4f}m is outside valid range ({self.config.valid_range_min}m - {self.config.valid_range_max}m).")
                return None

            # 2. Outlier Rejection: Lọc nhiễu đột biến
            if len(self.history) >= 5:
                past_values_m = [item[1] for item in self.history]
                mean_m = sum(past_values_m) / len(past_values_m)
                std_dev_m = (sum([(x - mean_m) ** 2 for x in past_values_m]) / len(past_values_m)) ** 0.5
                
                # Chỉ lọc khi có sự biến động nhất định để tránh lọc sai ở trạng thái ổn định
                if std_dev_m > 0.01 and abs(value_in_meters - mean_m) > self.config.spike_threshold_std_dev * std_dev_m:
                    logger.warning(f"REJECTED (SPIKE): Value {value_in_meters:.4f}m is considered a spike compared to recent mean of {mean_m:.4f}m.")
                    return None
            
            self.history.append((timestamp, value_in_meters))

            # 3. Trend Analysis: Phân tích xu hướng
            rate_of_change_mm_per_min = 0.0
            if len(self.history) >= self.config.trend_window_size:
                trend_data = list(self.history)[-self.config.trend_window_size:]
                start_point, end_point = trend_data[0], trend_data[-1]
                delta_time_sec = end_point[0] - start_point[0]
                delta_value_m = end_point[1] - start_point[1]

                if delta_time_sec > 1: # Tránh chia cho 0
                    rate_of_change_m_per_sec = delta_value_m / delta_time_sec
                    rate_of_change_mm_per_min = rate_of_change_m_per_sec * 1000 * 60

            # 4. Create Report: Tạo báo cáo để gửi về led.py
            report = {
                "type": "processed_water_report",
                "timestamp": timestamp,
                "processed_value_meters": value_in_meters, # Luôn là mét
                "analysis": {
                    "rate_of_change_mm_per_min": round(rate_of_change_mm_per_min, 3)
                },
                "original_payload": payload
            }
            return report

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error(f"Error processing payload: '{payload_str}'. Details: {e}")
            return None
        except Exception as e:
            logger.critical(f"Unexpected error in DataEngine: {e}", exc_info=True)
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
                        raise FatalWatchdogError(f"No message received for over {self.config.fatal_watchdog_timeout_sec} seconds.")
                    
                    if time_since_last_msg > self.config.watchdog_timeout_sec and (time.time() - self.last_reconnect_attempt > 60):
                        logger.warning(f"WATCHDOG: No message for {time_since_last_msg:.0f}s. Attempting to reconnect MQTT...")
                        try:
                            self.client.reconnect()
                        except Exception as e:
                            logger.error(f"MQTT reconnect failed: {e}")
                        self.last_reconnect_attempt = time.time()
        
        except (FatalWatchdogError, KeyboardInterrupt) as e:
            logger.info(f"Shutting down: {e}")
        except Exception as e:
            logger.critical(f"Unhandled exception in main loop: {e}", exc_info=True)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Cleanup complete. Process will now exit.")

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
        logger.critical(f"FATAL: Configuration file '{config_file_path}' not found! Please create it. Exiting.")
        sys.exit(1)
    
    config_parser.read(config_file_path, encoding='utf-8')
    logger.info(f"Successfully loaded configuration from '{config_file_path}'.")

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
        logger.critical(f"FATAL: Error reading configuration file '{config_file_path}': {e}. Exiting.")
        sys.exit(1)

    try:
        processor = MqttProcessor(config=args, topics=args.topics)
        processor.run()
    except Exception as e:
        logger.critical(f"An unexpected error occurred during program initialization: {e}", exc_info=True)
        sys.exit(1)
