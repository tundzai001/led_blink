import paho.mqtt.client as mqtt
import json
import math
from datetime import datetime
import time
import logging
import sys
import argparse
from collections import defaultdict
import traceback

# Cấu hình logging để ghi ra file và hiển thị lỗi trên stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SHIFTING - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shifting.log', encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

# Các hằng số cho khoảng thời gian tính bằng giây
INTERVAL_SECONDS = {
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "month": 2592000 # Ước tính 30 ngày
}

class GNSSIntervalCalculator:
    def __init__(self, settings):
        self.settings = settings
        self.last_data_for_classification = {} 
        self.interval_start_points = defaultdict(dict)
        self.client = None
        self.running = False
        logger.info(f"Worker được khởi tạo với cấu hình: {self.settings}")

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        R = 6371000  # Bán kính Trái Đất (mét)
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def parse_gngga(self, sentence):
        try:
            if not sentence.startswith("$GNGGA"):
                return None
            parts = sentence.split(",")
            if len(parts) < 7 or parts[6] == '0': # Bỏ qua nếu fix quality không hợp lệ
                return None
            
            raw_time, raw_lat, lat_dir, raw_lon, lon_dir = parts[1], parts[2], parts[3], parts[4], parts[5]
            if not all([raw_time, raw_lat, lat_dir, raw_lon, lon_dir]):
                return None
            
            # 1. Tạo đối tượng `time` từ chuỗi thời gian thô
            utc_time_obj = datetime.strptime(raw_time, "%H%M%S.%f" if '.' in raw_time else "%H%M%S").time()
            
            # 2. SỬA LỖI: Kết hợp ngày UTC hiện tại với đối tượng `time` vừa tạo
            utc_datetime = datetime.combine(datetime.utcnow().date(), utc_time_obj)

            def convert_coordinate(coord_str, direction):
                idx = coord_str.find('.')
                if idx == -1 or idx < 3: # Kiểm tra để tránh lỗi index out of range
                    return None
                degrees = int(coord_str[:idx-2])
                minutes = float(coord_str[idx-2:])
                decimal = degrees + minutes / 60.0
                return -decimal if direction in ['S', 'W'] else decimal

            lat = convert_coordinate(raw_lat, lat_dir)
            lon = convert_coordinate(raw_lon, lon_dir)
            
            if lat is None or lon is None:
                return None
                
            return {"lat": lat, "lon": lon, "time": utc_datetime}

        except Exception as e:
            logger.error(f"Lỗi không xác định trong parse_gngga: {e} | Sentence: {sentence}")
            return None

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("MQTT Connected.")
            for topic in self.settings['topic']:
                client.subscribe(topic)
                logger.info(f"Subscribed to raw topic: {topic}")
        else:
            logger.error(f"MQTT Connection Failed with code {int(rc)}")

    def on_message(self, client, userdata, msg):
        try:
            gnss_sentence = msg.payload.decode('utf-8').strip()
            new_data = self.parse_gngga(gnss_sentence)
            if not new_data: return

            topic = msg.topic
            classification_velocity_mm_s = 0
            if topic in self.last_data_for_classification:
                prev_data = self.last_data_for_classification[topic]
                dist = self.haversine_distance(prev_data['lat'], prev_data['lon'], new_data['lat'], new_data['lon'])
                time_diff = (new_data['time'] - prev_data['time']).total_seconds()
                if time_diff > 0:
                    classification_velocity_mm_s = (dist / time_diff) * 1000
            self.last_data_for_classification[topic] = new_data

            if self.settings.get('calc_second'):
                if classification_velocity_mm_s > 0:
                    payload = {
                        "type": "gnss_velocity_report", "sensorname": f"GNSS_{topic.replace('/', '_')}",
                        "timestamp": time.time(), "value": classification_velocity_mm_s, "unit": "mm/giây",
                        "classification_velocity_mm_s": classification_velocity_mm_s
                    }
                    print(json.dumps(payload), flush=True)

            for interval_name, duration in INTERVAL_SECONDS.items():
                if self.settings.get(f'calc_{interval_name}'):
                    if interval_name not in self.interval_start_points[topic]:
                        self.interval_start_points[topic][interval_name] = new_data
                        logger.info(f"[{topic}] Bắt đầu chu kỳ '{interval_name}'. Đang đợi {duration} giây...")
                        continue

                    start_point = self.interval_start_points[topic][interval_name]
                    time_elapsed = (new_data['time'] - start_point['time']).total_seconds()

                    if time_elapsed >= duration:
                        logger.info(f"[{topic}] Hoàn thành chu kỳ '{interval_name}' sau {time_elapsed:.1f} giây.")
                        dist_interval = self.haversine_distance(start_point['lat'], start_point['lon'], new_data['lat'], new_data['lon'])
                        velocity_interval = dist_interval / time_elapsed
                        display_value = velocity_interval * duration
                        display_unit = f"m/{interval_name.replace('minute', 'phút').replace('hour', 'giờ').replace('day', 'ngày').replace('month', 'tháng')}"
                        payload = {
                            "type": "gnss_velocity_report", "sensorname": f"GNSS_{topic.replace('/', '_')}",
                            "timestamp": time.time(), "value": display_value, "unit": display_unit,
                            "classification_velocity_mm_s": classification_velocity_mm_s
                        }
                        print(json.dumps(payload), flush=True)
                        self.interval_start_points[topic][interval_name] = new_data
        except Exception as e:
            logger.error(f"Error processing message from {msg.topic}: {e}")

    def start(self):
        self.running = True
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        if self.settings.get('username'):
            self.client.username_pw_set(self.settings.get('username'), self.settings.get('password'))
        self.client.connect(self.settings['broker'], self.settings['port'], 60)
        self.client.loop_forever()
    
    def stop(self):
        self.running = False
        if self.client and self.client.is_connected():
            self.client.disconnect()

def main():
    try:
        parser = argparse.ArgumentParser(description="GNSS Velocity Calculator Worker")
        parser.add_argument("--broker", required=True)
        parser.add_argument("--port", type=int, required=True)
        parser.add_argument("--username", default="")
        parser.add_argument("--password", default="")
        parser.add_argument("--topic", action="append", required=True)
        parser.add_argument("--calc-second", action="store_true")
        parser.add_argument("--calc-minute", action="store_true")
        parser.add_argument("--calc-hour", action="store_true")
        parser.add_argument("--calc-day", action="store_true")
        parser.add_argument("--calc-month", action="store_true")
        args = parser.parse_args()
    
        settings = vars(args)
        calculator = GNSSIntervalCalculator(settings)
        calculator.start()
    except Exception:
        # Ghi lại traceback chi tiết vào file log chính
        logger.critical("CRITICAL ERROR DURING INITIALIZATION OR RUNTIME", exc_info=True)
        # Ghi thêm vào file crash log để dễ tìm
        with open('shifting_crash.log', 'w', encoding='utf-8') as f:
            f.write(f"Shifting.py crashed at {datetime.now()}\n\n")
            f.write(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()