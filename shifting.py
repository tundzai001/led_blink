import paho.mqtt.client as mqtt
import json
import math
from datetime import datetime, timezone
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import argparse
import traceback
import numpy as np
from collections import deque
from scipy.optimize import curve_fit
import warnings
from scipy.optimize import OptimizeWarning
import os

# --- Cấu hình logging nâng cao với xoay vòng và thư mục riêng ---
LOG_DIRECTORY = "shifting"
os.makedirs(LOG_DIRECTORY, exist_ok=True)

logger = logging.getLogger("shifting")
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - SHIFTING - %(levelname)s - %(message)s')

log_file_path = os.path.join(LOG_DIRECTORY, "shifting.log")
file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=30, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

error_log_file_path = os.path.join(LOG_DIRECTORY, "shifting_error.log")
error_handler = TimedRotatingFileHandler(error_log_file_path, when='midnight', interval=1, backupCount=30, encoding='utf-8')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)
logger.addHandler(error_handler)

# --- Các hằng số cấu hình hệ thống ---
HISTORY_SAMPLES = 3600
MIN_SAMPLES_FOR_TREND = 600
MAX_PHYSICALLY_POSSIBLE_MPS = 100.0
ALERT_THRESHOLD_MPS = 1.5
CONSISTENCY_WINDOW = 4

# Tần suất báo cáo theo nhóm mức độ nguy hiểm
SLOW_LEVELS_INTERVAL = 60
MEDIUM_LEVELS_INTERVAL = 15
FAST_LEVELS_INTERVAL = 3

class KalmanFilter3D_6State:
    def __init__(self, dt=1.0, process_noise_xy=1e-7, process_noise_z=1e-6, measurement_noise_xy=1e-3, measurement_noise_z=5e-3):
        self.x = np.zeros((6, 1)); self.P = np.eye(6) * 1000.; self.F = np.eye(6)
        self.F[0, 3], self.F[1, 4], self.F[2, 5] = dt, dt, dt; self.Q = np.eye(6)
        self.Q[0,0] = self.Q[1,1] = self.Q[3,3] = self.Q[4,4] = process_noise_xy
        self.Q[2,2] = self.Q[5,5] = process_noise_z; self.H = np.zeros((3, 6))
        self.H[0, 0], self.H[1, 1], self.H[2, 2] = 1, 1, 1; self.R = np.eye(3)
        self.R[0,0] = self.R[1,1] = measurement_noise_xy; self.R[2,2] = measurement_noise_z
    def predict(self, dt):
        self.F[0, 3], self.F[1, 4], self.F[2, 5] = dt, dt, dt; self.x = self.F @ self.x
        self.P = self.F @ self.P @ self.F.T + self.Q
    def update(self, z):
        y = z - self.H @ self.x; S = self.H @ self.P @ self.H.T + self.R
        try:
            K = self.P @ self.H.T @ np.linalg.inv(S)
            self.x = self.x + K @ y
            self.P = (np.eye(6) - K @ self.H) @ self.P
        except np.linalg.LinAlgError:
            logger.warning("Ma trận hiệp phương sai S suy biến. Bỏ qua bước cập nhật Kalman.")
            pass
    def reset(self, z):
        self.x[0:3] = z; self.x[3:6] = np.zeros((3,1)); self.P = np.eye(6) * 10.0

def quadratic_func(t, a, b, c):
    return 0.5 * a * t**2 + b * t + c

class Ultimate_RTK_Analyzer:
    def __init__(self, classification_table):
        self.kf = KalmanFilter3D_6State()
        self.history = deque(maxlen=HISTORY_SAMPLES)
        self.sentry_buffer = deque(maxlen=CONSISTENCY_WINDOW)
        self.last_valid_point = None; self.last_report_time = 0
        self.current_classification_name = "COLLECTING"
        self.classification_table = sorted(
            [item for item in classification_table if item.get('mm_giay') is not None and str(item.get('mm_giay')).strip() != ''],
            key=lambda x: float(x.get('mm_giay')), reverse=True
        )
        self.FAST_LEVEL_NAMES = ["Cực kỳ nhanh", "Rất nhanh", "Nhanh"]
        self.MEDIUM_LEVEL_NAMES = ["Trung bình", "Chậm"]
        self.last_analysis_time = 0

    def _get_classification_from_velocity(self, velocity_mmps):
        for level in self.classification_table:
            threshold = float(level.get('mm_giay', 0))
            if velocity_mmps >= threshold:
                return level.get('name', 'Không xác định')
        return self.classification_table[-1].get('name', 'Cực kỳ chậm') if self.classification_table else 'Không xác định'

    def _analyze_trend(self):
        if len(self.history) < MIN_SAMPLES_FOR_TREND: return None
        timestamps = np.array([p['ts'] for p in self.history]); t_norm = timestamps - timestamps[0]
        lat = np.array([p['lat'] for p in self.history]); lon = np.array([p['lon'] for p in self.history]); h = np.array([p['h'] for p in self.history])
    
        # Nới lỏng giới hạn gia tốc, chỉ giới hạn vận tốc
        bounds = ([-np.inf, -1.0, -np.inf], [np.inf, 1.0, np.inf])

        try:
            lat_lin_fit = np.polyfit(t_norm, lat, 1)
            lon_lin_fit = np.polyfit(t_norm, lon, 1)
            h_lin_fit = np.polyfit(t_norm, h, 1)
            p0_lat = [0, lat_lin_fit[0], lat_lin_fit[1]]
            p0_lon = [0, lon_lin_fit[0], lon_lin_fit[1]]
            p0_h = [0, h_lin_fit[0], h_lin_fit[1]]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", OptimizeWarning)
                popt_lat, _ = curve_fit(quadratic_func, t_norm, lat, p0=p0_lat, bounds=bounds, maxfev=10000)
                popt_lon, _ = curve_fit(quadratic_func, t_norm, lon, p0=p0_lon, bounds=bounds, maxfev=10000)
                popt_h, _ = curve_fit(quadratic_func, t_norm, h, p0=p0_h, bounds=bounds, maxfev=10000)
        except (RuntimeError, ValueError) as e:
            logger.warning(f"Lỗi trong quá trình curve_fit: {e}. Bỏ qua lần phân tích này.")
            return None
    
        accel_lat, vel_lat, _ = popt_lat; accel_lon, vel_lon, _ = popt_lon; accel_h, vel_h, _ = popt_h
        current_lat_rad = math.radians(self.history[-1]['lat'])
        m_per_deg_lat = 111320; m_per_deg_lon = 111320 * math.cos(current_lat_rad)
        vx_mps = vel_lon * m_per_deg_lon; vy_mps = vel_lat * m_per_deg_lat; vz_mps = vel_h
        horizontal_velocity_mps = math.sqrt(vx_mps**2 + vy_mps**2)
        vertical_velocity_mps = vz_mps
        velocity_3d_mps = math.sqrt(horizontal_velocity_mps**2 + vertical_velocity_mps**2)
        direction_rad = math.atan2(vx_mps, vy_mps)
        direction_degrees = math.degrees(direction_rad)
        if direction_degrees < 0: direction_degrees += 360
        ax_mps2 = accel_lon * m_per_deg_lon; ay_mps2 = accel_lat * m_per_deg_lat
        horizontal_acceleration_mps2 = math.sqrt(ax_mps2**2 + ay_mps2**2)
        start_point = self.history[0]
        current_point_filtered = {"lat": self.kf.x[0,0], "lon": self.kf.x[1,0], "h": self.kf.x[2,0]}
        total_displacement_m = haversine_3d(start_point, current_point_filtered)
        
        return {
            "velocity_3d_mmps": velocity_3d_mps * 1000,
            "horizontal_velocity_mmps": horizontal_velocity_mps * 1000,
            "vertical_velocity_mps": vertical_velocity_mps * 1000,
            "direction_degrees": direction_degrees,
            "horizontal_acceleration_mmps2": horizontal_acceleration_mps2 * 1000,
            "total_displacement_m": total_displacement_m
        }

    def process_measurement(self, payload):
        try:
            parts = payload.split(',')
            if len(parts) < 15 or not payload.startswith('$GNGGA'): return None
            fix_quality = int(parts[6])
            if fix_quality not in [4, 5]: return None
            lat, lon = convert_nmea_to_decimal(parts[2], parts[3]), convert_nmea_to_decimal(parts[4], parts[5])
            height = float(parts[9]) + float(parts[11])
            utc_time_str = parts[1]; today = datetime.now(timezone.utc).date()
            h, m, s = int(utc_time_str[0:2]), int(utc_time_str[2:4]), float(utc_time_str[4:])
            sec, microsec = int(s), int((s - int(s)) * 1_000_000)
            dt_object = datetime(today.year, today.month, today.day, h, m, sec, microsec, tzinfo=timezone.utc)
            timestamp = dt_object.timestamp()
            if any(v is None for v in [lat, lon, height]): return None
            current_point = {"lat": lat, "lon": lon, "h": height, "ts": timestamp}
            measurement = np.array([[lat], [lon], [height]])
        except (ValueError, IndexError): return None

        dt = 1.0
        if self.last_valid_point:
            dt = current_point["ts"] - self.last_valid_point["ts"]
            if dt > 0:
                raw_velocity_mps = haversine_3d(current_point, self.last_valid_point) / dt
                if raw_velocity_mps > MAX_PHYSICALLY_POSSIBLE_MPS: return None
                self.sentry_buffer.append(raw_velocity_mps > ALERT_THRESHOLD_MPS)
                if len(self.sentry_buffer) == CONSISTENCY_WINDOW and sum(self.sentry_buffer) / CONSISTENCY_WINDOW > 0.75:
                    logger.critical(f"LỚP 2: SỰ KIỆN DỊCH CHUYỂN NHANH ĐƯỢC XÁC NHẬN!")
                    self.kf.reset(measurement); self.last_report_time = timestamp
                    event_velocity_mmps = raw_velocity_mps * 1000
                    return {"type": "ultimate_analysis_report", "timestamp": timestamp, "classification_velocity_mm_s": event_velocity_mmps,
                            "classification_name": self._get_classification_from_velocity(event_velocity_mmps), "event_confirmed": True, "analysis_data": {}}
        self.last_valid_point = current_point
        if not self.history: self.kf.x[0:3] = measurement
        self.kf.predict(dt); self.kf.update(measurement)
        self.history.append({"ts": timestamp, "lat": self.kf.x[0,0], "lon": self.kf.x[1,0], "h": self.kf.x[2,0]})
        
        if len(self.history) < MIN_SAMPLES_FOR_TREND:
            return {"type": "collection_status", "collected": len(self.history), "total": MIN_SAMPLES_FOR_TREND}
        
        current_time = time.time()
        if current_time - self.last_analysis_time < 15:
            return None 
        self.last_analysis_time = current_time

        trend_results = self._analyze_trend()
        if not trend_results: return None

        previous_classification = self.current_classification_name
        trend_velocity_mmps = trend_results["velocity_3d_mmps"]
        new_classification = self._get_classification_from_velocity(trend_velocity_mmps)
        self.current_classification_name = new_classification
        
        classification_has_changed = (new_classification != previous_classification)
        if classification_has_changed: logger.info(f"Phân loại dịch chuyển đã thay đổi: '{previous_classification}' -> '{new_classification}'")
            
        time_since_last_report = time.time() - self.last_report_time; timer_is_up = False
        if new_classification in self.FAST_LEVEL_NAMES:
            if time_since_last_report >= FAST_LEVELS_INTERVAL: timer_is_up = True
        elif new_classification in self.MEDIUM_LEVEL_NAMES:
            if time_since_last_report >= MEDIUM_LEVELS_INTERVAL: timer_is_up = True
        else:
            if time_since_last_report >= SLOW_LEVELS_INTERVAL: timer_is_up = True
        
        if not (classification_has_changed or timer_is_up): return None

        self.last_report_time = time.time()
        return {
            "type": "ultimate_analysis_report", "timestamp": timestamp,
            "classification_velocity_mm_s": trend_velocity_mmps, "classification_name": new_classification,
            "event_confirmed": False, "analysis_data": trend_results
        }

def haversine_3d(p1, p2):
    R = 6371000; lat1, lon1, h1, lat2, lon2, h2 = p1["lat"], p1["lon"], p1["h"], p2["lat"], p2["lon"], p2["h"]
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    dist_2d = R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)); return math.sqrt(dist_2d**2 + (h2 - h1)**2)

def convert_nmea_to_decimal(nmea_coord, direction):
    try:
        if not nmea_coord: return None
        degrees_str = nmea_coord[:-7]
        minutes_str = nmea_coord[-7:]
        degrees = float(degrees_str)
        minutes = float(minutes_str)
        decimal = degrees + minutes / 60.0
        return -decimal if direction in ['S', 'W'] else decimal
    except (ValueError, IndexError): return None

class MqttProcessor:
    def __init__(self, classification_table):
        self.analyzer = Ultimate_RTK_Analyzer(classification_table)
    def on_message(self, client, userdata, msg):
        try:
            report = self.analyzer.process_measurement(msg.payload.decode("utf-8"))
            if report:
                print(json.dumps(report, ensure_ascii=False)); sys.stdout.flush()
                if report.get("type") == "ultimate_analysis_report":
                    log_vel = report['classification_velocity_mm_s']; log_name = report['classification_name']
                    reason = "Class Change" if (time.time() - self.analyzer.last_report_time) < 1 else "Periodic"
                    if report.get("event_confirmed"): reason = "EVENT"
                    logger.info(f"REPORT ({reason}) | Class: '{log_name}' | V={log_vel:.4f} mm/s")
        except Exception as e: logger.error(f"Lỗi on_message: {e}\n{traceback.format_exc()}")

def on_connect(client, userdata, flags, reason_code, properties):
    # Luôn truy cập thuộc tính .value của đối tượng ReasonCode
    if reason_code.value == 0:
        logger.info("Kết nối MQTT broker thành công.")
        for topic in userdata['topics']: client.subscribe(topic); logger.info(f"Đã subscribe tới: {topic}")
    else:
        logger.error(f"Kết nối MQTT thất bại, mã lỗi: {reason_code.value}")

def main(broker, port, username, password, topics, classification_table):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata={'topics': topics})
    if username: client.username_pw_set(username, password)
    processor = MqttProcessor(classification_table)
    client.on_connect = on_connect; client.on_message = processor.on_message
    try:
        logger.info(f"Đang kết nối tới {broker}:{port}...")
        client.connect(broker, int(port), 60); client.loop_start()
        logger.info("Vòng lặp MQTT đã bắt đầu trong luồng nền.")
        last_heartbeat_time = time.time()
        last_log_day = datetime.now(timezone.utc).day
        while True:
            current_time = datetime.now(timezone.utc)
            if current_time.day != last_log_day and processor.analyzer.kf.x[0,0] != 0:
                logger.info(f"Phát hiện ngày mới ({current_time.day}). Đang ghi lại điểm chốt dài hạn...")
                checkpoint = {
                    "timestamp": current_time.timestamp(), "datetime_utc": current_time.isoformat(),
                    "lat": processor.analyzer.kf.x[0,0], "lon": processor.analyzer.kf.x[1,0], "h": processor.analyzer.kf.x[2,0]
                }
                try:
                    long_term_log_path = os.path.join(LOG_DIRECTORY, "long_term_displacement.jsonl")
                    with open(long_term_log_path, "a", encoding='utf-8') as f:
                        f.write(json.dumps(checkpoint) + "\n")
                    logger.info("Đã ghi thành công điểm chốt dài hạn.")
                except Exception as e: logger.error(f"Không thể ghi điểm chốt dài hạn: {e}")
                last_log_day = current_time.day
            if time.time() - last_heartbeat_time > 30:
                print(json.dumps({"type": "HEARTBEAT", "timestamp": time.time()})); sys.stdout.flush()
                last_heartbeat_time = time.time()
            time.sleep(1)
    except KeyboardInterrupt: logger.info("Nhận tín hiệu dừng...")
    except Exception as e: logger.error(f"Lỗi vòng lặp chính: {e}")
    finally:
        logger.info("Đang dừng và ngắt kết nối MQTT..."); client.loop_stop(); client.disconnect()
        logger.info("Đã ngắt kết nối MQTT.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ultimate GNSS-RTK Analysis Engine for Landslide Detection.")
    parser.add_argument('--broker', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--username'); parser.add_argument('--password')
    parser.add_argument('--topic', dest='topics', action='append', required=True)
    parser.add_argument('--classification-table', required=True, help='Chuỗi JSON chứa bảng phân loại tốc độ.')
    try:
        args = parser.parse_args()
        classification_table = json.loads(args.classification_table)
        main(broker=args.broker, port=args.port, username=args.username, password=args.password, topics=args.topics, classification_table=classification_table)
    except Exception as e:
        logger.critical(f"Lỗi khởi động chương trình: {e}", exc_info=True)
        sys.exit(1)
