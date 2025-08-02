import paho.mqtt.client as mqtt
import json
import math
from datetime import datetime, timezone
import time
import logging
import sys
import argparse
import traceback
import numpy as np
from collections import deque
from scipy.optimize import curve_fit

# --- Cấu hình logging ---
logger = logging.getLogger("shifting")
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - SHIFTING - %(levelname)s - %(message)s')
file_handler = logging.FileHandler("shifting.log", encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler(sys.stderr)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

# --- Các hằng số cấu hình hệ thống ---
HISTORY_SAMPLES = 3600
MIN_SAMPLES_FOR_TREND = 600
MAX_PHYSICALLY_POSSIBLE_MPS = 100.0

# Cấu hình Lớp 2
ALERT_THRESHOLD_MPS = 1.5
CONSISTENCY_WINDOW = 4
CONSISTENCY_AVG_VEL_THRESHOLD_MPS = 2.0

class KalmanFilter3D_6State:
    """Bộ lọc Kalman 3D, 6 trạng thái để ước tính vị trí và vận tốc."""
    def __init__(self, dt=1.0, process_noise_xy=1e-7, process_noise_z=1e-6, measurement_noise_xy=1e-3, measurement_noise_z=5e-3):
        self.x = np.zeros((6, 1))
        self.P = np.eye(6) * 1000.
        self.F = np.eye(6)
        self.F[0, 3], self.F[1, 4], self.F[2, 5] = dt, dt, dt
        self.Q = np.eye(6)
        self.Q[0,0] = self.Q[1,1] = self.Q[3,3] = self.Q[4,4] = process_noise_xy
        self.Q[2,2] = self.Q[5,5] = process_noise_z
        self.H = np.zeros((3, 6))
        self.H[0, 0], self.H[1, 1], self.H[2, 2] = 1, 1, 1
        self.R = np.eye(3)
        self.R[0,0] = self.R[1,1] = measurement_noise_xy
        self.R[2,2] = measurement_noise_z

    def predict(self, dt):
        self.F[0, 3], self.F[1, 4], self.F[2, 5] = dt, dt, dt
        self.x = self.F @ self.x
        self.P = self.F @ self.P @ self.F.T + self.Q

    def update(self, z):
        y = z - self.H @ self.x
        S = self.H @ self.P @ self.H.T + self.R
        K = self.P @ self.H.T @ np.linalg.inv(S)
        self.x = self.x + K @ y
        self.P = (np.eye(6) - K @ self.H) @ self.P

    def reset(self, z):
        """Reset mạnh bộ lọc để tin vào một thực tế mới."""
        self.x[0:3] = z
        self.x[3:6] = np.zeros((3,1))
        self.P = np.eye(6) * 10.0

def quadratic_func(t, a, b, c):
    """Hàm bậc hai cho hồi quy: pos = 0.5*a*t^2 + b*t + c"""
    return 0.5 * a * t**2 + b * t + c

class Ultimate_RTK_Analyzer:
    def __init__(self):
        self.kf = KalmanFilter3D_6State()
        self.history = deque(maxlen=HISTORY_SAMPLES)
        self.sentry_buffer = deque(maxlen=CONSISTENCY_WINDOW)
        self.last_valid_point = None

    def _analyze_trend(self):
        if len(self.history) < MIN_SAMPLES_FOR_TREND:
            return None

        timestamps = np.array([p['ts'] for p in self.history])
        t_norm = timestamps - timestamps[0]

        lat = np.array([p['lat'] for p in self.history])
        lon = np.array([p['lon'] for p in self.history])
        h = np.array([p['h'] for p in self.history])

        try:
            popt_lat, _ = curve_fit(quadratic_func, t_norm, lat)
            popt_lon, _ = curve_fit(quadratic_func, t_norm, lon)
            popt_h, _ = curve_fit(quadratic_func, t_norm, h)
        except RuntimeError:
            logger.warning("Hồi quy bậc hai thất bại, sẽ thử lại ở lần sau.")
            return None

        accel_lat, vel_lat = popt_lat[0], popt_lat[1]
        accel_lon, vel_lon = popt_lon[0], popt_lon[1]
        accel_h, vel_h = popt_h[0], popt_h[1]

        current_lat_rad = math.radians(self.history[-1]['lat'])
        m_per_deg_lat = 111320
        m_per_deg_lon = 111320 * math.cos(current_lat_rad)

        vx_mps, vy_mps, vz_mps = vel_lon * m_per_deg_lon, vel_lat * m_per_deg_lat, vel_h
        ax_mps2, ay_mps2, az_mps2 = accel_lon * m_per_deg_lon, accel_lat * m_per_deg_lat, accel_h

        velocity_3d = math.sqrt(vx_mps**2 + vy_mps**2 + vz_mps**2)
        acceleration_3d = math.sqrt(ax_mps2**2 + ay_mps2**2 + az_mps2**2)

        return {"velocity_mmps": velocity_3d * 1000, "acceleration_mmps2": acceleration_3d * 1000}

    def process_measurement(self, payload):
        try:
            # logger.info(f"Received raw data: {payload.strip()}") # Bỏ comment nếu muốn debug
            parts = payload.split(',')
            if len(parts) < 15 or not payload.startswith('$GNGGA'):
                return {"type": "parsing_error", "reason": "Invalid GNGGA format or length", "raw_data": payload.strip()}

            fix_quality = int(parts[6])
            if fix_quality not in [4, 5]:
                # logger.warning(f"Discarding message. Fix quality: {fix_quality} (Required: 4 or 5)")
                return None

            lat, lon = convert_nmea_to_decimal(parts[2], parts[3]), convert_nmea_to_decimal(parts[4], parts[5])
            height = float(parts[9]) + float(parts[11]) # Height above ellipsoid
            utc_time_str = parts[1]
            today = datetime.now(timezone.utc).date()
            h, m, s = int(utc_time_str[0:2]), int(utc_time_str[2:4]), float(utc_time_str[4:])
            sec, microsec = int(s), int((s - int(s)) * 1_000_000)
            dt_object = datetime(today.year, today.month, today.day, h, m, sec, microsec, tzinfo=timezone.utc)
            timestamp = dt_object.timestamp()

            if any(v is None for v in [lat, lon, height]):
                return {"type": "parsing_error", "reason": "Failed to convert NMEA coordinates", "raw_data": payload.strip()}

            current_point = {"lat": lat, "lon": lon, "h": height, "ts": timestamp}
            measurement = np.array([[lat], [lon], [height]])

        except (ValueError, IndexError) as e:
            # SỬA LỖI: Báo cáo lỗi phân tích một cách tường minh để gỡ lỗi
            logger.error(f"LỖI PHÂN TÍCH DỮ LIỆU NMEA: {e}. Dữ liệu thô: '{payload.strip()}'")
            return {"type": "parsing_error", "reason": str(e), "raw_data": payload.strip()}

        event_confirmed = False
        raw_velocity_mps = 0
        dt = 1.0

        if self.last_valid_point:
            dt = current_point["ts"] - self.last_valid_point["ts"]
            if dt > 0:
                dist = haversine_3d(current_point, self.last_valid_point)
                raw_velocity_mps = dist / dt

                if raw_velocity_mps > MAX_PHYSICALLY_POSSIBLE_MPS:
                    logger.critical(f"LỚP 1: Loại bỏ điểm phi vật lý! V_thô={raw_velocity_mps:.1f} m/s")
                    return None

                is_high_velocity = raw_velocity_mps > ALERT_THRESHOLD_MPS
                self.sentry_buffer.append(raw_velocity_mps if is_high_velocity else 0)

                if len(self.sentry_buffer) == CONSISTENCY_WINDOW:
                    avg_vel_in_window = sum(self.sentry_buffer) / len(self.sentry_buffer)
                    if avg_vel_in_window > CONSISTENCY_AVG_VEL_THRESHOLD_MPS:
                        event_confirmed = True
                        logger.critical(f"LỚP 2: SỰ KIỆN NHANH ĐƯỢC XÁC NHẬN! V_avg={avg_vel_in_window:.1f} m/s")
                        self.kf.reset(measurement)

        self.last_valid_point = current_point

        if not self.history: self.kf.x[0:3] = measurement
        self.kf.predict(dt)
        self.kf.update(measurement)

        filtered_pos = self.kf.x[0:3].flatten()
        self.history.append({"ts": timestamp, "lat": filtered_pos[0], "lon": filtered_pos[1], "h": filtered_pos[2]})

        trend_results = self._analyze_trend()

        if not trend_results:
            if len(self.history) < MIN_SAMPLES_FOR_TREND:
                return {"type": "collection_status", "collected": len(self.history), "total": MIN_SAMPLES_FOR_TREND}
            return None

        final_velocity_for_cruden = trend_results["velocity_mmps"]
        if event_confirmed:
            final_velocity_for_cruden = raw_velocity_mps * 1000

        report = {
            "type": "ultimate_analysis_report",
            "timestamp": timestamp,
            "classification_velocity_mm_s": final_velocity_for_cruden,
            "event_confirmed": event_confirmed,
            "trend_analysis": {
                "long_term_velocity_mmps": round(trend_results["velocity_mmps"], 4),
                "long_term_acceleration_mmps2": round(trend_results["acceleration_mmps2"], 4)
            },
            "quality": {"rtk_fix_status": fix_quality},
            "filtered_position": { "lat": filtered_pos[0], "lon": filtered_pos[1], "h": filtered_pos[2] }
        }
        return report

# --- Các hàm và lớp phụ ---
def haversine_3d(p1, p2):
    R = 6371000
    lat1, lon1, h1 = p1["lat"], p1["lon"], p1["h"]
    lat2, lon2, h2 = p2["lat"], p2["lon"], p2["h"]
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    dist_2d = R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    dist_3d = math.sqrt(dist_2d**2 + (h2 - h1)**2)
    return dist_3d

def convert_nmea_to_decimal(nmea_coord, direction):
    try:
        if not nmea_coord: return None
        # Kinh độ có định dạng DDDMM.mmmm, Vĩ độ có định dạng DDMM.mmmm
        if len(nmea_coord) > 7 and direction in ['E', 'W']: # Kinh độ
            degrees = float(nmea_coord[:3])
            minutes = float(nmea_coord[3:])
        else: # Vĩ độ
            degrees = float(nmea_coord[:2])
            minutes = float(nmea_coord[2:])

        decimal = degrees + minutes / 60.0
        if direction in ['S', 'W']:
            return -decimal
        return decimal
    except (ValueError, IndexError):
        return None

class MqttProcessor:
    def __init__(self):
        self.analyzer = Ultimate_RTK_Analyzer()

    def on_message(self, client, userdata, msg):
        try:
            report = self.analyzer.process_measurement(msg.payload.decode("utf-8"))
            if report:
                print(json.dumps(report, ensure_ascii=False))
                sys.stdout.flush()

                if report.get("type") == "ultimate_analysis_report":
                    log_vel = report['trend_analysis']['long_term_velocity_mmps']
                    log_accel = report['trend_analysis']['long_term_acceleration_mmps2']
                    log_event = " ***EVENT CONFIRMED!***" if report.get('event_confirmed') else ""
                    log_fix = report['quality']['rtk_fix_status']
                    logger.info(f"V_trend={log_vel:.2f} mm/s | A_trend={log_accel:.3f} mm/s² | Fix={log_fix}{log_event}")

        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng trong on_message: {e}\n{traceback.format_exc()}")

# SỬA LỖI: Cập nhật chữ ký hàm on_connect cho paho-mqtt v2.0+
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.rc == 0:
        logger.info("Kết nối MQTT broker thành công.")
        # Lấy lại các topic từ userdata để subscribe lại khi tự động kết nối lại
        topics = userdata['topics']
        for topic in topics:
            client.subscribe(topic)
            logger.info(f"Đã subscribe tới: {topic}")
    else:
        logger.error(f"Kết nối MQTT thất bại, mã lỗi: {reason_code.rc} ({reason_code})")

def main(broker, port, username, password, topics):
    # Cải tiến: Lưu topics vào userdata để dùng khi kết nối lại
    user_data = {'topics': topics}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=user_data)

    if username:
        client.username_pw_set(username, password)
    
    client.on_connect = on_connect
    processor = MqttProcessor()
    client.on_message = processor.on_message

    try:
        logger.info(f"Đang kết nối tới {broker}:{port}...")
        client.connect(broker, int(port), 60)

        # Cải tiến: Chạy vòng lặp mạng ở nền và gửi nhịp tim chủ động
        client.loop_start()
        logger.info("Vòng lặp MQTT đã bắt đầu trong luồng nền.")

        last_heartbeat_time = time.time()
        HEARTBEAT_INTERVAL = 30 # Giây

        while True:
            current_time = time.time()
            if current_time - last_heartbeat_time > HEARTBEAT_INTERVAL:
                heartbeat_packet = {"type": "HEARTBEAT", "timestamp": current_time}
                print(json.dumps(heartbeat_packet))
                sys.stdout.flush()
                last_heartbeat_time = current_time
                # logger.info("Đã gửi nhịp tim chủ động.") # Có thể comment dòng này để giảm log
            
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Nhận tín hiệu dừng, đang thoát...")
    except Exception as e:
        logger.error(f"Lỗi kết nối hoặc vòng lặp chính: {e}")
    finally:
        logger.info("Đang dừng vòng lặp MQTT và ngắt kết nối...")
        client.loop_stop()
        client.disconnect()
        logger.info("Đã ngắt kết nối MQTT.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ultimate GNSS-RTK Analysis Engine for Landslide Detection.")
    parser.add_argument('--broker', required=True, help='Địa chỉ MQTT broker')
    parser.add_argument('--port', type=int, required=True, help='Cổng MQTT broker')
    parser.add_argument('--username', help='Username MQTT')
    parser.add_argument('--password', help='Password MQTT')
    parser.add_argument('--gnss-topic', dest='gnss_topics', action='append', help='Topic GNSS để subscribe. Có thể dùng nhiều lần.')
    parser.add_argument('--water-topic', action='append', help='(Bỏ qua) Topic mực nước.')
    parser.add_argument('--pid-file', help='File để ghi Process ID (PID)')

    args = parser.parse_args()

    if not args.gnss_topics:
        logger.error("Lỗi nghiêm trọng: Không có --gnss-topic nào được cung cấp. Không thể hoạt động.")
        sys.exit(1)

    pid_file_path = args.pid_file
    if pid_file_path:
        try:
            import os
            pid = os.getpid()
            with open(pid_file_path, 'w') as f:
                f.write(str(pid))
            logger.info(f"Đã ghi PID {pid} vào file {pid_file_path}")
        except IOError as e:
            logger.error(f"Không thể ghi PID file: {e}")
            sys.exit(1)

    # Cải tiến: Sử dụng try...finally để đảm bảo file PID được xóa
    try:
        main(broker=args.broker, port=args.port, username=args.username, password=args.password, topics=args.gnss_topics)
    finally:
        if pid_file_path:
            try:
                import os
                if os.path.exists(pid_file_path):
                    os.remove(pid_file_path)
                    logger.info(f"Đã xóa PID file: {pid_file_path}")
            except IOError as e:
                logger.error(f"Không thể xóa PID file: {e}")
