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
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

# --- Các hằng số cấu hình hệ thống ---
HISTORY_SAMPLES = 3600             # Phân tích xu hướng trên 1 giờ dữ liệu (nếu 1 điểm/giây)
MIN_SAMPLES_FOR_TREND = 600        # Cần ít nhất 10 phút dữ liệu để tính xu hướng
MAX_PHYSICALLY_POSSIBLE_MPS = 100.0 # Lớp 1: Ngưỡng vận tốc phi vật lý (100 m/s)

# Cấu hình Lớp 2
ALERT_THRESHOLD_MPS = 1.5          # Ngưỡng kích hoạt kiểm tra
CONSISTENCY_WINDOW = 4             # Kiểm tra trong 4 điểm dữ liệu
CONSISTENCY_AVG_VEL_THRESHOLD_MPS = 2.0 # Vận tốc trung bình trong cửa sổ phải > 2.0 m/s để xác nhận

class KalmanFilter3D_6State:
    """Bộ lọc Kalman 3D, 6 trạng thái để ước tính vị trí và vận tốc."""
    def __init__(self, dt=1.0, process_noise_xy=1e-7, process_noise_z=1e-6, measurement_noise_xy=1e-3, measurement_noise_z=5e-3):
        self.x = np.zeros((6, 1))
        self.P = np.eye(6) * 1000.
        self.F = np.eye(6)
        self.F[0, 3], self.F[1, 4], self.F[2, 5] = dt, dt, dt
        self.Q = np.eye(6)
        # Sửa lỗi TypeError: không thể giải nén đối tượng float
        self.Q[0,0] = self.Q[1,1] = self.Q[3,3] = self.Q[4,4] = process_noise_xy
        self.Q[2,2] = self.Q[5,5] = process_noise_z
        self.H = np.zeros((3, 6))
        self.H[0, 0], self.H[1, 1], self.H[2, 2] = 1, 1, 1
        self.R = np.eye(3)
        # Sửa lỗi TypeError: không thể giải nén đối tượng float
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
        self.x[3:6] = 0
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
        # 1. Tiền xử lý & Giám định
        try:
            # Ghi log dữ liệu thô nhận được để dễ dàng gỡ lỗi
            logger.info(f"Received raw data: {payload.strip()}")
            parts = payload.split(',')
            if len(parts) < 15 or not payload.startswith('$GNGGA'): return None
            fix_quality = int(parts[6])
            if fix_quality not in [4, 5]:
                logger.warning(f"Discarding message. Fix quality: {fix_quality} (Required: 4 or 5)")
                return None
            
            lat, lon = convert_nmea_to_decimal(parts[2], parts[3]), convert_nmea_to_decimal(parts[4], parts[5])
            height = float(parts[9]) - float(parts[11])
            utc_time_str = parts[1]
            today = datetime.now(timezone.utc).date()
            h, m, s = int(utc_time_str[0:2]), int(utc_time_str[2:4]), float(utc_time_str[4:])
            sec, microsec = int(s), int((s - int(s)) * 1_000_000)
            dt_object = datetime(today.year, today.month, today.day, h, m, sec, microsec, tzinfo=timezone.utc)
            timestamp = dt_object.timestamp()

            if any(v is None for v in [lat, lon, height]): return None
            
            current_point = {"lat": lat, "lon": lon, "h": height, "ts": timestamp}
            measurement = np.array([[lat], [lon], [height]])
        except (ValueError, IndexError): return None

        event_confirmed = False
        raw_velocity_mps = 0
        dt = 1.0

        # Lớp 1 & 2: Vệ sĩ & Lính gác
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

        # Lớp 3: Bộ não Phân tích
        if not self.history: self.kf.x[0:3] = measurement
        self.kf.predict(dt)
        self.kf.update(measurement)
        
        filtered_pos = self.kf.x[0:3].flatten()
        self.history.append({"ts": timestamp, "lat": filtered_pos[0], "lon": filtered_pos[1], "h": filtered_pos[2]})

        trend_results = self._analyze_trend()

        # Giai đoạn thu thập dữ liệu (10 phút đầu)
        if not trend_results:
            # Chỉ gửi báo cáo trạng thái nếu lý do là chưa đủ mẫu
            if len(self.history) < MIN_SAMPLES_FOR_TREND:
                # Tạo báo cáo trạng thái để led.py có thể hiển thị tiến độ
                status_report = {
                    "type": "collection_status",
                    "collected": len(self.history),
                    "total": MIN_SAMPLES_FOR_TREND
                }
                return status_report
            return None # Trả về None nếu hồi quy thất bại hoặc các lý do khác
        
        # Giai đoạn phân tích (sau 10 phút)
        # Bộ tổng hợp Quyết định
        final_velocity_for_cruden = trend_results["velocity_mmps"]
        if event_confirmed:
            final_velocity_for_cruden = raw_velocity_mps * 1000

        # Tạo báo cáo phân tích cuối cùng với cấu trúc chi tiết
        report = {
            "type": "ultimate_analysis_report", # Type này tương thích với led.py đã được sửa đổi
            "timestamp": timestamp,
            "value": final_velocity_for_cruden,
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
        if len(nmea_coord) > 7 and direction in ['E', 'W']:
            degrees = float(nmea_coord[:3])
            minutes = float(nmea_coord[3:])
        else:
            degrees = float(nmea_coord[:2])
            minutes = float(nmea_coord[2:])
        if direction in ['S', 'W']:
            return -(degrees + minutes / 60.0)
        return degrees + minutes / 60.0
    except (ValueError, IndexError):
        return None

class VelocityCalculator:
    def __init__(self):
        self.processor = Ultimate_RTK_Analyzer()

    def on_message(self, client, userdata, msg):
        try:
            report = self.processor.process_measurement(msg.payload.decode("utf-8"))
            if report:
                # Gửi báo cáo (dù là status hay analysis) về cho led.py
                print(json.dumps(report, ensure_ascii=False))
                sys.stdout.flush()
                
                # Chỉ ghi log phân tích chi tiết vào file shifting.log khi có báo cáo cuối cùng
                if report.get("type") == "ultimate_analysis_report":
                    log_vel = report['trend_analysis']['long_term_velocity_mmps']
                    log_accel = report['trend_analysis']['long_term_acceleration_mmps2']
                    log_event = " ***EVENT CONFIRMED!***" if report.get('event_confirmed') else ""
                    log_fix = report['quality']['rtk_fix_status']
                    logger.info(f"V_trend={log_vel:.2f} mm/s | A_trend={log_accel:.3f} mm/s² | Fix={log_fix}{log_event}")

        except Exception as e:
            logger.error(f"Lỗi trong on_message: {e}\n{traceback.format_exc()}")

def main(broker, port, username, password, topics):
    calculator = VelocityCalculator()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if username:
        client.username_pw_set(username, password)
    client.on_message = calculator.on_message

    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("Kết nối MQTT broker thành công.")
            for topic in topics:
                client.subscribe(topic)
                logger.info(f"Đã subscribe tới: {topic}")
        else:
            logger.error(f"Kết nối MQTT thất bại, mã lỗi: {rc}")
    client.on_connect = on_connect
    try:
        client.connect(broker, int(port), 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Nhận tín hiệu dừng, đang thoát...")
    except Exception as e:
        logger.error(f"Lỗi kết nối hoặc vòng lặp chính: {e}")
    finally:
        client.disconnect()
        logger.info("Đã ngắt kết nối MQTT.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ultimate GNSS-RTK Analysis Engine for Landslide Detection.")
    parser.add_argument('--broker', required=True, help='Địa chỉ MQTT broker')
    parser.add_argument('--port', type=int, required=True, help='Cổng MQTT broker')
    parser.add_argument('--username', help='Username MQTT')
    parser.add_argument('--password', help='Password MQTT')
    
    # === SỬA LỖI: Chỉ nhận và xử lý các topic GNSS ===
    # 1. Chấp nhận '--gnss-topic' và lưu vào danh sách 'gnss_topics'.
    parser.add_argument('--gnss-topic', dest='gnss_topics', action='append', 
                        help='Topic GNSS để subscribe. Có thể dùng nhiều lần.')
                        
    # 2. Chấp nhận các đối số khác mà led.py gửi qua để không báo lỗi, nhưng chúng ta sẽ bỏ qua chúng.
    parser.add_argument('--water-topic', action='append', help='(Bỏ qua) Topic mực nước.')
    parser.add_argument('--publish-topic', help='(Bỏ qua) Topic để publish kết quả.')
    parser.add_argument('--water-warn-threshold', type=float, help='(Bỏ qua) Ngưỡng cảnh báo mực nước.')
    parser.add_argument('--water-crit-threshold', type=float, help='(Bỏ qua) Ngưỡng nguy hiểm mực nước.')
    parser.add_argument('--pid-file', help='File để ghi Process ID (PID)')
    
    # Phân tích các đối số từ dòng lệnh
    args = parser.parse_args()

    # 3. Kiểm tra xem có topic GNSS nào được cung cấp không. Nếu không, thoát.
    if not args.gnss_topics:
        logger.error("Lỗi nghiêm trọng: Không có --gnss-topic nào được cung cấp. Không thể hoạt động.")
        sys.exit(1)

    # Ghi PID file nếu được yêu cầu
    if args.pid_file:
        try:
            import os
            pid = os.getpid()
            with open(args.pid_file, 'w') as f:
                f.write(str(pid))
            logger.info(f"Đã ghi PID {pid} vào file {args.pid_file}")
        except IOError as e:
            logger.error(f"Không thể ghi PID file: {e}")
            sys.exit(1)
            
    try:
        # 4. Gọi hàm main và chỉ truyền vào danh sách các topic GNSS.
        main(broker=args.broker, port=args.port, username=args.username, 
             password=args.password, topics=args.gnss_topics)
    finally:
        # Xóa PID file khi thoát
        if args.pid_file:
            try:
                import os
                if os.path.exists(args.pid_file):
                    os.remove(args.pid_file)
                    logger.info(f"Đã xóa PID file: {args.pid_file}")
            except IOError as e:
                logger.error(f"Không thể xóa PID file: {e}")
