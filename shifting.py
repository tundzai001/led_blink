import paho.mqtt.client as mqtt
import json
import math
from datetime import datetime, timezone, timedelta
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import argparse
import traceback
import re
import numpy as np
from collections import deque
from scipy.stats import norm
import pywt
import os
import queue
import configparser
import psutil
import threading

class GNSSProcessingError(Exception):
    """Base exception for GNSS processing errors"""
    pass

class DataValidationError(GNSSProcessingError):
    """Raised when input data validation fails"""
    pass

class FilterConvergenceError(GNSSProcessingError):
    """Raised when particle filter fails to converge"""
    pass

class ConfigurationError(GNSSProcessingError):
    """Raised when configuration is invalid"""
    pass
# ======================================================================
# LỚP 0: CẤU HÌNH, LOGGING VÀ CÁC TIỆN ÍCH
# ======================================================================

TZ_UTC_7 = timezone(timedelta(hours=7))
LOG_DIRECTORY = "shifting_bayesian"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
logger = logging.getLogger("shifting_bayesian")
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - SHIFTING-BAYES - %(levelname)s - %(message)s')
formatter.converter = lambda *args: datetime.now(TZ_UTC_7).timetuple()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
log_file_path = os.path.join(LOG_DIRECTORY, "shifting.log")
file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=7, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class FatalWatchdogError(Exception):
    pass

class Constants:
    def __init__(self, config_parser):
        se_config = config_parser['ShiftingEngine']
        iv_config = config_parser['InputValidation']
        od_config = config_parser['OutlierDetection']
        pf_config = config_parser['ParticleFilter']
        rs_config = config_parser['RecoveryAndStability']
        ar_config = config_parser['AnalysisAndReporting']

        # [ShiftingEngine]
        self.INITIAL_STABILIZATION_PERIOD = se_config.getint('initial_stabilization_period', 600)
        self.LONG_TERM_HISTORY_MAXLEN = se_config.getint('long_term_history_maxlen', 900)
        
        # [InputValidation]
        self.MIN_SATELLITES = iv_config.getint('min_satellites', 5)
        self.MAX_HDOP = iv_config.getfloat('max_hdop', 4.0)
        self.MAX_TIME_DRIFT_SECONDS = iv_config.getfloat('max_time_drift_seconds', 10.0)
        
        # [OutlierDetection]
        self.SPATIAL_CONSISTENCY_BUFFER_SIZE = od_config.getint('spatial_consistency_buffer_size', 15)
        self.MIN_SPATIAL_CONSISTENCY_POINTS = od_config.getint('min_spatial_consistency_points', 5)
        self.SPATIAL_CONSISTENCY_STD_DEV_FACTOR = od_config.getfloat('spatial_consistency_std_dev_factor', 5.0)
        self.SPATIAL_ABSOLUTE_MAX_THRESHOLD = od_config.getfloat('spatial_absolute_max_threshold', 2.5)
        
        ### CẢI TIẾN 1.1: THAM SỐ MỚI CHO NGƯỠNG LỌC LEO THANG ###
        self.REJECT_STREAK_FOR_ESCALATION = od_config.getint('reject_streak_for_escalation', 30)
        self.ESCALATION_FACTOR_INCREMENT = od_config.getfloat('escalation_factor_increment', 0.1)
        self.MAX_ESCALATION_FACTOR = od_config.getfloat('max_escalation_factor', 2.5)
        self.DE_ESCALATION_FACTOR_DECREMENT = od_config.getfloat('de_escalation_factor_decrement', 0.05)
        
        # [ParticleFilter]
        self.MAX_PREDICTION_DT = pf_config.getfloat('max_prediction_dt', 5.0)
        self.MAX_UNCERTAINTY_METERS = pf_config.getfloat('max_uncertainty_meters', 5.0)
        self.PF_RESET_INTERVAL = pf_config.getint('pf_reset_interval', 20000)
        
        # [RecoveryAndStability]
        self.ORIGIN_FINDING_ATTEMPT_LIMIT = rs_config.getint('origin_finding_attempt_limit', 300)
        self.COOLDOWN_RECOVERY_BUFFER_SIZE = rs_config.getint('cooldown_recovery_buffer_size', 20)
        self.COOLDOWN_RECOVERY_STD_DEV_THRESHOLD = rs_config.getfloat('cooldown_recovery_std_dev_threshold', 0.8)
        
        ### CẢI TIẾN 1.2 & 2.1: THAM SỐ MỚI CHO TRẠNG THÁI VÀ PHỤC HỒI ###
        self.DEGRADED_STATE_THRESHOLD_REJECTS = rs_config.getint('degraded_state_threshold_rejects', 60)
        self.CRITICAL_STATE_THRESHOLD_SECONDS = rs_config.getint('critical_state_threshold_seconds', 900)
        self.GRACEFUL_REENTRY_TIME_GAP_S = rs_config.getfloat('graceful_reentry_time_gap_s', 30.0)

        # [AnalysisAndReporting]
        self.MIN_TRANSIENT_AMPLITUDE_M = ar_config.getfloat('min_transient_amplitude_m', 0.002)
        self.MAX_RESAMPLED_POINTS_FOR_WAVELET = ar_config.getint('max_resampled_points_for_wavelet', 100000)

        # Các hằng số ít thay đổi hơn
        self.STATIC_VELOCITY_THRESHOLD_M_PER_S = (0.5 / 1000) / (24 * 3600)
        self.CREEP_VELOCITY_THRESHOLD_M_PER_S = (2 / 1000) / (24 * 3600)
        self.CONSTANT_ACCELERATION_THRESHOLD_M_PER_S2 = 1e-9
        self.JERK_ACCELERATION_THRESHOLD_M_PER_S2 = 1e-7
        self.ANALYSIS_WINDOW_FOR_FREQUENCY = 100
        
        self.TIME_WRAP_THRESHOLD_HOURS = 12
        self.MAX_TIME_DRIFT_HOURS = 2
        self.MIN_ORIGIN_CANDIDATES = 50
        self.MEMORY_SAFETY_THRESHOLD_GB = 1.0
        self.CPU_LOAD_THRESHOLD = 0.90
        self.MAX_WAVELET_POINTS = 50000

def wgs84_to_enu(lat, lon, h, lat_ref, lon_ref, h_ref):
    R = 6378137.0
    f = 1/298.257223563
    e2 = 2*f - f**2
    lat_rad, lon_rad = map(math.radians, [lat, lon])
    lat_ref_rad, lon_ref_rad = map(math.radians, [lat_ref, lon_ref])
    
    sqrt_arg_ref = max(1e-12, 1 - e2 * math.sin(lat_ref_rad)**2)  # Đảm bảo > 0
    if sqrt_arg_ref <= 0:
        logger.error(f"Invalid sqrt argument in WGS84 conversion: {sqrt_arg_ref}")
        sqrt_arg_ref = 1e-12
    N_ref = R / math.sqrt(sqrt_arg_ref)
    
    x_ref = (h_ref + N_ref) * math.cos(lat_ref_rad) * math.cos(lon_ref_rad)
    y_ref = (h_ref + N_ref) * math.cos(lat_ref_rad) * math.sin(lon_ref_rad)
    z_ref = (h_ref + (1 - e2) * N_ref) * math.sin(lat_ref_rad)
    
    sqrt_arg_curr = max(1e-12, 1 - e2 * math.sin(lat_rad)**2)
    if sqrt_arg_curr <= 0:
        logger.error(f"Invalid sqrt argument in WGS84 conversion: {sqrt_arg_curr}")
        sqrt_arg_curr = 1e-12
    N_curr = R / math.sqrt(sqrt_arg_curr)
    
    x_curr = (h + N_curr) * math.cos(lat_rad) * math.cos(lon_rad)
    y_curr = (h + N_curr) * math.cos(lat_rad) * math.sin(lon_rad)
    z_curr = (h + (1 - e2) * N_curr) * math.sin(lat_rad)
    
    dx, dy, dz = x_curr - x_ref, y_curr - y_ref, z_curr - z_ref
    
    sl, cl = math.sin(lon_ref_rad), math.cos(lon_ref_rad)
    sf, cf = math.sin(lat_ref_rad), math.cos(lat_ref_rad)
    
    e = -sl*dx + cl*dy
    n = -sf*cl*dx - sf*sl*dy + cf*dz
    u = cf*cl*dx + cf*sl*dy + sf*dz
    
    return e, n, u

def haversine_3d(p1, p2):
    R = 6371000
    lat1, lon1, h1 = p1.get("lat"), p1.get("lon"), p1.get("h")
    lat2, lon2, h2 = p2.get("lat"), p2.get("lon"), p2.get("h")
    if any(v is None for v in [lat1, lon1, h1, lat2, lon2, h2]):
        return float('inf')
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    dist_2d = R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return math.sqrt(dist_2d**2 + (h2 - h1)**2)

def convert_nmea_to_decimal(nmea_coord, direction):
    try:
        if not nmea_coord or '.' not in nmea_coord: return None
        dot_index = nmea_coord.find('.')
        degrees_str = nmea_coord[:dot_index - 2]
        minutes_str = nmea_coord[dot_index - 2:]
        degrees = float(degrees_str)
        minutes = float(minutes_str)
        decimal = degrees + minutes / 60.0
        if direction in ['S', 'W']: decimal = -decimal
        return decimal
    except (ValueError, IndexError): return None

def verify_checksum(sentence):
    try:
        sentence = sentence.strip()
        parts = sentence.split('*')
        if not sentence.startswith('$') or len(parts) != 2: 
            return False
        
        main_part = parts[0][1:]
        checksum_part = parts[1]

        if len(checksum_part) != 2 or not all(c in '0123456789ABCDEFabcdef' for c in checksum_part):
            logger.debug(f"Invalid checksum format: '{checksum_part}'")
            return False
            
        calculated_checksum = 0
        for char in main_part: 
            calculated_checksum ^= ord(char)
        return f"{calculated_checksum:02X}" == checksum_part.upper()
    except (ValueError, IndexError, AttributeError) as e:
        logger.debug(f"Checksum verification failed: {e}")
        return False

class ParticleFilterGNSS:
    def __init__(self, num_particles=2000, process_noise_std=None):
        self.N = num_particles
        self.particles = np.zeros((self.N, 6))
        self.weights = np.ones(self.N) / self.N
        if process_noise_std is None:
            process_noise_std = [0.005, 0.005, 0.006, 0.02, 0.02, 0.01]
        self.process_noise = np.diag(np.array(process_noise_std)**2)

    def predict(self, dt):
        self.particles[:, 0] += self.particles[:, 3] * dt
        self.particles[:, 1] += self.particles[:, 4] * dt
        self.particles[:, 2] += self.particles[:, 5] * dt
        self.particles += np.random.multivariate_normal(np.zeros(6), self.process_noise, self.N)

    def update(self, measurement, hdop):
        measurement_noise = 0.5 + (hdop - 1.0) * 0.8
        distances = np.linalg.norm(self.particles[:, :3] - measurement, axis=1)
        if not np.all(np.isfinite(distances)):
            logger.error("Non-finite values detected in particle distances. Re-initializing weights.")
            self.weights.fill(1.0 / self.N)
            return True

        likelihood = norm.pdf(distances, 0, measurement_noise)
        self.weights = likelihood + 1e-300
        
        sum_weights = np.sum(self.weights)
        if sum_weights > 1e-9:
            self.weights /= sum_weights
        else:
            self.weights.fill(1.0 / self.N)
            logger.warning("Particle weights collapsed, re-initializing weights.")
            return False
        return True

    def resample_if_needed(self):
        if 1. / np.sum(np.square(self.weights)) < self.N / 2:
            indices = np.random.choice(np.arange(self.N), size=self.N, replace=True, p=self.weights)
            self.particles = self.particles[indices]
            self.weights.fill(1.0 / self.N)

    def estimate(self):
        sum_weights = np.sum(self.weights)
        if not np.isfinite(sum_weights) or sum_weights <= 1e-9:
            logger.error(f"Invalid particle weights detected (sum={sum_weights}). Activating fallback.")
            return np.mean(self.particles, axis=0), np.identity(6)

        try:
            mean = np.average(self.particles, weights=self.weights, axis=0)
            covariance = np.cov(self.particles.T, aweights=self.weights)
            if np.any(np.isnan(covariance)) or np.any(np.isinf(covariance)):
                logger.error("Covariance matrix contains NaN/Inf values. Activating fallback.")
                return mean, np.identity(6)
            return mean, covariance
        
        except Exception as e:
            logger.error(f"Unhandled error during estimation ({type(e).__name__}). Final fallback initiated.")
            return np.mean(self.particles, axis=0), np.identity(6)
        
    def reset(self):
        self.particles = np.zeros((self.N, 6))
        self.weights.fill(1.0 / self.N)

    ### CẢI TIẾN 2.1: PHƯƠNG THỨC "TÁI HÒA NHẬP MỀM MẠI" ###
    def re_initialize_after_gap(self, new_position):
        """
        Khởi tạo lại bộ lọc một cách êm ái sau một khoảng trống dữ liệu lớn.
        Tập trung các hạt vào vị trí mới và reset vận tốc về 0.
        """
        logger.info(f"Graceful Re-entry: Re-initializing filter around new position {new_position}.")
        # Đặt tất cả các hạt vào vị trí mới
        self.particles[:, :3] = new_position
        # RESET VẬN TỐC VỀ 0 ĐỂ TRÁNH SỐC
        self.particles[:, 3:] = 0
        
        # Thêm một chút nhiễu nhỏ để các hạt không bị trùng lặp hoàn toàn
        # Sử dụng 1/10 nhiễu quá trình tiêu chuẩn
        noise = np.random.multivariate_normal(np.zeros(6), self.process_noise / 10, self.N)
        self.particles += noise
        
        # Reset trọng số
        self.weights.fill(1.0 / self.N)

class DataForensicsLab:
    def __init__(self, constants):
        self.constants = constants
        self.nmea_regex = re.compile(
            r'^\$[A-Z]{2}GGA,(\d{6}(?:\.\d*)?|\d{6}),(\d{4}\.\d+),([NS]),'
            r'(\d{5}\.\d+),([EW]),([0-8]),(\d{1,2}),(\d*\.?\d+),(-?\d*\.?\d+),M,(-?\d*\.?\d+),M,([^,]*),([^,]*)\*([0-9A-Fa-f]{2})$'
        )
        self.last_valid_nmea_dt = self._load_last_timestamp()
        self.candidate_buffer = deque(maxlen=5)
        self.time_drift_buffer = deque(maxlen=10)
        self.estimated_drift = 0.0
        self.spatial_consistency_buffer = deque(maxlen=self.constants.SPATIAL_CONSISTENCY_BUFFER_SIZE)

        ### CẢI TIẾN 1.1: BIẾN TRẠNG THÁI MỚI CHO NGƯỠNG LỌC LEO THANG ###
        self.reject_streak_counter = 0
        self.dynamic_threshold_factor = 1.0 # Bắt đầu với hệ số 1.0 (không thay đổi)

    def _load_last_timestamp(self, path="last_timestamp.state"):
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    ts_str = f.read().strip()
                    return datetime.fromisoformat(ts_str)
        except Exception as e:
            logger.warning(f"Could not load last timestamp from state file: {e}")
        return None
    
    def _save_last_timestamp(self, path="last_timestamp.state"):
        if self.last_valid_nmea_dt:
            try:
                with open(path, "w") as f:
                    f.write(self.last_valid_nmea_dt.isoformat())
            except Exception as e:
                logger.error(f"Could not save last timestamp to state file: {e}")

    def inspect(self, payload_str):
        if not verify_checksum(payload_str):
            logger.warning(f"L1 REJECT - Checksum Invalid: '{payload_str.strip()}'")
            return None
        
        match = self.nmea_regex.match(payload_str.strip())
        if not match:
            logger.debug(f"L1 IGNORE - Not a GGA sentence: '{payload_str.strip()}'")
            return None
        
        try:
            groups = match.groups()
            h, m, s_full = int(groups[0][:2]), int(groups[0][2:4]), float(groups[0][4:])
            s, us = int(s_full), int((s_full - int(s_full)) * 1e6)
            
            server_now_utc = datetime.now(timezone.utc) - timedelta(seconds=self.estimated_drift)
            candidate_dt = server_now_utc.replace(hour=h, minute=m, second=s, microsecond=us, tzinfo=timezone.utc)

            if self.last_valid_nmea_dt:
                options = [candidate_dt - timedelta(days=1), candidate_dt, candidate_dt + timedelta(days=1)]
                valid_options = [opt for opt in options if opt > self.last_valid_nmea_dt]
                if valid_options:
                    # Chọn timestamp gần nhất về phía trước
                    nmea_utc_dt = min(valid_options, key=lambda d: abs((d - self.last_valid_nmea_dt).total_seconds()))
                else:
                    logger.critical(f"Could not find a valid monotonic timestamp. Last: {self.last_valid_nmea_dt}, Candidate Time: {candidate_dt.time()}. Point rejected.")
                    return None
            else:
                diff_seconds = (candidate_dt - server_now_utc).total_seconds()
                if diff_seconds > self.constants.TIME_WRAP_THRESHOLD_HOURS * 3600: candidate_dt -= timedelta(days=1)
                elif diff_seconds < -12 * 3600: candidate_dt += timedelta(days=1)
                nmea_utc_dt = candidate_dt
            
            nmea_utc_ts = nmea_utc_dt.timestamp()

            current_point = {
                "wgs": { "lat": convert_nmea_to_decimal(groups[1], groups[2]), "lon": convert_nmea_to_decimal(groups[3], groups[4]), "h": float(groups[8]) + float(groups[9]) },
                "nmea_ts": nmea_utc_ts, "sys_ts": time.time(), "quality": int(groups[5]), "satellites": int(groups[6]), "hdop": float(groups[7])
            }

            if any(v is None for v in current_point['wgs'].values()): raise ValueError("Invalid coordinate (parsing failed)")
            
            if current_point['quality'] not in [1, 2, 4, 5] or current_point['satellites'] < self.constants.MIN_SATELLITES or current_point['hdop'] > self.constants.MAX_HDOP:
                logger.warning(f"L1 REJECT - Poor GNSS Quality (Q={current_point['quality']}, Sats={current_point['satellites']}, HDOP={current_point['hdop']:.1f})")
                return None
                
        except (ValueError, IndexError) as e:
            logger.warning(f"L1 REJECT - Failed to parse NMEA sentence: '{payload_str.strip()}'. Reason: {e}")
            return None
        
        current_drift = current_point['sys_ts'] - nmea_utc_ts
        if abs(current_drift) < 7200: # Chỉ cập nhật drift nếu hợp lý
            self.time_drift_buffer.append(current_drift)
            if len(self.time_drift_buffer) > 3:
                self.estimated_drift = np.median(list(self.time_drift_buffer))
        
        if len(self.spatial_consistency_buffer) >= self.constants.MIN_SPATIAL_CONSISTENCY_POINTS:
            ref_wgs = self.spatial_consistency_buffer[0]['wgs']
            buffer_enu_points = np.array([wgs84_to_enu(p['wgs']['lat'], p['wgs']['lon'], p['wgs']['h'], ref_wgs['lat'], ref_wgs['lon'], ref_wgs['h']) for p in self.spatial_consistency_buffer])
            median_pos = np.median(buffer_enu_points, axis=0)
            new_enu_point = np.array(wgs84_to_enu(current_point['wgs']['lat'], current_point['wgs']['lon'], current_point['wgs']['h'], ref_wgs['lat'], ref_wgs['lon'], ref_wgs['h']))
            distance_from_median_to_new = np.linalg.norm(new_enu_point - median_pos)

            distances_from_median_in_buffer = np.linalg.norm(buffer_enu_points - median_pos, axis=1)
            # Sử dụng MAD (Median Absolute Deviation) để ước tính độ lệch chuẩn một cách mạnh mẽ
            mad = np.median(np.abs(distances_from_median_in_buffer - np.median(distances_from_median_in_buffer)))
            robust_std_estimate = 1.4826 * mad
            
            dynamic_threshold = 0.5 + self.constants.SPATIAL_CONSISTENCY_STD_DEV_FACTOR * robust_std_estimate
            
            ### CẢI TIẾN 1.1: LOGIC NGƯỠNG LỌC LEO THANG ###
            # Áp dụng hệ số nới lỏng động
            final_threshold_base = min(dynamic_threshold, self.constants.SPATIAL_ABSOLUTE_MAX_THRESHOLD)
            final_threshold = final_threshold_base * self.dynamic_threshold_factor

            if distance_from_median_to_new > final_threshold:
                self.reject_streak_counter += 1
                # Nếu chuỗi từ chối đủ dài, bắt đầu leo thang
                if self.reject_streak_counter > self.constants.REJECT_STREAK_FOR_ESCALATION:
                    old_factor = self.dynamic_threshold_factor
                    self.dynamic_threshold_factor = min(
                        self.constants.MAX_ESCALATION_FACTOR,
                        self.dynamic_threshold_factor + self.constants.ESCALATION_FACTOR_INCREMENT
                    )
                    if old_factor != self.dynamic_threshold_factor:
                         logger.warning(f"ADAPTIVE THRESHOLD: Signal unstable. Escalating filter tolerance. Factor: {old_factor:.2f} -> {self.dynamic_threshold_factor:.2f}")

                logger.warning(f"L1 REJECT - SPATIAL OUTLIER. Point is {distance_from_median_to_new:.2f}m from cluster median, exceeding final threshold of {final_threshold:.2f}m (Base: {final_threshold_base:.2f}m, Factor: {self.dynamic_threshold_factor:.2f}x).")
                return None
            else:
                # Nếu chấp nhận một điểm, bắt đầu quá trình giảm leo thang
                if self.dynamic_threshold_factor > 1.0:
                    old_factor = self.dynamic_threshold_factor
                    self.dynamic_threshold_factor = max(
                        1.0, 
                        self.dynamic_threshold_factor - self.constants.DE_ESCALATION_FACTOR_DECREMENT
                    )
                    if abs(old_factor - self.dynamic_threshold_factor) > 1e-3:
                         logger.info(f"ADAPTIVE THRESHOLD: Signal stabilizing. De-escalating filter tolerance. Factor: {old_factor:.2f} -> {self.dynamic_threshold_factor:.2f}")
                # Reset chuỗi từ chối
                self.reject_streak_counter = 0

        # Cập nhật trạng thái và các buffer SAU KHI điểm dữ liệu đã vượt qua tất cả các kiểm tra
        self.last_valid_nmea_dt = nmea_utc_dt
        self._save_last_timestamp()
        self.spatial_consistency_buffer.append(current_point)
        self.candidate_buffer.append(current_point)
        
        return current_point

class MasterControlEngine:
    def __init__(self, config, classification_table, config_parser):
        self.config = config
        self.constants = Constants(config_parser)
        self.classification_table = classification_table
        self.l1_forensics_lab = DataForensicsLab(self.constants)
        
        pf_config = config_parser['ParticleFilter']
        noise_str = pf_config.get('process_noise_std_devs', '0.005, 0.005, 0.006, 0.02, 0.02, 0.01')
        process_noise_std = [float(x.strip()) for x in noise_str.split(',')]
        self.l2_particle_filter = ParticleFilterGNSS(
            num_particles=self.config.num_particles,
            process_noise_std=process_noise_std
        )
        self.origin = None
        
        self.long_term_history = deque(maxlen=self.constants.LONG_TERM_HISTORY_MAXLEN)
        
        self.processed_points_count = 0
        self.messages_since_last_report = 0
        self.reporting_interval_messages = 5
        self.short_term_stability_buffer = deque(maxlen=200)
        self._frequency_analysis_buffer = deque(maxlen=self.constants.ANALYSIS_WINDOW_FOR_FREQUENCY)
        self.messages_since_last_reassessment = 0
        self.REASSESSMENT_INTERVAL = 500
        
        # Biến trạng thái cho các cơ chế nâng cao
        self.filter_in_cooldown = False
        self._cooldown_recovery_buffer = deque(maxlen=self.constants.COOLDOWN_RECOVERY_BUFFER_SIZE)
        self.cooldown_attempts = 0
        self._origin_candidate_buffer = deque(maxlen=10)
        self._origin_finding_attempts = 0
        self._transient_learning_buffer = deque(maxlen=int(self.constants.INITIAL_STABILIZATION_PERIOD - self.constants.ANALYSIS_WINDOW_FOR_FREQUENCY))
        self.dynamic_transient_threshold = 10
        self.points_since_last_pf_reset = 0

        ### CẢI TIẾN 1.2: MÁY TRẠNG THÁI SỨC KHỎE TÍN HIỆU ###
        self.signal_health_state = "INITIALIZING" # Các trạng thái: INITIALIZING, STABLE, DEGRADED, CRITICAL_NO_SIGNAL
        self.consecutive_rejects_count = 0
        self.time_entered_degraded_state = None
        
        self.stabilization_noise_buffer = deque(maxlen=self.constants.INITIAL_STABILIZATION_PERIOD)
        self.origin = self._load_origin_state()
        if self.origin:
            logger.info(f"Successfully loaded a persistent origin point from state file.")
            self.signal_health_state = "STABLE"
    
        # Thêm vào đầu class MasterControlEngine
    def safe_division(self, numerator, denominator, fallback=0.0, min_denominator=1e-12):
        """
        Thực hiện phép chia an toàn với protection chống division by zero
        """
        try:
            if not np.isfinite(denominator) or abs(denominator) < min_denominator:
                logger.debug(f"Safe division: denominator too small ({denominator}), using fallback")
                return fallback
            
            result = numerator / denominator
            
            if not np.isfinite(result):
                logger.debug(f"Safe division: result not finite ({result}), using fallback")
                return fallback
                
            return result
        except (ZeroDivisionError, OverflowError):
            logger.debug(f"Safe division: mathematical error, using fallback")
            return fallback

    def safe_exp(self, x, fallback=0.0):
        """
        Thực hiện np.exp an toàn với protection chống overflow
        """
        try:
            if not np.isfinite(x):
                return fallback
            
            # Clamp x để tránh overflow
            x_clamped = np.clip(x, -700, 700)  # exp(700) ≈ 10^304, gần giới hạn float64
            result = np.exp(x_clamped)
            
            if not np.isfinite(result):
                return fallback
                
            return result
        except (OverflowError):
            return fallback
    
    def _load_origin_state(self, path=os.path.join(LOG_DIRECTORY, "origin_state.json")):
        try:
            if os.path.exists(path):
                with open(path, "r", encoding='utf-8') as f:
                    origin_data = json.load(f)
                    if 'lat' in origin_data and 'lon' in origin_data and 'h' in origin_data:
                        return origin_data
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Could not load origin state file: {e}")
        return None

    def _save_origin_state(self, path=os.path.join(LOG_DIRECTORY, "origin_state.json")):
        if self.origin:
            try:
                with open(path, "w", encoding='utf-8') as f:
                    json.dump(self.origin, f, indent=4)
            except IOError as e:
                logger.error(f"Could not save origin state file: {e}")

    ### CẢI TIẾN 1.2: HÀM QUẢN LÝ TRẠNG THÁI TÍN HIỆU ###
    def _update_signal_health_state(self, point_was_verified):
        current_time = time.time()
        
        if point_was_verified:
            self.consecutive_rejects_count = 0
            if self.signal_health_state != "STABLE" and self.signal_health_state != "INITIALIZING":
                logger.info("Signal health has returned to STABLE.")
                self.signal_health_state = "STABLE"
                self.time_entered_degraded_state = None
                return {
                    "type": "system_health_report",
                    "timestamp": current_time, "datetime_utc7": datetime.fromtimestamp(current_time, tz=TZ_UTC_7).isoformat(),
                    "status_code": "SIGNAL_RECOVERED",
                    "message": "Tín hiệu GNSS đã phục hồi và hoạt động ổn định."
                }
        else: # Điểm dữ liệu bị từ chối
            self.consecutive_rejects_count += 1
            if self.signal_health_state == "STABLE" and self.consecutive_rejects_count > self.constants.DEGRADED_STATE_THRESHOLD_REJECTS:
                self.signal_health_state = "DEGRADED"
                self.time_entered_degraded_state = current_time
                logger.warning("Signal health has entered DEGRADED state due to continuous data rejection.")
                return {
                    "type": "system_health_report",
                    "timestamp": current_time, "datetime_utc7": datetime.fromtimestamp(current_time, tz=TZ_UTC_7).isoformat(),
                    "status_code": "SIGNAL_DEGRADED",
                    "message": "Cảnh báo: Tín hiệu GNSS suy giảm, dữ liệu đang bị từ chối liên tục."
                }
            elif self.signal_health_state == "DEGRADED":
                time_in_degraded = current_time - self.time_entered_degraded_state
                if time_in_degraded > self.constants.CRITICAL_STATE_THRESHOLD_SECONDS:
                    self.signal_health_state = "CRITICAL_NO_SIGNAL"
                    logger.critical("Signal health has entered CRITICAL_NO_SIGNAL state after prolonged signal degradation.")
                    return {
                        "type": "system_health_report",
                        "timestamp": current_time, "datetime_utc7": datetime.fromtimestamp(current_time, tz=TZ_UTC_7).isoformat(),
                        "status_code": "CRITICAL_NO_SIGNAL",
                        "message": "Nguy cấp: Mất tín hiệu GNSS đáng tin cậy trong thời gian dài. Hệ thống không thể đảm bảo giám sát."
                    }
        return None

    def process_payload(self, payload_str):
        try:
            verified_point = self.l1_forensics_lab.inspect(payload_str)
            
            # Cập nhật trạng thái sức khỏe tín hiệu và nhận báo cáo nếu có
            health_report = self._update_signal_health_state(point_was_verified=(verified_point is not None))
            if health_report:
                return health_report

            if not verified_point:
                return None

            # ======================================================================
            # GIAI ĐOẠN 1: TÌM KIẾM ĐIỂM GỐC (ORIGIN)
            # ======================================================================
            if self.origin is None:
                self._origin_candidate_buffer.append(verified_point['wgs'])
                self._origin_finding_attempts += 1
                
                if len(self._origin_candidate_buffer) < self._origin_candidate_buffer.maxlen:
                    return {"type": "stabilization_progress", "timestamp": verified_point['nmea_ts'], "datetime_utc7": datetime.fromtimestamp(verified_point['nmea_ts'], tz=TZ_UTC_7).isoformat(), "count": len(self._origin_candidate_buffer), "total": self._origin_candidate_buffer.maxlen, "message": "Đang thu thập dữ liệu để xác định điểm gốc..."}
                
                first_pt = self._origin_candidate_buffer[0]
                enu_points = [wgs84_to_enu(p['lat'], p['lon'], p['h'], first_pt['lat'], first_pt['lon'], first_pt['h']) for p in self._origin_candidate_buffer]
                std_dev = np.linalg.norm(np.std(np.array(enu_points), axis=0))

                if std_dev < 1.0:
                    self.origin = self._origin_candidate_buffer[len(self._origin_candidate_buffer) // 2]
                    self._save_origin_state()
                    self.l2_particle_filter.reset()
                    self._origin_candidate_buffer.clear()
                    self.signal_health_state = "STABLE" # Chuyển sang trạng thái ổn định
                    logger.info(f"STABLE origin point established with StdDev: {std_dev:.3f}m. System is LIVE.")
                else:
                    logger.warning(f"Origin candidates UNSTABLE (StdDev: {std_dev:.3f}m). Awaiting new data.")
                    self._origin_candidate_buffer.popleft()
                
                if self._origin_finding_attempts >= self.constants.ORIGIN_FINDING_ATTEMPT_LIMIT and self.origin is None:
                    logger.critical(f"ORIGIN FINDING TIMEOUT after {self.constants.ORIGIN_FINDING_ATTEMPT_LIMIT} attempts. System will not process displacement data until a stable signal is acquired.")
                    self._origin_finding_attempts = 0 
                    self._origin_candidate_buffer.clear()
                    return {"type": "error_state", "timestamp": verified_point['nmea_ts'], "datetime_utc7": datetime.fromtimestamp(verified_point['nmea_ts'], tz=TZ_UTC_7).isoformat(), "error_code": "E101_ORIGIN_UNSTABLE", "message": "Không thể tìm thấy điểm gốc ổn định. Đang chờ tín hiệu GPS tốt hơn."}
                
                return {"type": "stabilization_progress", "timestamp": verified_point['nmea_ts'], "datetime_utc7": datetime.fromtimestamp(verified_point['nmea_ts'], tz=TZ_UTC_7).isoformat(), "count": len(self._origin_candidate_buffer), "total": self._origin_candidate_buffer.maxlen, "message": f"Đang ổn định điểm gốc... (Độ lệch: {std_dev:.3f}m)"}
            
            # ======================================================================
            # GIAI ĐOẠN 2: XỬ LÝ DỮ LIỆU KHI ĐÃ CÓ ĐIỂM GỐC
            # ======================================================================
            
            e, n, u = wgs84_to_enu(verified_point['wgs']['lat'], verified_point['wgs']['lon'], verified_point['wgs']['h'], self.origin['lat'], self.origin['lon'], self.origin['h'])
            raw_enu_point = np.array([e, n, u])
            
            if not np.all(np.isfinite(raw_enu_point)):
                logger.critical(f"L2 REJECT - Invalid ENU coordinates generated (NaN or Inf). Point dropped. WGS: {verified_point['wgs']}")
                return None

            if self.filter_in_cooldown:
                self._cooldown_recovery_buffer.append(raw_enu_point)
                logger.info(f"COOLDOWN ACTIVE: Collecting data for re-stabilization ({len(self._cooldown_recovery_buffer)}/{self.constants.COOLDOWN_RECOVERY_BUFFER_SIZE}).")

                if len(self._cooldown_recovery_buffer) == self.constants.COOLDOWN_RECOVERY_BUFFER_SIZE:
                    self.cooldown_attempts += 1
                    points = np.array(list(self._cooldown_recovery_buffer))
                    std_dev_3d = np.linalg.norm(np.std(points, axis=0))
                    
                    current_threshold = self.constants.COOLDOWN_RECOVERY_STD_DEV_THRESHOLD
                    if self.cooldown_attempts > 10:
                        current_threshold *= 1.5
                        logger.warning(f"Cooldown recovery is slow. Relaxing threshold to {current_threshold:.2f}m.")

                    if std_dev_3d < current_threshold:
                        self.filter_in_cooldown = False
                        self.cooldown_attempts = 0
                        self._cooldown_recovery_buffer.clear()
                        self.l2_particle_filter.reset() 
                        logger.critical(f"INTELLIGENT COOLDOWN RECOVERY: Signal stabilized (StdDev: {std_dev_3d:.3f}m). Filter re-activated.")
                    else:
                        logger.warning(f"Cooldown check failed: Signal still unstable (StdDev: {std_dev_3d:.3f}m). Waiting for more data.")
                        self._cooldown_recovery_buffer.popleft()
                
                return None
            
            last_point = self.long_term_history[-1] if self.long_term_history else None
            dt = verified_point['nmea_ts'] - last_point['ts'] if last_point else 1.0
            
            discontinuity_flag = False
            if dt <= 0.1: return None
            
            ### CẢI TIẾN 2.1: LOGIC "TÁI HÒA NHẬP MỀM MẠI" ###
            if dt > self.constants.GRACEFUL_REENTRY_TIME_GAP_S:
                logger.warning(f"Large time gap detected ({dt:.1f}s). Activating Graceful Re-entry.")
                self.l2_particle_filter.re_initialize_after_gap(raw_enu_point)
                discontinuity_flag = True # Đánh dấu điểm dữ liệu này là một điểm đứt gãy
            else:
                if dt > self.constants.MAX_PREDICTION_DT:
                    logger.warning(f"Capping prediction dt from {dt:.1f}s to {self.constants.MAX_PREDICTION_DT}s.")
                    dt = self.constants.MAX_PREDICTION_DT
                self.l2_particle_filter.predict(dt)
                
            update_successful = self.l2_particle_filter.update(raw_enu_point, verified_point['hdop'])
            if not update_successful:
                logger.critical("FILTER DIVERGENCE (Weight Collapse)! Resetting and starting INTELLIGENT COOLDOWN.")
                self.l2_particle_filter.reset()
                self.filter_in_cooldown = True
                return None
            
            estimated_state, covariance = self.l2_particle_filter.estimate()
            position_uncertainty_m = math.sqrt(np.trace(covariance[:3,:3]))

            if not math.isfinite(position_uncertainty_m) or position_uncertainty_m > self.constants.MAX_UNCERTAINTY_METERS:
                logger.critical(f"FILTER DIVERGENCE! Uncertainty: {position_uncertainty_m:.2f}m. Resetting and starting INTELLIGENT COOLDOWN.")
                self.l2_particle_filter.reset()
                self.filter_in_cooldown = True
                return None

            self.l2_particle_filter.resample_if_needed()
            
            ### CẢI TIẾN 2.2: LƯU TRỮ LỊCH SỬ VỚI CỜ GIÁN ĐOẠN ###
            history_entry = {
                "ts": verified_point['nmea_ts'],
                "state": estimated_state,
                "discontinuity": discontinuity_flag
            }
            self.long_term_history.append(history_entry)

            self.processed_points_count += 1
            self.short_term_stability_buffer.append(raw_enu_point)
            
            self.messages_since_last_reassessment += 1
            if self.messages_since_last_reassessment >= self.REASSESSMENT_INTERVAL:
                self._dynamically_adjust_reporting_interval()
                self.messages_since_last_reassessment = 0

            # ======================================================================
            # GIAI ĐOẠN 3: ỔN ĐỊNH BAN ĐẦU & HỌC NGƯỠNG TỰ ĐỘNG
            # ======================================================================
            if self.processed_points_count <= self.constants.INITIAL_STABILIZATION_PERIOD:
                self.stabilization_noise_buffer.append(raw_enu_point)
                if self.processed_points_count > self.constants.ANALYSIS_WINDOW_FOR_FREQUENCY:
                    wavelet_mri = self._analyze_wavelet_mri()
                    if wavelet_mri: self._transient_learning_buffer.append(wavelet_mri.get("transient_event_count", 0))

                if self.processed_points_count == self.constants.INITIAL_STABILIZATION_PERIOD:
                    if len(self.stabilization_noise_buffer) > 50:
                        points = np.array(list(self.stabilization_noise_buffer))
                        baseline_noise_3d_std = np.linalg.norm(np.std(points, axis=0))
                        new_factor = 3.0 + baseline_noise_3d_std * 10 
                        final_factor = np.clip(new_factor, 3.0, 8.0) 
                        logger.info("--- ADAPTIVE THRESHOLDING COMPLETE ---")
                        logger.info(f"Baseline 3D noise standard deviation: {baseline_noise_3d_std:.4f}m")
                        logger.info(f"Old SPATIAL_CONSISTENCY_STD_DEV_FACTOR: {self.constants.SPATIAL_CONSISTENCY_STD_DEV_FACTOR}")
                        self.constants.SPATIAL_CONSISTENCY_STD_DEV_FACTOR = final_factor
                        logger.info(f"New SPATIAL_CONSISTENCY_STD_DEV_FACTOR set to: {self.constants.SPATIAL_CONSISTENCY_STD_DEV_FACTOR:.2f}")
                        self.stabilization_noise_buffer.clear()

                    if self._transient_learning_buffer:
                        mean_transients = np.mean(self._transient_learning_buffer)
                        std_transients = np.std(self._transient_learning_buffer)
                        self.dynamic_transient_threshold = max(7, round(mean_transients + 2.5 * std_transients))
                        logger.info(f"Initial transient threshold learning complete. Dynamic threshold set to: {self.dynamic_transient_threshold}")
                        self._transient_learning_buffer.clear()
                
                return {"type": "stabilization_progress", "timestamp": verified_point['nmea_ts'], "datetime_utc7": datetime.fromtimestamp(verified_point['nmea_ts'], tz=TZ_UTC_7).isoformat(), "count": self.processed_points_count, "total": self.constants.INITIAL_STABILIZATION_PERIOD, "message": f"Hệ thống đang ổn định... {self.processed_points_count}/{self.constants.INITIAL_STABILIZATION_PERIOD}"}

            # ======================================================================
            # GIAI ĐOẠN 4: HOẠT ĐỘNG BÌNH THƯỜNG & BÁO CÁO
            # ======================================================================
            ### CẢI TIẾN 3.1: CHỈ TẠO BÁO CÁO PHÂN TÍCH KHI TÍN HIỆU ỔN ĐỊNH ###
            self.messages_since_last_report += 1
            if self.messages_since_last_report >= self.reporting_interval_messages:
                self.messages_since_last_report = 0
                if self.signal_health_state == "STABLE":
                    return self._activate_qrf_and_stratcom()
                else:
                    logger.info(f"Skipping strategic report due to non-STABLE signal health state: {self.signal_health_state}")
            
            # Logic reset định kỳ của Particle Filter
            self.points_since_last_pf_reset += 1
            if self.points_since_last_pf_reset >= self.constants.PF_RESET_INTERVAL:
                logger.info(f"Performing periodic particle filter reset to prevent floating point drift.")
                current_estimate, _ = self.l2_particle_filter.estimate()
                self.l2_particle_filter.reset()
                try:
                    self.l2_particle_filter.particles = np.random.multivariate_normal(current_estimate, self.l2_particle_filter.process_noise, self.l2_particle_filter.N)
                except np.linalg.LinAlgError:
                    logger.critical("Failed periodic reset due to singular covariance. Applying manual noise fallback.")
                    self.l2_particle_filter.particles = np.tile(current_estimate, (self.l2_particle_filter.N, 1))
                    noise = np.random.randn(self.l2_particle_filter.N, 6) * np.sqrt(np.diag(self.l2_particle_filter.process_noise))
                    self.l2_particle_filter.particles += noise
                self.points_since_last_pf_reset = 0

            return None
        except Exception as e:
            logger.error(f"FATAL error during payload processing in MasterControlEngine: {e}", exc_info=True)
            return None

    def _dynamically_adjust_reporting_interval(self, initial=False):
        buffer_to_use = self.short_term_stability_buffer
        if len(buffer_to_use) < 50:
            return

        points = np.array(list(buffer_to_use))
        
        median = np.median(points, axis=0)
        distances = np.linalg.norm(points - median, axis=1)
        mad = np.median(np.abs(distances - np.median(distances)))
        robust_std_estimate = 1.4826 * mad
        old_interval = self.reporting_interval_messages

        if robust_std_estimate < 0.2:
            self.reporting_interval_messages = 15
        elif robust_std_estimate < 0.8:
            self.reporting_interval_messages = 10
        else:
            self.reporting_interval_messages = 5
        
        if initial:
            msg = f"Initial stability analysis complete: Area stability assessed (Robust StdDev: {robust_std_estimate:.3f}m). Reporting interval set to every {self.reporting_interval_messages} messages."
            logger.info(msg)
            ts = time.time()
            ts_iso = datetime.fromtimestamp(ts, tz=TZ_UTC_7).isoformat()
            print(json.dumps({"type": "frequency_analysis_complete", "timestamp": ts, "datetime_utc7": ts_iso, "message": msg, "status": "INFO"})); sys.stdout.flush()
        elif old_interval != self.reporting_interval_messages:
            logger.info(f"Dynamically adjusted reporting interval due to changing conditions (Robust StdDev: {robust_std_estimate:.3f}m). New interval: {self.reporting_interval_messages} messages.")

    def _activate_qrf_and_stratcom(self):
        if len(self.long_term_history) < 200:
            return None
        
        wavelet_mri = self._analyze_wavelet_mri()
        if not wavelet_mri:
            logger.info("Skipping strategic report: Wavelet analysis did not produce valid results (likely not enough data).")
            return None

        model_probs = self._run_mmae_analysis()
        belief, assessment = self._run_bayesian_fusion(wavelet_mri, model_probs)
        vel_class = self._get_classification_from_velocity(wavelet_mri.get("trend_velocity_mmps", 0))
        ts_unix = self.long_term_history[-1]['ts']
        ts_iso = datetime.fromtimestamp(ts_unix, tz=TZ_UTC_7).isoformat()
        
        ### CẢI TIẾN 3.1: PHÂN LOẠI CẢNH BÁO ###
        report_status = "stable"
        if belief["RapidFailure"] > getattr(self.config, 'belief_threshold_rapid_failure', 0.5):
            report_status = "gnss_danger"
        elif belief["SlowCreep"] > getattr(self.config, 'belief_threshold_slow_creep', 0.6):
            report_status = "gnss_warning"

        return {
            "type": "strategic_intelligence_report",
            "timestamp": ts_unix,
            "datetime_utc7": ts_iso,
            "status_code": report_status,
            "classification_name": vel_class,
            "classification_velocity_mm_s": wavelet_mri.get("trend_velocity_mmps", 0),
            "belief_state_vector": belief,
            "most_likely_state": max(belief, key=belief.get) if belief else "N/A",
            "qrf_assessment": {"wavelet_mri": wavelet_mri},
            "stratcom_analysis": {
                "most_likely_motion_model": max(model_probs, key=model_probs.get) if model_probs else "N/A",
                "model_probabilities": model_probs,
                "assessment": assessment
            }
        }
    
    def _analyze_wavelet_mri(self):
        if len(self.long_term_history) < 50:
            return {}
        
        try:
            available_gb = psutil.virtual_memory().available / (1024**3)
            if available_gb < 1.0:  # Ít hơn 1GB
                logger.warning(f"Low memory ({available_gb:.1f}GB). Skipping wavelet analysis.")
                return {}
            load_1_min, _, _ = os.getloadavg()
            num_cores = os.cpu_count() or 1
            if (load_1_min / num_cores) > 0.90:
                logger.warning(f"CPU load is high ({load_1_min / num_cores:.1%}). Skipping expensive wavelet analysis.")
                return {}
        except (OSError, AttributeError):
            pass

        try:
            last_discontinuity_idx = -1
            for i in range(len(self.long_term_history) - 1, -1, -1):
                if self.long_term_history[i]['discontinuity']:
                    last_discontinuity_idx = i
                    break
            
            # Lấy chuỗi dữ liệu liền mạch để phân tích
            analysis_data = list(self.long_term_history)[last_discontinuity_idx + 1:]

            if len(analysis_data) < 50:
                 logger.info(f"Not enough continuous data points for wavelet analysis after last discontinuity ({len(analysis_data)} points).")
                 return {}

            logger.debug(f"Analyzing {len(analysis_data)} continuous data points for wavelet MRI.")
            
            timestamps = np.array([p['ts'] for p in analysis_data])
            states = np.array([p['state'] for p in analysis_data])
            
            total_duration = timestamps[-1] - timestamps[0]
            if total_duration <= 1e-6: return {}
            avg_dt = np.mean(np.diff(timestamps))
            if avg_dt <= 1e-6: return {}

            proposed_num_points = int(total_duration / avg_dt)
            MAX_SAFE_POINTS = 50000  # Giới hạn an toàn
    
            if proposed_num_points > MAX_SAFE_POINTS:
                logger.warning(f"Capping wavelet points from {proposed_num_points} to {MAX_SAFE_POINTS}")
                avg_dt = total_duration / MAX_SAFE_POINTS
                proposed_num_points = MAX_SAFE_POINTS
            required_memory_bytes = 3 * proposed_num_points * 8
            available_memory_bytes = psutil.virtual_memory().available

            if required_memory_bytes > available_memory_bytes * 0.5:
                logger.critical(f"MEMORY SAFETY HALT: Wavelet analysis requires {required_memory_bytes/1e6:.2f}MB, exceeding 50% of available RAM. Analysis skipped.")
                return {}
            
            if proposed_num_points > self.constants.MAX_RESAMPLED_POINTS_FOR_WAVELET:
                logger.warning(f"Wavelet resampling would create a large array ({proposed_num_points} points). Capping at {self.constants.MAX_RESAMPLED_POINTS_FOR_WAVELET}.")
                avg_dt = total_duration / self.constants.MAX_RESAMPLED_POINTS_FOR_WAVELET
            
            regular_ts = np.arange(timestamps[0], timestamps[-1], avg_dt)
            data_len = len(regular_ts)
            
            x = np.interp(regular_ts, timestamps, states[:, 0])
            y = np.interp(regular_ts, timestamps, states[:, 1])
            z = np.interp(regular_ts, timestamps, states[:, 2])

            wavelet = 'db8'
            required_len = pywt.Wavelet(wavelet).dec_len * 2
            if data_len < required_len: return {}
            
            max_level = pywt.dwt_max_level(data_len, wavelet)
            level = min(6, max_level)
            if level < 4: return {}

            def reconstruct(coeffs, indices, length):
                recon = [np.zeros_like(c) for c in coeffs]
                for i in indices:
                    if i < len(recon): recon[i] = coeffs[i]
                return pywt.waverec(recon, wavelet, mode='symmetric')[:length]

            coeffs_x = pywt.wavedec(x, wavelet, level=level)
            coeffs_y = pywt.wavedec(y, wavelet, level=level)
            coeffs_z = pywt.wavedec(z, wavelet, level=level)
            
            trend_x = reconstruct(coeffs_x, [0], data_len)
            trend_y = reconstruct(coeffs_y, [0], data_len)
            trend_z = reconstruct(coeffs_z, [0], data_len)
            transients = reconstruct(coeffs_x, [level - 1, level], data_len)

            dt_total = regular_ts[-1] - regular_ts[0]
            if dt_total <= 0: dt_total = 1e-6

            vx = (trend_x[-1] - trend_x[0]) / dt_total
            vy = (trend_y[-1] - trend_y[0]) / dt_total
            vz = (trend_z[-1] - trend_z[0]) / dt_total
            trend_velocity_mps = math.sqrt(vx**2 + vy**2 + vz**2)
            
            direction = math.degrees(math.atan2(vx, vy))
            direction = (direction + 360) % 360
            
            num_transients = 0
            if transients.size > 1:
                median_transient = np.median(transients)
                mad = np.median(np.abs(transients - median_transient))
                robust_std_estimate = 1.4826 * mad
                robust_threshold = 3.5 * robust_std_estimate
                final_threshold = max(self.constants.MIN_TRANSIENT_AMPLITUDE_M, robust_threshold)
                num_transients = np.sum(np.abs(transients) > final_threshold)
                logger.debug(f"Transient detection: MAD={mad:.5f}m, RobustThresh={robust_threshold:.5f}m, FinalThresh={final_threshold:.5f}m, Count={num_transients}")

            return {
                "trend_velocity_mmps": trend_velocity_mps * 1000,
                "transient_event_count": int(num_transients),
                "direction_degrees": direction
            }
        except Exception as e:
            logger.error(f"L3 Wavelet Analysis Failed: {e}", exc_info=True)
            return {}

    def _run_mmae_analysis(self):
        if len(self.long_term_history) < 50:
            return {"Static": 1.0, "LinearDrift": 0.0, "Oscillation": 0.0, "Jerk": 0.0}

        states = np.array([p['state'] for p in self.long_term_history])
        timestamps = np.array([p['ts'] for p in self.long_term_history])
        
        vel_vectors = states[:, 3:]
        vel_magnitudes = np.linalg.norm(vel_vectors, axis=1)
        
        dt = np.diff(timestamps)
        dt[dt == 0] = 1e-6 
        accel_vectors = np.diff(vel_vectors, axis=0) / dt[:, np.newaxis]
        accel_magnitudes = np.linalg.norm(accel_vectors, axis=1)

        mean_vel = np.mean(vel_magnitudes)
        if accel_magnitudes.size > 0:
            mean_accel = np.mean(accel_magnitudes)
            std_accel = np.std(accel_magnitudes)
        else:
            mean_accel = 0.0
            std_accel = 0.0

        # 🔧 SỬA: Defensive thresholds với validation
        def safe_threshold(value, min_threshold=1e-12):
            """Đảm bảo threshold luôn > 0 và hợp lý"""
            if not np.isfinite(value) or value <= 0:
                return min_threshold
            return max(value, min_threshold)
        
        # 🔧 SỬA: Sử dụng safe_threshold function
        safe_static_threshold = safe_threshold(self.constants.STATIC_VELOCITY_THRESHOLD_M_PER_S)
        safe_accel_threshold = safe_threshold(self.constants.CONSTANT_ACCELERATION_THRESHOLD_M_PER_S2)
        safe_jerk_threshold = safe_threshold(self.constants.JERK_ACCELERATION_THRESHOLD_M_PER_S2)
        
        # 🔧 SỬA: Thêm validation cho input values
        if not np.isfinite(mean_vel) or mean_vel < 0:
            mean_vel = 0.0
        if not np.isfinite(mean_accel) or mean_accel < 0:
            mean_accel = 0.0
        if not np.isfinite(std_accel) or std_accel < 0:
            std_accel = 0.0
        
        # 🔧 SỬA: Safe calculations với try-catch
        try:
            score_static = np.exp(-mean_vel / safe_static_threshold)
            score_drift = (1 - score_static) * np.exp(-mean_accel / safe_accel_threshold)
            score_jerk = 1 - np.exp(-std_accel / safe_jerk_threshold)
        except (ZeroDivisionError, OverflowError) as e:
            logger.error(f"Mathematical error in MMAE analysis: {e}. Using fallback scores.")
            # Fallback values
            score_static = 1.0
            score_drift = 0.0
            score_jerk = 0.0

        # Oscillation calculation cũng cần protection
        mean_vel_scalar = np.mean(vel_magnitudes)
        if not np.isfinite(mean_vel_scalar):
            mean_vel_scalar = 0.0
            
        crossings = np.sum(np.diff(np.sign(vel_magnitudes - mean_vel_scalar)) != 0)
        denominator = max(1, len(vel_magnitudes) - 1)  # Đã có protection
        oscillation_metric = crossings / denominator
        
        # 🔧 SỬA: Safe oscillation score calculation
        try:
            score_oscillation = 1 - np.exp(-oscillation_metric / 0.2)
            if not np.isfinite(score_oscillation) or score_oscillation < 0:
                score_oscillation = 0.0
        except (OverflowError):
            score_oscillation = 0.0

        scores = {
            "Static": max(0.0, min(1.0, score_static)), 
            "LinearDrift": max(0.0, min(1.0, score_drift)), 
            "Oscillation": max(0.0, min(1.0, score_oscillation)), 
            "Jerk": max(0.0, min(1.0, score_jerk))
        }
        
        # 🔧 SỬA: Safe normalization
        total_score = sum(scores.values())
        if total_score > 1e-9:
            normalized_probs = {key: value / total_score for key, value in scores.items()}
        else:
            # Fallback khi tất cả scores = 0
            normalized_probs = {"Static": 1.0, "LinearDrift": 0.0, "Oscillation": 0.0, "Jerk": 0.0}
            logger.warning("All MMAE scores were zero. Using Static fallback.")
                
        logger.debug(f"MMA-Heuristic Probs: {normalized_probs}")
        return normalized_probs

    def _run_bayesian_fusion(self, wavelet, mmae):
        belief = {"Stable": 1.0, "SlowCreep": 0.0, "RapidFailure": 0.0}
        vel_mmps = wavelet.get("trend_velocity_mmps", 0)
        transients = wavelet.get("transient_event_count", 0)
        p_drift = mmae.get("LinearDrift", 0)

        if vel_mmps > 0.1 or p_drift > 0.6:
            belief["SlowCreep"] = max(vel_mmps / (vel_mmps + 50.0), p_drift)

        if transients > self.dynamic_transient_threshold:
            excess = transients - self.dynamic_transient_threshold
            belief["RapidFailure"] = 1 - (1 / (1 + excess / 5.0))

        belief["Stable"] = max(0, 1.0 - belief["SlowCreep"] - belief["RapidFailure"])
        
        total = sum(belief.values())
        if total > 0:
            belief = {k: v/total for k, v in belief.items()}
        
        assessment = "Hệ thống hoạt động ổn định."
        
        if belief["Stable"] > 0.9 and self.processed_points_count > self.constants.INITIAL_STABILIZATION_PERIOD:
            self._transient_learning_buffer.append(transients)
            if len(self._transient_learning_buffer) > 100:
                mean_t = np.mean(self._transient_learning_buffer)
                std_t = np.std(self._transient_learning_buffer)
                new_thresh = max(7, round(mean_t + 2.5 * std_t))
                if self.dynamic_transient_threshold != new_thresh and abs(new_thresh - self.dynamic_transient_threshold) < 5:
                    logger.info(f"Continuously adapting transient threshold. Old: {self.dynamic_transient_threshold}, New: {new_thresh}")
                    self.dynamic_transient_threshold = new_thresh
        
        if belief["RapidFailure"] > getattr(self.config, 'belief_threshold_rapid_failure', 0.5):
            assessment = "NGUY HIỂM: Phát hiện xác suất cao về dịch chuyển nhanh!"
        elif belief["SlowCreep"] > getattr(self.config, 'belief_threshold_slow_creep', 0.6):
            assessment = "CẢNH BÁO: Phát hiện dịch chuyển trôi chậm kéo dài."
        
        return belief, assessment
        
    def _get_classification_from_velocity(self, velocity_mmps):
        for level in self.classification_table:
            try:
                if velocity_mmps >= float(level.get('mm_giay', 0)):
                    return level.get('name', 'N/A')
            except (ValueError, TypeError):
                continue
        return self.classification_table[-1].get('name', 'Extremely Slow') if self.classification_table else 'N/A'

class MqttProcessor:
    def __init__(self, config, classification_table, topics, config_parser):
        self.config = config
        self.message_queue = queue.Queue(maxsize=1000)
        self.engine = MasterControlEngine(config, classification_table, config_parser)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata={'topics': topics})
        if config.username: self.client.username_pw_set(config.username, config.password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.last_message_timestamp = time.time()
        self.rate_limit_tracker = {}
        self.last_reconnect_attempt = 0
        self.state_lock = threading.Lock()
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
        now = time.time()
        source_id = msg.topic

        with self.state_lock:
            last_time = self.rate_limit_tracker.get(source_id)
            if last_time and (now - last_time) < self.config.rate_limit_min_interval_sec:
                return
            
            self.rate_limit_tracker[source_id] = now
            with self.watchdog_lock:
                self.last_message_timestamp = now
        try:
            self.message_queue.put(msg.payload.decode("utf-8"), block=False)
        except queue.Full:
            logger.warning("Message queue is full. Discarding incoming message. System may be overloaded.")
            
    def run(self):
        try:
            logger.info(f"Connecting to {self.config.broker}:{self.config.port}...")
            self.client.connect(self.config.broker, self.config.port, 60)
            self.client.loop_start()

            time_in_cooldown_start = None
            cooldown_alert_level = 0
            cooldown_timeout = getattr(self.config, 'cooldown_watchdog_timeout_sec', 900)
            last_heartbeat_time = time.time()
            HEARTBEAT_INTERVAL = 30

            while True:
                if time.time() - last_heartbeat_time > HEARTBEAT_INTERVAL:
                    heartbeat_report = {"type": "HEARTBEAT", "timestamp": time.time()}
                    print(json.dumps(heartbeat_report), flush=True)
                    last_heartbeat_time = time.time()
                    
                try:
                    payload = self.message_queue.get(timeout=1.0)
                    
                    report = self.engine.process_payload(payload)
                    if report:
                        print(json.dumps(report, ensure_ascii=False)); sys.stdout.flush()

                except queue.Empty:
                    with self.watchdog_lock:
                        time_since_last_msg = time.time() - self.last_message_timestamp
                    
                    if time_since_last_msg > self.config.fatal_watchdog_timeout_sec:
                        raise FatalWatchdogError(f"No message received for over {self.config.fatal_watchdog_timeout_sec} seconds.")
                    
                    if time_since_last_msg > self.config.watchdog_timeout_sec and (time.time() - self.last_reconnect_attempt > 60):
                        logger.warning(f"DATA WATCHDOG: No message for {time_since_last_msg:.0f}s. Attempting to reconnect MQTT...")
                        try:
                            self.client.reconnect()
                        except Exception as e:
                            logger.error(f"MQTT reconnect attempt failed: {e}")
                        self.last_reconnect_attempt = time.time()
                
                if self.engine.filter_in_cooldown:
                    if time_in_cooldown_start is None:
                        time_in_cooldown_start = time.time()
                    
                    duration = time.time() - time_in_cooldown_start
                    if duration > cooldown_timeout:
                        raise FatalWatchdogError(f"System stuck in cooldown mode for over {cooldown_timeout} seconds.")
                    
                    elif duration > cooldown_timeout * 0.5 and cooldown_alert_level < 1:
                        logger.warning(f"HIGH ALERT: System has been in cooldown for over {duration:.0f} seconds.")
                        cooldown_alert_level = 1
                else:
                    if time_in_cooldown_start is not None:
                        logger.info("System has recovered from cooldown mode.")
                    time_in_cooldown_start = None
                    cooldown_alert_level = 0

        except FatalWatchdogError as e:
            logger.critical(f"FATAL WATCHDOG TRIGGERED: {e}. Initiating graceful shutdown.")
        except KeyboardInterrupt:
            logger.info("Termination signal received (Ctrl+C). Initiating graceful shutdown.")
        except Exception as e:
            logger.critical(f"An unhandled exception occurred in the main processing loop: {e}", exc_info=True)
        finally:
            logger.info("Stopping MQTT network loop...")
            self.client.loop_stop()
            logger.info("Disconnecting from MQTT broker...")
            self.client.disconnect()
            logger.info("Cleanup complete. Application will now exit.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Self-Correcting Bayesian Inference Engine for GNSS Data.")
    parser.add_argument('--broker', required=True, help='MQTT Broker address')
    parser.add_argument('--port', type=int, required=True, help='MQTT Broker port')
    parser.add_argument('--username', default='', help='MQTT username (optional)')
    parser.add_argument('--password', default='', help='MQTT password (optional)')
    parser.add_argument('--topic', dest='topics', action='append', required=True, help='MQTT topic(s) to subscribe to for GNSS data (can be specified multiple times)')
    parser.add_argument('--classification-table', required=True, help='JSON string of the speed classification table.')
    args = parser.parse_args()

    config_parser = configparser.ConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(base_dir, 'shifting_config.ini')
    if not os.path.exists(config_file_path):
        logger.critical(f"FATAL: Configuration file '{config_file_path}' not found! Please create it. Exiting.")
        sys.exit(1)
    
    config_parser.read(config_file_path, encoding='utf-8')
    logger.info(f"Successfully loaded configuration from '{config_file_path}'.")

    try:
        se_config = config_parser['ShiftingEngine']
        pf_config = config_parser['ParticleFilter']
        ar_config = config_parser['AnalysisAndReporting']
        wp_config = config_parser['WatchdogAndPerformance']

        args.num_particles = se_config.getint('num_particles', 2000)
        
        args.belief_threshold_slow_creep = ar_config.getfloat('belief_threshold_slow_creep', 0.6)
        args.belief_threshold_rapid_failure = ar_config.getfloat('belief_threshold_rapid_failure', 0.5)

        args.watchdog_timeout_sec = wp_config.getint('watchdog_timeout_sec', 60)
        args.fatal_watchdog_timeout_sec = wp_config.getint('fatal_watchdog_timeout_sec', 600)
        args.rate_limit_min_interval_sec = wp_config.getfloat('rate_limit_min_interval_sec', 0.5)
        args.cooldown_watchdog_timeout_sec = wp_config.getint('cooldown_watchdog_timeout_sec', 900)
       
    except (configparser.NoSectionError, KeyError, ValueError) as e:
        logger.critical(f"FATAL: Error reading configuration from '{config_file_path}'. Missing section or invalid key/value: {e}. Exiting.")
        sys.exit(1)
    try:
        classification_data = json.loads(args.classification_table)
        processor = MqttProcessor(
            config=args, 
            classification_table=classification_data, 
            topics=args.topics,
            config_parser=config_parser
        )
        processor.run()
    except json.JSONDecodeError:
        logger.critical("Failed to parse --classification-table argument. It is not a valid JSON string. Exiting.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during program initialization: {e}", exc_info=True)
        sys.exit(1)
