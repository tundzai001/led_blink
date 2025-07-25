import tkinter as tk
from tkinter import messagebox, filedialog, Toplevel
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tksheet import Sheet
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import threading
import time
import warnings
import configparser
import signal
import os
import sys
import queue
from collections import deque
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import numpy as np
os.environ['PYGAME_HIDE_SUPPORT_PROMPT'] = "1"
import pygame
import RPi.GPIO as GPIO

# --- CÁC HẰNG SỐ TOÀN CỤC ---
warnings.filterwarnings("ignore", category=DeprecationWarning)
CONFIG_FILE = 'config.ini'
SESSION_FILE = "session.json"
DATA_CLEAR_SIGNAL = "CLEAR_ALL_DATA"
LED1_PIN = 3
LED2_PIN = 27
MAX_PLOT_POINTS = 10000
SOUNDS_DIR = "/home/vippro123/Desktop/code/sounds"
# ==============================================================================
# LỚP LOGIC NỀN (BACKEND) - Logic cảnh báo đa cấp
# ==============================================================================
class Backend:
    def __init__(self):
        self.listening = False
        self.exiting = False
        self.status_text = "Trạng thái: THỦ CÔNG"
        self.status_color = "red"
        self.config = configparser.ConfigParser()
        self.broker, self.port, self.username, self.password = "aitogy.xyz", 1883, "abc", "xyz"
        self.publish_topic = ""
        self.subscribe_topics = []
        self.warning_threshold = 1.0
        self.critical_threshold = 1.2
        self.led1_pin, self.led2_pin = LED1_PIN, LED2_PIN
        self.warning_sound = None
        self.critical_sound = None
        self.siren_sound = None
        self.decreasing_sound = None
        self.safe_sound_1 = None # Dành cho file "safe.mp3"
        self.safe_sound_2 = None # Dành cho file "safe2.mp3"
        self.alert_thread = None
        self.mixer_initialized = False
        self.current_alert_level = 0        # 0: An toàn, 1: Cảnh báo, 2: Nguy hiểm
        self.safe_readings_count = 0        # Đếm số lần đọc an toàn liên tiếp
        self.warning_readings_count = 0     # Đếm số lần cảnh báo sau khi TĂNG
        self.decreasing_warning_count = 0 # Đếm số lần cảnh báo sau khi GIẢM
        self.was_in_high_level_state = False # Cờ báo đã từng ở mức cao (cảnh báo/nguy hiểm)
        self.initial_safe_played = False    # Cờ báo đã phát âm thanh an toàn lần đầu
        self.safe_return_phase = 0          # Giai đoạn trở về an toàn (0: chưa, 1: đếm đến 10, 2: đếm đến 15, 3: xong)
        self.sensor_data = []
        self.plot_data_points = deque(maxlen=MAX_PLOT_POINTS)
        self.gui_update_queue = queue.Queue()
        self.client = mqtt.Client(protocol=mqtt.MQTTv311)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.stop_event = threading.Event()
        self.setup_audio_mixer()
        self.setup_gpio()
        self.load_config()

    def setup_audio_mixer(self):
        try:
            pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=4096)
            self.mixer_initialized = True
        except Exception as e:
            print(f"LỖI: Không thể khởi tạo pygame mixer: {e}")
            self.mixer_initialized = False

    def _load_audio_files(self):
        if not self.mixer_initialized: return
        SIREN_FILE = os.path.join(SOUNDS_DIR, "coi1.mp3")
        WARNING_FILE = os.path.join(SOUNDS_DIR, "warning.mp3")
        DANGER_FILE = os.path.join(SOUNDS_DIR, "danger.mp3")
        DECREASE_FILE = os.path.join(SOUNDS_DIR, "decrease.mp3")
        SAFE_FILE_1 = os.path.join(SOUNDS_DIR, "safe.mp3")
        SAFE_FILE_2 = os.path.join(SOUNDS_DIR, "safe2.mp3")
        try:
            if os.path.exists(SIREN_FILE): self.siren_sound = pygame.mixer.Sound(SIREN_FILE)
            else: print(f"CẢNH BÁO: Không tìm thấy file {SIREN_FILE}")
            if os.path.exists(WARNING_FILE): self.warning_sound = pygame.mixer.Sound(WARNING_FILE)
            else: print(f"CẢNH BÁO: Không tìm thấy file {WARNING_FILE}")
            if os.path.exists(DANGER_FILE): self.critical_sound = pygame.mixer.Sound(DANGER_FILE)
            else: print(f"CẢNH BÁO: Không tìm thấy file {DANGER_FILE}")
            if os.path.exists(DECREASE_FILE): self.decreasing_sound = pygame.mixer.Sound(DECREASE_FILE)
            else: print(f"CẢNH BÁO: Không tìm thấy file {DECREASE_FILE}")
            if os.path.exists(SAFE_FILE_1): self.safe_sound_1 = pygame.mixer.Sound(SAFE_FILE_1)
            else: print(f"CẢNH BÁO: Không tìm thấy file {SAFE_FILE_1}")
            if os.path.exists(SAFE_FILE_2): self.safe_sound_2 = pygame.mixer.Sound(SAFE_FILE_2)
            else: print(f"CẢNH BÁO: Không tìm thấy file {SAFE_FILE_2}")
        except Exception as e:
            print(f"LỖI khi tải file âm thanh: {e}")

    def on_message(self, client, userdata, msg):
        if not self.listening or self.exiting: return
        try:
            data = json.loads(msg.payload.decode())
            if "value" not in data:
                status_info = data.get("status", f"Tin nhắn không chứa key 'value': {data}")
                print(f"INFO: Nhận được tin nhắn không phải dữ liệu, bỏ qua: '{status_info}'")
                return # Dừng xử lý ngay lập tức
            value = float(data.get("value"))
            previous_level = self.current_alert_level
            new_level = 0
            if value >= self.critical_threshold:
                new_level = 2 # Nguy hiểm
            elif value >= self.warning_threshold:
                new_level = 1 # Cảnh báo
            else:
                new_level = 0 # An toàn
            if new_level == 0:
                self.warning_readings_count = 0
                self.decreasing_warning_count = 0
                self.safe_readings_count += 1
                # A1. An toàn lần đầu (chưa từng có cảnh báo)
                if not self.was_in_high_level_state:
                    if not self.initial_safe_played and self.safe_readings_count == 10:
                        print("An toàn ban đầu: Đủ 10 lần, phát 'safe.mp3'.")
                        self._play_sequence_in_thread([self.safe_sound_1])
                        self.initial_safe_played = True
                # A2. Quay về an toàn sau khi đã có cảnh báo
                else:
                    if self.safe_return_phase == 0:
                         self.safe_return_phase = 1
                    if self.safe_return_phase == 1 and self.safe_readings_count == 10:
                        print("Về an toàn: Đủ 10 lần, phát 'safe2.mp3'.")
                        self._play_sequence_in_thread([self.safe_sound_2])
                        self.safe_return_phase = 2 # Chuyển sang giai đoạn đếm tiếp
                    elif self.safe_return_phase == 2 and self.safe_readings_count == 15:
                        print("Về an toàn: Đủ 15 lần, phát 'safe2.mp3' lần cuối.")
                        self._play_sequence_in_thread([self.safe_sound_2])
                        self.safe_return_phase = 3 # Hoàn thành
                        self.was_in_high_level_state = False # Reset cờ, coi như đã xử lý xong đợt nguy hiểm
                        self.initial_safe_played = True # Đánh dấu đã phát âm thanh an toàn
            # B. TRẠNG THÁI CẢNH BÁO (new_level == 1)
            elif new_level == 1:
                self.safe_readings_count = 0
                self.safe_return_phase = 0
                if not self.was_in_high_level_state:
                    self.was_in_high_level_state = True
                # B1. Tăng từ An toàn lên Cảnh báo
                if previous_level == 0:
                    print("TĂNG lên Cảnh báo. Phát 'coi1' -> 'warning'.")
                    self._play_sequence_in_thread([self.siren_sound, self.warning_sound])
                    self.warning_readings_count = 1
                    self.decreasing_warning_count = 0
                # B2. Giảm từ Nguy hiểm xuống Cảnh báo
                elif previous_level == 2:
                    print("GIẢM từ Nguy hiểm xuống Cảnh báo. Phát 'decrease'.")
                    self._play_sequence_in_thread([self.decreasing_sound])
                    self.decreasing_warning_count = 1
                    self.warning_readings_count = 0
                # B3. Duy trì ở mức Cảnh báo
                elif previous_level == 1:
                    if self.warning_readings_count > 0:
                        self.warning_readings_count += 1
                        print(f"Duy trì Cảnh báo (tăng), lần {self.warning_readings_count}.")
                        if self.warning_readings_count % 6 == 0: # Cứ 6 lần (60s)
                             print("Phát lại cảnh báo tăng: 'coi1' -> 'warning'.")
                             self._play_sequence_in_thread([self.siren_sound, self.warning_sound])
                    elif self.decreasing_warning_count > 0:
                        self.decreasing_warning_count += 1
                        print(f"Duy trì Cảnh báo (giảm), lần {self.decreasing_warning_count}.")
                        if self.decreasing_warning_count % 10 == 0: # Cứ 10 lần
                            print("Phát lại cảnh báo giảm: 'decrease'.")
                            self._play_sequence_in_thread([self.decreasing_sound])
            # C. TRẠNG THÁI NGUY HIỂM (new_level == 2)
            elif new_level == 2:
                self.safe_readings_count = 0
                self.warning_readings_count = 0
                self.decreasing_warning_count = 0
                self.safe_return_phase = 0
                if not self.was_in_high_level_state:
                    self.was_in_high_level_state = True
                print("NGUY HIEM")
                self._play_sequence_in_thread([self.siren_sound, self.critical_sound, self.siren_sound])
            if new_level != previous_level:
                if new_level == 0 and previous_level > 0:
                    self.safe_readings_count = 1
                print(f"Chuyển trạng thái từ {previous_level} -> {new_level}")
            self.current_alert_level = new_level
            name = data.get("sensorname", msg.topic)
            ts = float(data.get("timestamp", time.time()))
            dt_object = datetime.fromtimestamp(ts)
            status = "NGUY HIEM" if new_level == 2 else ("CANH BAO" if new_level == 1 else "AN TOAN")
            record = (name, str(value), status, dt_object.strftime("%H:%M:%S %d-%m"))
            self.sensor_data.append(record)
            self.plot_data_points.append((dt_object, value))
            self.gui_update_queue.put(record)
            threading.Thread(target=self.flash_led, args=(self.led1_pin,), daemon=True).start()
            if new_level > 0:
                threading.Thread(target=self.flash_led, args=(self.led2_pin,), daemon=True).start()
            if self.publish_topic:
                payload_out_str = f"{name},{value},{status},{ts}"
                self.client.publish(self.publish_topic, payload_out_str)
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print(f"Lỗi xử lý message: {e}")

    def stop_all_alerts(self):
        if self.mixer_initialized:
            pygame.mixer.stop()

    def _play_sequence_in_thread(self, sound_list):
        self.stop_all_alerts()
        if self.alert_thread and self.alert_thread.is_alive():
            pass

        def target():
            if not self.mixer_initialized: return
            for sound in sound_list:
                if sound:
                    if threading.current_thread() != self.alert_thread:
                        return
                    print(f" -> Đang phát một âm thanh trong chuỗi...")
                    sound.play()
                    while pygame.mixer.get_busy():
                        if threading.current_thread() != self.alert_thread:
                            pygame.mixer.stop()
                            return
                        time.sleep(0.1)
                else:
                    print("CẢNH BÁO: Bỏ qua file âm thanh không tồn tại trong chuỗi.")
                time.sleep(0.2)
        self.alert_thread = threading.Thread(target=target, daemon=True)
        self.alert_thread.start()

    def update_and_reconnect(self, settings: dict):
        self.broker, self.port = settings['broker'], int(settings['port'])
        self.username, self.password = settings['username'], settings['password']
        self.publish_topic = settings['publish']
        self.subscribe_topics = [t for t in settings['topics'].splitlines() if t]
        self.warning_threshold = float(settings['warning_threshold'])
        self.critical_threshold = float(settings['critical_threshold'])
        self.save_config()
        if self.listening:
            self.toggle_off()
            time.sleep(1)
            self.toggle_on()

    def load_config(self):
        if not os.path.exists(CONFIG_FILE):
            self._load_audio_files()
            return
        try:
            self.config.read(CONFIG_FILE)
            if "MQTT" in self.config:
                mqtt_cfg = self.config["MQTT"]
                self.broker = mqtt_cfg.get("broker", self.broker)
                self.port = mqtt_cfg.getint("port", self.port)
                self.username = mqtt_cfg.get("username", self.username)
                self.password = mqtt_cfg.get("password", self.password)
                self.subscribe_topics = [t for t in mqtt_cfg.get("topics", "").splitlines() if t]
                self.publish_topic = mqtt_cfg.get("publish", self.publish_topic)
            if "Settings" in self.config:
                settings_cfg = self.config["Settings"]
                self.warning_threshold = settings_cfg.getfloat("warning_threshold", self.warning_threshold)
                self.critical_threshold = settings_cfg.getfloat("critical_threshold", self.critical_threshold)
            print("Đã tải cấu hình.")
            self._load_audio_files()
        except Exception as e:
            print(f"Lỗi khi tải cấu hình từ {CONFIG_FILE}: {e}")

    def save_config(self):
        self.config['MQTT'] = {
            'broker': self.broker, 'port': self.port, 'username': self.username,
            'password': self.password, 'topics': "\n".join(self.subscribe_topics),
            'publish': self.publish_topic
        }
        self.config['Settings'] = {
            'warning_threshold': self.warning_threshold,
            'critical_threshold': self.critical_threshold
        }
        try:
            with open(CONFIG_FILE, 'w') as f: self.config.write(f)
            print("Đã lưu cấu hình.")
        except IOError as e:
            print(f"Lỗi Lưu File: {e}")

    def shutdown(self, silent=False):
        if self.exiting: return
        if not silent: print("\nBắt đầu quá trình dọn dẹp để thoát...")
        self.exiting = True
        self.stop_event.set()
        self.stop_all_alerts()
        self.alert_thread = None
        try:
            self.client.loop_stop(force=True)
            self.client.disconnect()
        except Exception: pass
        GPIO.cleanup()
        if not silent: print(" -> Backend đã dừng.")

    def setup_gpio(self):
        try:
            GPIO.setmode(GPIO.BCM)
            GPIO.setwarnings(False)
            GPIO.setup(self.led1_pin, GPIO.OUT, initial=GPIO.LOW)
            GPIO.setup(self.led2_pin, GPIO.OUT, initial=GPIO.LOW)
            print("GPIO setup successful.")
        except Exception as e:
            print(f"Lỗi khi cài đặt GPIO: {e}")

    def start_background_tasks(self):
        self.load_session_data()
        threading.Thread(target=self.auto_clear_scheduler, daemon=True).start()
        print("Đã khởi chạy các tác vụ nền.")

    def flash_led(self, pin, duration=0.3):
        try:
            GPIO.output(pin, GPIO.HIGH)
            time.sleep(duration)
            GPIO.output(pin, GPIO.LOW)
        except Exception as e:
            print(f"Lỗi nháy LED trên pin {pin}: {e}")

    def on_connect(self, client, userdata, flags, rc):
        if self.exiting: return
        if rc == 0:
            print("MQTT Connected successfully.")
            self.status_text, self.status_color = "Trạng thái: TỰ ĐỘNG", "green"
            for t in self.subscribe_topics:
                client.subscribe(t)
                print(f"Subscribed: {t}")
            if not self.subscribe_topics:
                self.status_text = "Trạng thái: TỰ ĐỘNG (Không có topic)"
        else:
            print(f"Failed to connect, return code {rc}")
            self.status_text, self.status_color = "Trạng thái: LỖI KẾT NỐI", "red"
            self.listening = False

    def on_disconnect(self, client, userdata, rc):
        if not self.exiting and self.listening:
            print("Mất kết nối MQTT...")
            self.status_text, self.status_color = "Trạng thái: MẤT KẾT NỐI", "orange"

    def get_gui_updates(self):
        updates = []
        while not self.gui_update_queue.empty():
            try:
                updates.append(self.gui_update_queue.get_nowait())
            except queue.Empty:
                break
        return updates

    def toggle_on(self):
        if self.listening: return
        self.listening = True
        self.status_text, self.status_color = "Trạng thái: ĐANG KẾT NỐI...", "orange"
        if not self.broker:
            self.listening = False
            self.status_text, self.status_color = "Trạng thái: THỦ CÔNG (Lỗi Broker)", "red"
            return

        self.client.username_pw_set(self.username, self.password)
        try:
            print(f"Đang kết nối tới MQTT broker: {self.broker}:{self.port}...")
            self.client.connect_async(self.broker, self.port, 60)
            self.client.loop_start()
        except Exception as e:
            self.listening = False
            self.status_text, self.status_color = "Trạng thái: LỖI KẾT NỐI", "red"
            print(f"Lỗi kết nối MQTT: {e}")

    def toggle_off(self):
        if not self.listening: return
        self.listening = False
        try:
            self.client.loop_stop()
            self.client.disconnect()
            print("Đã ngắt kết nối MQTT.")
        except Exception: pass
        self.status_text, self.status_color = "Trạng thái: THỦ CÔNG", "red"

    def check_leds(self):
        if self.listening:
            print("Không thể kiểm tra LED ở chế độ TỰ ĐỘNG.")
            return False
        threading.Thread(target=self._run_led_check, daemon=True).start()
        return True

    def _run_led_check(self):
        print("Kiểm tra LED...")
        self.flash_led(self.led1_pin, duration=0.5)
        time.sleep(0.1)
        self.flash_led(self.led2_pin, duration=0.5)

    def auto_clear_scheduler(self):
        while not self.stop_event.is_set():
            now = datetime.now()
            if now.hour == 0 and now.minute == 0:
                print("Đã đến 00:00, tự động xóa dữ liệu...")
                self.clear_all_data()
                time.sleep(61)
            else:
                time.sleep(30)

    def clear_all_data(self):
        self.sensor_data.clear()
        self.plot_data_points.clear()
        self.gui_update_queue.put(DATA_CLEAR_SIGNAL)
        print("Đã xóa dữ liệu nền.")

    def save_session_data(self, silent=False):
        if not silent: print(" -> Đang lưu trạng thái hiện tại vào file...")
        try:
            plot_data_serializable = [(dt.isoformat(), val) for dt, val in self.plot_data_points]
            session = {"sensor_data": self.sensor_data, "plot_data_points": plot_data_serializable}
            with open(SESSION_FILE, "w") as f: json.dump(session, f)
            if not silent: print(f" -> Đã lưu trạng thái vào {SESSION_FILE}")
        except Exception as e:
            print(f" -> Lỗi khi lưu trạng thái: {e}")

    def load_session_data(self):
        if not os.path.exists(SESSION_FILE): return
        print(f" -> Tìm thấy file trạng thái {SESSION_FILE}, đang tải lại dữ liệu...")
        try:
            with open(SESSION_FILE, "r") as f: session = json.load(f)
            self.sensor_data = session.get("sensor_data", [])
            plot_data_serializable = session.get("plot_data_points", [])
            self.plot_data_points.clear()
            for dt_str, val in plot_data_serializable:
                self.plot_data_points.append((datetime.fromisoformat(dt_str), val))
            for record in self.sensor_data: self.gui_update_queue.put(record)
            print(" -> Đã tải lại dữ liệu thành công.")
        except Exception as e:
            print(f" -> Lỗi khi tải trạng thái: {e}")
        finally:
            if os.path.exists(SESSION_FILE): os.remove(SESSION_FILE)

# ==============================================================================
# LỚP GIAO DIỆN NGƯỜI DÙNG (GUI)
# ==============================================================================
class AppGUI:
    def __init__(self, root: tk.Toplevel, backend: Backend, on_close_callback):
        self.root = root
        self.backend = backend
        self.on_close_callback = on_close_callback
        self.root.title("Giao diện Cảm biến & Điều khiển LED")
        self.root.geometry(f"{self.root.winfo_screenwidth()}x{self.root.winfo_screenheight()-70}+0+0")
        self.chart_window = None
        self.settings_window = None # Cửa sổ cài đặt
        self.warning_threshold_var = tk.StringVar()
        self.critical_threshold_var = tk.StringVar()
        self.CONVERSION_FACTORS = {"m": 1.0, "cm": 100.0, "mm": 1000.0, "ft": 3.28084}
        self.points_per_view = 40
        self.current_start_index = 0
        self.last_highlighted_row = None
        self._is_updating_slider = False
        self._slider_after_id = None
        self.create_widgets()
        self.load_initial_data()
        self.root.after(250, self.periodic_update)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close_window)

    def create_left_panel(self, parent):
        left = ttk.LabelFrame(parent, text="Cài đặt MQTT", padding=10)
        left.grid(row=0, column=0, sticky="nsw", padx=(0, 15))

        def add_labeled_entry(frame, label, row, show=None):
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=3)
            entry = ttk.Entry(frame, show=show)
            entry.grid(row=row, column=1, sticky="ew", pady=3, columnspan=2)
            return entry

        self.broker_entry = add_labeled_entry(left, "MQTT Broker:", 0)
        self.port_entry = add_labeled_entry(left, "Port:", 1)
        self.user_entry = add_labeled_entry(left, "Username:", 2)
        self.pass_entry = add_labeled_entry(left, "Password:", 3, show="*")
        show_btn = ttk.Button(left, text="👁", command=self.toggle_pass, width=2, bootstyle="light")
        show_btn.grid(row=3, column=2, sticky="e")
        self.pub_entry = add_labeled_entry(left, "Publish Topic:", 4)

        # ---- SỬA ĐỔI: Chia ô Sub topic thành hai ô Water và GNSS ----
        ttk.Label(left, text="Water Sub Topic:").grid(row=5, column=0, columnspan=3, sticky="w", pady=(10, 2))
        self.water_topic_entry = ttk.Entry(left)
        self.water_topic_entry.grid(row=6, column=0, columnspan=3, pady=(0, 5), sticky="ew")

        ttk.Label(left, text="GNSS Sub Topic:").grid(row=7, column=0, columnspan=3, sticky="w", pady=(10, 2))
        self.gnss_topic_entry = ttk.Entry(left)
        self.gnss_topic_entry.grid(row=8, column=0, columnspan=3, pady=(0, 5), sticky="ew")
        # ---- KẾT THÚC SỬA ĐỔI ----

        # Điều chỉnh vị trí các nút bên dưới
        ttk.Button(left, text="Cài đặt", command=self.open_settings_window, bootstyle="secondary").grid(row=9, column=0, columnspan=3, sticky="ew", pady=(10, 5))
        ttk.Button(left, text="Lưu & Áp dụng", command=self.apply_and_save_config, bootstyle="primary").grid(row=10, column=0, columnspan=3, sticky="ew", pady=(5,0))


    def open_settings_window(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.lift()
            return

        self.settings_window = Toplevel(self.root)
        self.settings_window.title("Cài đặt Nâng cao")
        self.settings_window.geometry("400x250")
        self.settings_window.transient(self.root) # Giữ cửa sổ này luôn ở trên cửa sổ chính

        notebook = ttk.Notebook(self.settings_window)
        notebook.pack(pady=10, padx=10, fill="both", expand=True)

        # Tab 1: GNSS
        gnss_frame = ttk.Frame(notebook, padding="10")
        notebook.add(gnss_frame, text='GNSS')
        ttk.Label(gnss_frame, text="Các cài đặt cho GNSS sẽ được bổ sung tại đây.").pack(pady=20)

        # Tab 2: Mực nước
        water_level_frame = ttk.Frame(notebook, padding="10")
        notebook.add(water_level_frame, text='Mực nước')

        # Thêm các entry cho ngưỡng vào tab Mực nước
        def add_labeled_entry_settings(frame, label, row, textvariable):
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=5, padx=5)
            entry = ttk.Entry(frame, textvariable=textvariable)
            entry.grid(row=row, column=1, sticky="ew", pady=5, padx=5)
            frame.grid_columnconfigure(1, weight=1)

        add_labeled_entry_settings(water_level_frame, "Ngưỡng Cảnh Báo (m):", 0, self.warning_threshold_var)
        add_labeled_entry_settings(water_level_frame, "Ngưỡng Nguy Hiểm (m):", 1, self.critical_threshold_var)

        # Nút để đóng cửa sổ cài đặt
        ttk.Button(self.settings_window, text="Đóng", command=self.settings_window.destroy).pack(pady=10)


    def load_initial_data(self):
        # Load dữ liệu MQTT
        self.broker_entry.insert(0, self.backend.broker)
        self.port_entry.insert(0, str(self.backend.port))
        self.user_entry.insert(0, self.backend.username)
        self.pass_entry.insert(0, self.backend.password)
        self.pub_entry.insert(0, self.backend.publish_topic)
        
        # ---- SỬA ĐỔI: Tải dữ liệu vào hai ô topic ----
        topics = self.backend.subscribe_topics
        if len(topics) > 0:
            self.water_topic_entry.insert(0, topics[0])
        if len(topics) > 1:
            self.gnss_topic_entry.insert(0, topics[1])
        # ---- KẾT THÚC SỬA ĐỔI ----

        # Load dữ liệu ngưỡng vào các biến StringVar
        self.warning_threshold_var.set(str(self.backend.warning_threshold))
        self.critical_threshold_var.set(str(self.backend.critical_threshold))

    def apply_and_save_config(self, show_message=True):
        # Lấy giá trị ngưỡng từ các biến StringVar
        warning_thresh_val = self.warning_threshold_var.get()
        critical_thresh_val = self.critical_threshold_var.get()

        # ---- SỬA ĐỔI: Lấy dữ liệu từ hai ô topic ----
        water_topic = self.water_topic_entry.get().strip()
        gnss_topic = self.gnss_topic_entry.get().strip()
        all_topics_list = []
        if water_topic:
            all_topics_list.append(water_topic)
        if gnss_topic:
            all_topics_list.append(gnss_topic)
        topics_string = "\n".join(all_topics_list)
        # ---- KẾT THÚC SỬA ĐỔI ----

        settings = {
            'broker': self.broker_entry.get(),
            'port': self.port_entry.get(),
            'username': self.user_entry.get(),
            'password': self.pass_entry.get(),
            'topics': topics_string,
            'publish': self.pub_entry.get(),
            'warning_threshold': warning_thresh_val,
            'critical_threshold': critical_thresh_val
        }
        try:
            # Xác thực dữ liệu ngưỡng
            float(settings['warning_threshold'])
            float(settings['critical_threshold'])
            self.backend.update_and_reconnect(settings)
            if show_message:
                messagebox.showinfo("Thành công", "Đã lưu và áp dụng cấu hình.", parent=self.root)
        except ValueError:
            messagebox.showerror("Lỗi", "Giá trị ngưỡng không hợp lệ. Vui lòng nhập số.", parent=self.root)
        except Exception as e:
            messagebox.showerror("Lỗi", f"Không thể áp dụng cấu hình: {e}", parent=self.root)


    def periodic_update(self):
        if not self.root.winfo_exists(): return
        self.update_status_label()
        new_updates = self.backend.get_gui_updates()
        if new_updates:
            if DATA_CLEAR_SIGNAL in new_updates:
                self.sheet.set_sheet_data([])
                if self.chart_window and self.chart_window.winfo_exists(): self.clear_chart_data()
                print("GUI đã nhận tín hiệu và xóa bảng.")
            else:
                valid_records = [rec for rec in new_updates if isinstance(rec, tuple)]
                if valid_records:
                    self.sheet.insert_rows(valid_records)
                    self.sheet.dehighlight_all()
                    last_row_index = self.sheet.get_total_rows() - 1
                    if last_row_index >= 0:
                        self.sheet.see(row=last_row_index)
                        self.sheet.deselect()
                        last_record = valid_records[-1]
                        status = last_record[2]
                        if status == "NGUY HIEM":
                            highlight_color = "#F8D7DA"
                        elif status == "CANH BAO":
                            highlight_color = "#FFF3CD"
                        else:
                            highlight_color = "#D4EDDA"
                        self.sheet.highlight_rows(rows=[last_row_index], bg=highlight_color, fg="black")
            if self.chart_window and self.chart_window.winfo_exists(): self.update_plot()
        self.root.after(250, self.periodic_update)

    def destroy_all_windows(self):
        if self.settings_window and self.settings_window.winfo_exists(): self.settings_window.destroy()
        if self.chart_window and self.chart_window.winfo_exists(): self.chart_window.destroy()
        if self.root and self.root.winfo_exists(): self.root.destroy()

    def on_close_window(self):
        print("Đã đóng cửa sổ giao diện. Gõ 'show' trong terminal để mở lại.")
        self.on_close_callback()
        self.destroy_all_windows()

    def exit_program_graceful(self):
        if messagebox.askokcancel("Xác nhận", "Bạn có chắc muốn thoát hoàn toàn chương trình?", parent=self.root):
            print("Tự động lưu cấu hình hiện tại trước khi thoát...")
            self.apply_and_save_config(show_message=False) # Lưu lại các thay đổi cuối cùng
            self.on_close_callback(shutdown=True)

    def create_widgets(self):
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill="both", expand=True)
        main.grid_columnconfigure(1, weight=1)
        main.grid_columnconfigure(0, weight=0)
        main.grid_rowconfigure(0, weight=1)
        self.create_left_panel(main)
        self.create_right_panel(main)

    def create_right_panel(self, parent):
        right = ttk.Frame(parent)
        right.grid(row=0, column=1, sticky="nsew")
        right.grid_rowconfigure(1, weight=1)
        right.grid_columnconfigure(0, weight=1)
        self.status_label = ttk.Label(right, text="", font=("Arial", 11, "bold"))
        self.status_label.grid(row=0, column=0, sticky="ew", pady=(0, 5))
        sheet_frame = ttk.Frame(right)
        sheet_frame.grid(row=1, column=0, sticky="nsew")
        self.sheet = Sheet(sheet_frame, headers=["Tên", "Giá trị (m)", "Trạng thái", "Thời gian"], show_row_index=True)
        self.sheet.pack(fill=tk.BOTH, expand=True)
        self.sheet.disable_bindings()
        self.sheet.set_options(font=("Arial", 10, "normal"), header_font=("Arial", 10, "bold"), align="center")
        self.create_control_panel(right)

    def create_control_panel(self, parent_frame):
        bottom_part = ttk.Frame(parent_frame)
        bottom_part.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        ctrl = ttk.Frame(bottom_part)
        ctrl.pack(side=tk.TOP, fill=tk.X, expand=True)
        for i in range(5): ctrl.grid_columnconfigure(i, weight=1)
        ttk.Button(ctrl, text="Tự động (ON)", command=self.backend.toggle_on, bootstyle="success").grid(row=0, column=0, padx=2, sticky="ew")
        ttk.Button(ctrl, text="Thủ công (OFF)", command=self.backend.toggle_off, bootstyle="danger").grid(row=0, column=1, padx=2, sticky="ew")
        self.save_csv_button = ttk.Button(ctrl, text="Lưu CSV", command=self.save_to_csv, bootstyle="info")
        self.save_csv_button.grid(row=0, column=2, padx=2, sticky="ew")
        ttk.Button(ctrl, text="Xóa Dữ Liệu", command=self.clear_table_gui, bootstyle="warning").grid(row=0, column=3, padx=2, sticky="ew")
        ttk.Button(ctrl, text="Xem Biểu Đồ", command=self.show_chart_window, bootstyle="primary").grid(row=0, column=4, padx=2, sticky="ew")
        led_panel = ttk.LabelFrame(bottom_part, text="Kiểm tra Thiết bị", padding=5)
        led_panel.pack(side=tk.TOP, fill=tk.X, expand=True, pady=(10, 0))
        led_panel.grid_columnconfigure(0, weight=3)
        led_panel.grid_columnconfigure(1, weight=1)
        ttk.Button(led_panel, text="Kiểm tra LED", command=self.on_check_led_click).grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        ttk.Button(led_panel, text="Thoát", command=self.exit_program_graceful, bootstyle="secondary-outline").grid(row=0, column=1, padx=5, pady=5, sticky="ew")

    def update_status_label(self):
        if self.status_label.cget("text") != self.backend.status_text or self.status_label.cget("foreground") != self.backend.status_color:
            self.status_label.config(text=self.backend.status_text, foreground=self.backend.status_color)

    def toggle_pass(self): self.pass_entry.config(show="" if self.pass_entry.cget("show") else "*")

    def on_check_led_click(self):
        if not self.backend.check_leds(): messagebox.showwarning("Cảnh báo", "Chỉ có thể kiểm tra LED ở chế độ THỦ CÔNG (OFF).", parent=self.root)

    def clear_table_gui(self):
        if messagebox.askokcancel("Xác nhận", "Bạn có chắc muốn xóa toàn bộ dữ liệu hiện tại?", parent=self.root): self.backend.clear_all_data()

    def save_to_csv(self):
        if not self.backend.sensor_data: messagebox.showinfo("Thông báo", "Không có dữ liệu để lưu.", parent=self.root); return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")], title="Lưu file CSV", parent=self.root)
        if path:
            self.save_csv_button.config(state="disabled")
            threading.Thread(target=self._write_csv_in_background, args=(path, list(self.backend.sensor_data)), daemon=True).start()

    def _write_csv_in_background(self, path, data_to_save):
        try:
            import csv
            with open(path, "w", newline="", encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(self.sheet.headers())
                writer.writerows(data_to_save)
            if self.root.winfo_exists():
                self.root.after(0, lambda p=path: messagebox.showinfo("Thành công", f"Đã lưu dữ liệu vào {os.path.basename(p)}", parent=self.root))
        except Exception as e:
            if self.root.winfo_exists():
                self.root.after(0, lambda err=e: messagebox.showerror("Lỗi", f"Không thể lưu file:\n\n{err}", parent=self.root))
        finally:
            if self.root.winfo_exists():
                self.root.after(0, lambda: self.save_csv_button.config(state="normal"))

    def show_chart_window(self):
        if self.chart_window and self.chart_window.winfo_exists(): self.chart_window.lift(); return
        self.chart_window = tk.Toplevel(self.root)
        self.chart_window.title("Biểu đồ Dữ liệu Cảm biến")
        self.chart_window.geometry("900x650")
        self.chart_window.protocol("WM_DELETE_WINDOW", self.on_chart_close)
        top_frame = ttk.Frame(self.chart_window, padding=(10, 5))
        top_frame.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top_frame, text="Chọn đơn vị:").pack(side=tk.LEFT, padx=(0, 5))
        self.unit_selector = ttk.Combobox(top_frame, state="readonly", values=list(self.CONVERSION_FACTORS.keys()))
        self.unit_selector.set("m"); self.unit_selector.pack(side=tk.LEFT, padx=5)
        self.unit_selector.bind("<<ComboboxSelected>>", lambda e: self.update_plot())
        self.auto_follow_var = tk.BooleanVar(value=True)
        auto_follow_check = ttk.Checkbutton(top_frame, text="Tự động theo dõi", variable=self.auto_follow_var, command=self.on_auto_follow_toggle)
        auto_follow_check.pack(side=tk.LEFT, padx=20)
        self.current_start_index = 0
        chart_frame = ttk.Frame(self.chart_window, padding=(10, 5))
        chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.fig = Figure(figsize=(9, 4.5), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        slider_frame = ttk.Frame(self.chart_window, padding=10)
        slider_frame.pack(side=tk.BOTTOM, fill=tk.X)
        self.position_var = tk.DoubleVar()
        self.position_scale = ttk.Scale(slider_frame, from_=0, to=100, orient=tk.HORIZONTAL, variable=self.position_var, command=self.on_slider_change)
        self.position_scale.pack(side=tk.TOP, fill=tk.X, expand=True)
        self.info_label = ttk.Label(slider_frame, text="Tổng điểm: 0 | Hiển thị: 0-0", font=("Arial", 9))
        self.info_label.pack(side=tk.TOP, pady=(5, 0))
        self.update_plot()

    def clear_chart_data(self):
        self.current_start_index = 0
        if hasattr(self, 'auto_follow_var'):
            self.auto_follow_var.set(True)
        self.update_plot()

    def update_plot(self):
        if not (self.chart_window and self.chart_window.winfo_exists()): return
        all_data = list(self.backend.plot_data_points)
        total_points = len(all_data)
        if total_points > self.points_per_view:
            max_start = total_points - self.points_per_view
            self.current_start_index = min(self.current_start_index, max_start)
        start, end = self._update_slider_and_indices(total_points)
        display_data_slice = all_data[start:end]
        self.ax.clear()
        self._setup_plot_style()
        if not display_data_slice:
            self.ax.text(0.5, 0.5, 'Chưa có dữ liệu', ha='center', va='center', transform=self.ax.transAxes, fontsize=16, color='gray')
            self.info_label.config(text="Tổng điểm: 0 | Hiển thị: 0-0")
        else:
            indices, values, times, unit, warning_thresh, critical_thresh = self._prepare_plot_data(display_data_slice, start)
            self._setup_plot_style(unit)
            self._draw_plot_elements(indices, values, warning_thresh, critical_thresh, unit)
            self._configure_plot_axes(start, end, total_points, indices, times)
        self.canvas.draw()

    def _update_slider_and_indices(self, total_points):
        if total_points <= self.points_per_view:
            self.position_scale.config(state="disabled")
            self.current_start_index = 0
            self._is_updating_slider = True
            self.position_scale.set(0)
            self._is_updating_slider = False
        else:
            self.position_scale.config(state="normal")
            if self.auto_follow_var.get():
                self.current_start_index = max(0, total_points - self.points_per_view)
        if total_points > self.points_per_view:
            max_start_idx = max(0, total_points - self.points_per_view)
            pos_percent = (self.current_start_index / max_start_idx) * 100 if max_start_idx > 0 else 100
        else:
            pos_percent = 100
        self._is_updating_slider = True
        self.position_scale.set(pos_percent)
        self._is_updating_slider = False
        start = self.current_start_index
        end = min(total_points, start + self.points_per_view)
        return start, end

    def _prepare_plot_data(self, data_slice, start_index):
        unit = self.unit_selector.get()
        conversion_factor = self.CONVERSION_FACTORS.get(unit, 1.0)
        indices = range(start_index, start_index + len(data_slice))
        values = [item[1] * conversion_factor for item in data_slice]
        times = [item[0] for item in data_slice]
        warning_thresh = self.backend.warning_threshold * conversion_factor
        critical_thresh = self.backend.critical_threshold * conversion_factor
        return indices, values, times, unit, warning_thresh, critical_thresh

    def _setup_plot_style(self, unit='Giá trị'):
        self.ax.set_title('Dữ liệu Cảm biến Theo Thời Gian', fontsize=14, fontweight='bold')
        self.ax.set_xlabel('Thời gian', fontsize=12)
        self.ax.set_ylabel(f'Giá trị ({unit})', fontsize=12)
        self.ax.grid(True, which='major', linestyle='--', alpha=0.6)

    def _draw_plot_elements(self, indices, values, warning_thresh, critical_thresh, unit):
        safe_indices, warn_indices, crit_indices = [], [], []
        safe_values, warn_values, crit_values = [], [], []
        for i, val in zip(indices, values):
            if val >= critical_thresh:
                crit_indices.append(i)
                crit_values.append(val)
            elif val >= warning_thresh:
                warn_indices.append(i)
                warn_values.append(val)
            else:
                safe_indices.append(i)
                safe_values.append(val)
        self.ax.plot(indices, values, color='gray', linestyle='-', linewidth=1, alpha=0.5, zorder=3)
        self.ax.scatter(safe_indices, safe_values, color='green', s=40, label='An toàn', zorder=5)
        self.ax.scatter(warn_indices, warn_values, color='orange', s=40, label='Cảnh báo', zorder=5)
        self.ax.scatter(crit_indices, crit_values, color='red', s=40, label='Nguy hiểm', zorder=5)
        self.ax.axhline(y=warning_thresh, color='gold', linestyle='--', linewidth=2, alpha=0.9, label=f'Ngưỡng Cảnh báo ({warning_thresh:.2f} {unit})')
        self.ax.axhline(y=critical_thresh, color='darkorange', linestyle='--', linewidth=2, alpha=0.9, label=f'Ngưỡng Nguy hiểm ({critical_thresh:.2f} {unit})')

    def _configure_plot_axes(self, start, end, total_points, indices, times):
        self.ax.set_xlim(left=start - 0.5, right=start + self.points_per_view - 0.5)
        num_ticks = min(len(indices), 8)
        if num_ticks > 1:
            tick_indices_in_slice = np.linspace(0, len(indices) - 1, num_ticks, dtype=int)
            self.ax.set_xticks([indices[i] for i in tick_indices_in_slice])
            self.ax.set_xticklabels([times[i].strftime('%H:%M:%S') for i in tick_indices_in_slice], rotation=45, ha='right')
        elif len(indices) == 1:
            self.ax.set_xticks(indices); self.ax.set_xticklabels([t.strftime('%H:%M:%S') for t in times])
        handles, labels = self.ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        self.ax.legend(by_label.values(), by_label.keys(), loc='upper left')
        self.info_label.config(text=f"Tổng điểm: {total_points} | Hiển thị: {start+1}-{end}")
        try:
            self.fig.tight_layout()
        except (RecursionError, RuntimeError):
            print("Cảnh báo: Lỗi tạm thời khi tính toán layout biểu đồ.")

    def on_auto_follow_toggle(self):
        if self.auto_follow_var.get():
            total_points = len(self.backend.plot_data_points)
            if total_points > self.points_per_view:
                self.current_start_index = max(0, total_points - self.points_per_view)
            else:
                self.current_start_index = 0
            self.update_plot()

    def on_slider_change(self, value_str):
        if self._is_updating_slider: return
        if self._slider_after_id:
            self.root.after_cancel(self._slider_after_id)
        self._slider_after_id = self.root.after(100, lambda v=value_str: self._perform_slider_update(v))

    def _perform_slider_update(self, value_str):
        self._slider_after_id = None
        if self.auto_follow_var.get():
            self.auto_follow_var.set(False)
        total_points = len(self.backend.plot_data_points)
        if total_points <= self.points_per_view:
            return
        max_start_index = total_points - self.points_per_view
        self.current_start_index = int((float(value_str) / 100) * max_start_index)
        self.update_plot()

    def on_chart_close(self):
        if self._slider_after_id:
            self.root.after_cancel(self._slider_after_id)
            self._slider_after_id = None
        plt.close(self.fig)
        self.chart_window.destroy()
        self.chart_window = None

# ==============================================================================
# KHỐI ĐIỀU KHIỂN CHÍNH (MAIN CONTROLLER)
# ==============================================================================
class MainController:
    def __init__(self, backend, command_queue):
        self.backend = backend
        self.command_queue = command_queue
        self.app_instance = None
        self.root = ttk.Window()
        self.root.withdraw()

    def run(self):
        self.check_for_commands()
        self.root.mainloop()

    def check_for_commands(self):
        try:
            command = self.command_queue.get_nowait()
            if command == 'show': self.create_gui_window()
            elif command == 'exit': self.handle_shutdown()
            elif command == 'restart': self.handle_restart()
        except queue.Empty: pass
        finally:
            if not self.backend.exiting and self.root.winfo_exists():
                self.root.after(100, self.check_for_commands)

    def create_gui_window(self):
        if self.app_instance and self.app_instance.root.winfo_exists():
            print("Giao diện đã đang chạy."); self.app_instance.root.lift();
            return
        print("Đang khởi động giao diện người dùng...")
        toplevel_window = tk.Toplevel(self.root)
        self.app_instance = AppGUI(toplevel_window, self.backend, self.on_gui_close)

    def on_gui_close(self, shutdown=False):
        self.app_instance = None
        if shutdown: self.command_queue.put('exit')

    def handle_shutdown(self, silent=False):
        if not silent: print(" -> Nhận lệnh thoát...")
        if self.app_instance: self.app_instance.destroy_all_windows(); self.app_instance = None
        self.backend.shutdown(silent=silent)
        if self.root.winfo_exists(): self.root.destroy()

    def handle_restart(self):
        print(" -> Nhận lệnh khởi động lại...")
        global needs_restart; needs_restart = True
        self.backend.save_session_data(silent=True)
        self.handle_shutdown(silent=True)

# ==============================================================================
# KHỐI THỰC THI CHÍNH (MAIN)
# ==============================================================================
needs_restart = False
command_queue = queue.Queue()
def console_input_listener(cmd_queue: queue.Queue):
    while True:
        try:
            command = input().strip().lower()
            if command: cmd_queue.put(command)
            if command in ['exit', 'restart']: break
        except (EOFError, KeyboardInterrupt):
            cmd_queue.put('exit'); break

def signal_handler(signum, frame):
    print("\nNhận tín hiệu ngắt (Ctrl+C), đang thoát...")
    if not command_queue.empty():
        try: command_queue.get_nowait()
        except queue.Empty: pass
    command_queue.put('exit')

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    backend_instance = Backend()
    backend_instance.start_background_tasks()
    console_thread = threading.Thread(target=console_input_listener, args=(command_queue,), daemon=True)
    console_thread.start()
    print("Chương trình đã sẵn sàng.")
    print("Gõ 'show' để mở giao diện, 'exit' để thoát, 'restart' để khởi động lại.")
    main_controller = MainController(backend_instance, command_queue)
    command_queue.put('show')
    main_controller.run()
    if needs_restart:
        print("\n" + "="*50); print("KHỞI ĐỘNG LẠI CHƯƠNG TRÌNH..."); print("="*50 + "\n")
        try: os.execv(sys.executable, ['python'] + sys.argv)
        except Exception as e: print(f"LỖI KHÔNG THỂ KHỞI ĐỘNG LẠI: {e}")
    else: print("Chương trình đã kết thúc.")
