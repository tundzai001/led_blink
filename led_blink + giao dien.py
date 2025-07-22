import tkinter as tk
from tkinter import TclError, messagebox, filedialog
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tksheet import Sheet
import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
try:
    import RPi.GPIO as GPIO
    IS_PI = True
except (ImportError, RuntimeError):
    IS_PI = False
    print("CẢNH BÁO: Không thể import RPi.GPIO. Chạy ở chế độ mô phỏng GPIO.")
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
import matplotlib.dates as mdates
import numpy as np
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore", category=DeprecationWarning)

class MockGPIO:
    BCM = "BCM_MODE"; OUT = "OUT_MODE"; LOW = 0; HIGH = 1
    def setmode(self, mode): print(f"[GPIO Mock] Set mode to {mode}")
    def setwarnings(self, state): print(f"[GPIO Mock] Set warnings to {state}")
    def setup(self, pin, mode): print(f"[GPIO Mock] Setup pin {pin} to mode {mode}")
    def output(self, pin, state): print(f"[GPIO Mock] Set pin {pin} to state {'HIGH' if state else 'LOW'}")
    def input(self, pin): return self.LOW
    def cleanup(self): print("[GPIO Mock] GPIO cleanup called.")

if not IS_PI: GPIO = MockGPIO()

# ==============================================================================
# LỚP LOGIC NỀN (BACKEND) - Không thay đổi
# ==============================================================================
class Backend:
    def __init__(self):
        self.listening = False; self.exiting = False
        self.status_text = "Trạng thái: THỦ CÔNG"; self.status_color = "red"
        self.config = configparser.ConfigParser(); self.broker = "aitogy.xyz"; self.port = 1883
        self.username = "abc"; self.password = "xyz"; self.publish_topic = ""
        self.subscribe_topics = []; self.threshold = 1.1
        self.led1_pin = 3; self.led2_pin = 27
        self.session_file = "session.json"; self.sensor_data = []
        self.plot_data_points = deque(maxlen=10000)
        self.new_data_lock = threading.Lock()
        self.new_data_queue = []; self.client = mqtt.Client(protocol=mqtt.MQTTv311)
        self.client.on_connect = self.on_connect; self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message; self.stop_event = threading.Event()
        self.setup_gpio(); self.load_config()

    def setup_gpio(self):
        try:
            GPIO.setmode(GPIO.BCM); GPIO.setwarnings(False)
            GPIO.setup(self.led1_pin, GPIO.OUT); GPIO.setup(self.led2_pin, GPIO.OUT)
            GPIO.output(self.led1_pin, GPIO.LOW); GPIO.output(self.led2_pin, GPIO.LOW)
            print("GPIO setup successful.")
        except Exception as e: print(f"Error setting up GPIO. Error: {e}")

    def start_background_tasks(self):
        self.load_session_data()
        threading.Thread(target=self.auto_clear_scheduler, daemon=True).start()
        print("Đã khởi chạy các tác vụ nền.")

    def flash_led(self, pin, duration=0.3):
        try: GPIO.output(pin, GPIO.HIGH); time.sleep(duration); GPIO.output(pin, GPIO.LOW)
        except Exception as e: print(f"Error flashing LED on pin {pin}: {e}")

    def on_connect(self, client, userdata, flags, rc):
        if self.exiting: return
        if rc == 0:
            print("MQTT Connected successfully."); self.status_text = "Trạng thái: TỰ ĐỘNG"; self.status_color = "green"
            for t in self.subscribe_topics: client.subscribe(t); print(f"Subscribed: {t}")
            if not self.subscribe_topics: self.status_text = "Trạng thái: TỰ ĐỘNG (Không có topic)"
        else:
            print(f"Failed to connect, return code {rc}"); self.status_text = "Trạng thái: LỖI KẾT NỐI"
            self.status_color = "red"; self.listening = False

    def on_disconnect(self, client, userdata, rc):
        if not self.exiting and self.listening:
            print("Mất kết nối MQTT..."); self.status_text = "Trạng thái: MẤT KẾT NỐI"; self.status_color = "orange"

    def on_message(self, client, userdata, msg):
        if not self.listening or self.exiting: return
        try:
            data = json.loads(msg.payload.decode()); name = data.get("sensorname", msg.topic)
            value = float(data.get("value", 0)); ts = float(data.get("timestamp", time.time()))
            dt_object = datetime.fromtimestamp(ts); self.plot_data_points.append((dt_object, value))
            status = "VUOT MUC" if value > self.threshold else "AN TOAN"
            record = (name, f"{value:.2f}", status, dt_object.strftime("%H:%M:%S %d-%m"))
            self.sensor_data.append(record)
            with self.new_data_lock: self.new_data_queue.append(record)
            threading.Thread(target=self.flash_led, args=(self.led1_pin,), daemon=True).start()
            if value > self.threshold: threading.Thread(target=self.flash_led, args=(self.led2_pin,), daemon=True).start()
            if self.publish_topic: self.client.publish(self.publish_topic, f"({value:.2f}, {status}, {int(ts)})")
        except Exception as e: print(f"Lỗi trong on_message: {e}")

    def get_new_data(self):
        with self.new_data_lock: data = list(self.new_data_queue); self.new_data_queue.clear(); return data

    def toggle_on(self):
        if self.listening: return True
        self.listening = True; self.status_text = "Trạng thái: ĐANG KẾT NỐI..."; self.status_color = "orange"
        if not self.broker:
            self.listening = False; self.status_text = "Trạng thái: THỦ CÔNG (Lỗi Broker)"; self.status_color = "red"; return True
        self.client.username_pw_set(self.username, self.password)
        try:
            print(f"Đang kết nối tới MQTT broker: {self.broker}:{self.port}...")
            self.client.connect_async(self.broker, self.port, 60); self.client.loop_start()
        except Exception as e:
            self.listening = False; self.status_text = "Trạng thái: LỖI KẾT NỐI"; self.status_color = "red"; print(f"Lỗi kết nối MQTT: {e}")
        return True

    def toggle_off(self):
        if not self.listening: return
        self.listening = False
        try: self.client.loop_stop(); self.client.disconnect(); print("Đã ngắt kết nối MQTT.")
        except Exception: pass
        self.status_text = "Trạng thái: THỦ CÔNG"; self.status_color = "red"

    def update_and_reconnect(self, settings):
        self.broker = settings['broker']; self.port = int(settings['port']); self.username = settings['username']
        self.password = settings['password']; self.publish_topic = settings['publish']
        self.subscribe_topics = [t for t in settings['topics'].splitlines() if t]; self.threshold = float(settings['threshold'])
        self.save_config()
        if self.listening:
            print("Đang ở chế độ tự động, áp dụng cấu hình MQTT mới..."); self.toggle_off(); self.toggle_on()

    def check_leds(self):
        if self.listening:
            print("Không thể kiểm tra LED ở chế độ TỰ ĐỘNG."); return False
        def run_check():
            print("Kiểm tra LED...")
            try: self.flash_led(self.led1_pin, duration=0.5); time.sleep(0.1); self.flash_led(self.led2_pin, duration=0.5)
            except Exception as e: print(f"Lỗi khi kiểm tra LED: {e}")
        threading.Thread(target=run_check, daemon=True).start()
        return True

    def auto_clear_scheduler(self):
        while not self.stop_event.is_set():
            now = datetime.now()
            if now.hour == 0 and now.minute == 0: print("Đã đến 00:00, tự động xóa dữ liệu..."); self.clear_all_data(); time.sleep(61)
            else: time.sleep(30)

    def clear_all_data(self):
        self.sensor_data.clear(); self.plot_data_points.clear()
        with self.new_data_lock: self.new_data_queue.clear(); self.new_data_queue.append("CLEAR")
        print("Đã xóa dữ liệu nền.")

    def load_config(self):
        if not os.path.exists('config.ini'): return
        try:
            config = configparser.ConfigParser(); config.read('config.ini')
            if "MQTT" in config:
                mqtt_cfg = config["MQTT"]; self.broker = mqtt_cfg.get("broker", self.broker); self.port = mqtt_cfg.getint("port", self.port)
                self.username = mqtt_cfg.get("username", self.username); self.password = mqtt_cfg.get("password", self.password)
                self.subscribe_topics = [t for t in mqtt_cfg.get("topics", "").splitlines() if t]; self.publish_topic = mqtt_cfg.get("publish", self.publish_topic)
            if "Settings" in config: self.threshold = config["Settings"].getfloat("threshold", self.threshold)
            print("Đã tải cấu hình.")
        except Exception as e: print(f"Lỗi khi tải cấu hình từ config.ini: {e}")

    def save_config(self):
        config = configparser.ConfigParser()
        config['MQTT'] = {'broker': self.broker, 'port': self.port, 'username': self.username, 'password': self.password, 'topics': "\n".join(self.subscribe_topics), 'publish': self.publish_topic}
        config['Settings'] = {'threshold': self.threshold}
        try:
            with open('config.ini', 'w') as f: config.write(f)
            print("Đã lưu cấu hình."); return True
        except Exception as e: print(f"Lỗi Lưu File: {e}"); return False

    def save_session_data(self, silent=False):
        if not silent: print(" -> Đang lưu trạng thái hiện tại vào file...")
        try:
            plot_data_list = list(self.plot_data_points)
            plot_data_serializable = [(dt.isoformat(), val) for dt, val in plot_data_list]
            session = {"sensor_data": self.sensor_data, "plot_data_points": plot_data_serializable}
            with open(self.session_file, "w") as f: json.dump(session, f)
            if not silent: print(f" -> Đã lưu trạng thái vào {self.session_file}")
        except Exception as e: print(f" -> Lỗi khi lưu trạng thái: {e}")

    def load_session_data(self):
        if os.path.exists(self.session_file):
            print(f" -> Tìm thấy file trạng thái {self.session_file}, đang tải lại dữ liệu...")
            try:
                with open(self.session_file, "r") as f: session = json.load(f)
                self.sensor_data = session.get("sensor_data", [])
                plot_data_serializable = session.get("plot_data_points", [])
                self.plot_data_points.clear()
                for dt_str, val in plot_data_serializable:
                    self.plot_data_points.append((datetime.fromisoformat(dt_str), val))
                with self.new_data_lock: self.new_data_queue.extend(self.sensor_data)
                print(" -> Đã tải lại dữ liệu thành công.")
            except Exception as e: print(f" -> Lỗi khi tải trạng thái: {e}")
            finally:
                if os.path.exists(self.session_file): os.remove(self.session_file)

    def shutdown(self, silent=False):
        if self.exiting: return
        if not silent: print("\nBắt đầu quá trình dọn dẹp để thoát/khởi động lại...")
        self.exiting = True; self.stop_event.set()
        try: self.client.loop_stop(force=False); self.client.disconnect()
        except Exception: pass
        try: GPIO.cleanup()
        except Exception: pass
        if not silent: print(" -> Backend đã dừng.")

# ==============================================================================
# LỚP GIAO DIỆN NGƯỜI DÙNG (GUI) - Phiên bản cuối cùng
# ==============================================================================
class AppGUI:
    def __init__(self, root, backend, on_close_callback):
        self.root = root; self.backend = backend; self.on_close_callback = on_close_callback
        self.root.title("Giao diện Cảm biến & Điều khiển LED")
        self.root.geometry(f"{self.root.winfo_screenwidth()}x{self.root.winfo_screenheight()-70}+0+0")
        self.chart_window = None; self.CONVERSION_FACTORS = {"m": 1.0, "cm": 100.0, "mm": 1000.0, "ft": 3.28084}
        
        # Biến quản lý trạng thái biểu đồ
        self.points_per_view = 40  # Số điểm hiển thị trên màn hình
        self.current_start_index = 0 # Chỉ số bắt đầu của "cửa sổ" xem
        
        self.create_widgets(); self.load_initial_data()
        self.root.after(500, self.periodic_update)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close_window)

    def destroy_all_windows(self):
        if self.chart_window and self.chart_window.winfo_exists(): self.chart_window.destroy()
        if self.root and self.root.winfo_exists(): self.root.destroy()

    def on_close_window(self):
        print("Đã đóng cửa sổ giao diện. Tiến trình nền vẫn đang chạy."); print("Gõ 'show' trong terminal để mở lại.")
        self.on_close_callback(); self.destroy_all_windows()

    def exit_program_graceful(self):
        if messagebox.askokcancel("Xác nhận", "Bạn có chắc muốn thoát hoàn toàn chương trình?", parent=self.root):
            self.on_close_callback(shutdown=True)

    def create_widgets(self):
        main = ttk.Frame(self.root); main.pack(fill="both", expand=True, padx=10, pady=10)
        main.grid_columnconfigure(1, weight=1); main.grid_rowconfigure(0, weight=1)
        self.create_left_panel(main); self.create_right_panel(main)

    def create_left_panel(self, parent):
        left = ttk.LabelFrame(parent, text="Cài đặt MQTT & Ngưỡng")
        left.grid(row=0, column=0, sticky="nsw", padx=(0, 15), pady=10); left.grid_rowconfigure(8, weight=1)
        def add_labeled_entry(frame, label, row, width=14, show=None):
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w")
            entry = ttk.Entry(frame, width=width, show=show); entry.grid(row=row, column=1, sticky="ew", pady=2)
            frame.grid_columnconfigure(1, weight=1); return entry
        self.broker_entry = add_labeled_entry(left, "MQTT Broker:", 0); self.port_entry = add_labeled_entry(left, "Port:", 1)
        self.user_entry = add_labeled_entry(left, "Username:", 2); self.pass_entry = add_labeled_entry(left, "Password:", 3, show="*")
        self.pub_entry = add_labeled_entry(left, "Publish Topic:", 4); self.threshold_entry = add_labeled_entry(left, "Ngưỡng (m):", 5)
        show_btn = ttk.Button(left, text="👁", command=self.toggle_pass, width=2); show_btn.grid(row=3, column=2, sticky="w")
        ttk.Label(left, text="Subscribe Topics:").grid(row=7, column=0, columnspan=2, sticky="w")
        self.topic_input = tk.Text(left, width=22, height=6); self.topic_input.grid(row=8, column=0, columnspan=3, pady=(0, 5), sticky="nsew")
        ttk.Button(left, text="Lưu & Áp dụng", command=self.apply_and_save_config, bootstyle="primary").grid(row=9, column=0, columnspan=3, sticky="ew", pady=5)

    def create_right_panel(self, parent):
        right = ttk.Frame(parent); right.grid(row=0, column=1, sticky="nsew")
        bottom_part = ttk.Frame(right); bottom_part.pack(side=tk.BOTTOM, fill=tk.X, expand=False, pady=(10, 0))
        self.create_control_panel(bottom_part)
        top_part = ttk.Frame(right); top_part.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.status_label = ttk.Label(top_part, text="", foreground="red", font=("Arial", 11, "bold")); self.status_label.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
        sheet_frame = ttk.Frame(top_part); sheet_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.sheet = Sheet(sheet_frame, headers=["Tên", "Giá trị (m)", "Trạng thái", "Thời gian"], show_row_index=False, column_widths=[150, 100, 100, 150])
        self.sheet.enable_bindings(); self.sheet.set_options(font=("Arial", 10, "normal"), align="w", header_font=("Arial", 10, "bold"))
        self.sheet.disable_bindings(["edit_cell", "arrowkeys"]); self.sheet.pack(fill=tk.BOTH, expand=True)

    def create_control_panel(self, parent_frame):
        ctrl = ttk.Frame(parent_frame); ctrl.pack(side=tk.TOP, fill=tk.X, expand=True)
        for i in range(5): ctrl.grid_columnconfigure(i, weight=1)
        ttk.Button(ctrl, text="Tự động (ON)", command=self.backend.toggle_on, bootstyle="success").grid(row=0, column=0, padx=2, sticky="ew")
        ttk.Button(ctrl, text="Thủ công (OFF)", command=self.backend.toggle_off, bootstyle="danger").grid(row=0, column=1, padx=2, sticky="ew")
        self.save_csv_button = ttk.Button(ctrl, text="Lưu CSV", command=self.save_to_csv, bootstyle="info"); self.save_csv_button.grid(row=0, column=2, padx=2, sticky="ew")
        ttk.Button(ctrl, text="Xóa Dữ Liệu", command=self.clear_table_gui, bootstyle="warning").grid(row=0, column=3, padx=2, sticky="ew")
        ttk.Button(ctrl, text="Xem Biểu Đồ", command=self.show_chart_window, bootstyle="primary").grid(row=0, column=4, padx=2, sticky="ew")
        led_panel = ttk.LabelFrame(parent_frame, text="Kiểm tra Thiết bị"); led_panel.pack(side=tk.TOP, fill=tk.X, expand=True, pady=(5, 0))
        led_panel.grid_columnconfigure(0, weight=3); led_panel.grid_columnconfigure(1, weight=1)
        ttk.Button(led_panel, text="Kiểm tra LED", command=self.on_check_led_click).grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        ttk.Button(led_panel, text="Thoát", command=self.exit_program_graceful, bootstyle="secondary").grid(row=0, column=1, padx=5, pady=5, sticky="ew")

    def on_check_led_click(self):
        success = self.backend.check_leds()
        if not success: messagebox.showwarning("Cảnh báo", "Chỉ có thể kiểm tra LED ở chế độ THỦ CÔNG (OFF).", parent=self.root)

    def load_initial_data(self):
        self.broker_entry.delete(0, tk.END); self.port_entry.delete(0, tk.END); self.user_entry.delete(0, tk.END)
        self.pass_entry.delete(0, tk.END); self.pub_entry.delete(0, tk.END); self.threshold_entry.delete(0, tk.END)
        self.topic_input.delete("1.0", tk.END); self.broker_entry.insert(0, self.backend.broker)
        self.port_entry.insert(0, str(self.backend.port)); self.user_entry.insert(0, self.backend.username)
        self.pass_entry.insert(0, self.backend.password); self.pub_entry.insert(0, self.backend.publish_topic)
        self.threshold_entry.insert(0, str(self.backend.threshold)); self.topic_input.insert("1.0", "\n".join(self.backend.subscribe_topics))
        initial_data = self.backend.get_new_data()
        if initial_data:
            valid_data = [d for d in initial_data if isinstance(d, tuple)]
            self.sheet.set_sheet_data(valid_data)
        else: self.sheet.set_sheet_data([])
        self.update_status_label()

    def periodic_update(self):
        if self.backend.exiting or not self.root.winfo_exists(): return
        self.update_status_label()
        new_records = self.backend.get_new_data()
        if new_records:
            if any(record == "CLEAR" for record in new_records):
                self.sheet.set_sheet_data([])
                if self.chart_window and self.chart_window.winfo_exists(): self.clear_chart_data()
                print("GUI đã nhận tín hiệu và xóa bảng.")
            else:
                for record in new_records:
                    if isinstance(record, tuple): self.add_row_to_table(record)
            if self.chart_window and self.chart_window.winfo_exists(): self.update_plot()
        if self.root.winfo_exists(): self.root.after(500, self.periodic_update)

    def update_status_label(self):
        if self.status_label.cget("text") != self.backend.status_text or self.status_label.cget("foreground") != self.backend.status_color:
            self.status_label.config(text=self.backend.status_text, foreground=self.backend.status_color)

    def toggle_pass(self): self.pass_entry.config(show="" if self.pass_entry.cget("show") else "*")

    def apply_and_save_config(self):
        settings = { 'broker': self.broker_entry.get(), 'port': self.port_entry.get(), 'username': self.user_entry.get(), 'password': self.pass_entry.get(), 'topics': self.topic_input.get("1.0", "end").strip(), 'publish': self.pub_entry.get(), 'threshold': self.threshold_entry.get() }
        try:
            int(settings['port']); float(settings['threshold']); self.backend.update_and_reconnect(settings)
            messagebox.showinfo("Thành công", "Đã lưu và áp dụng cấu hình.", parent=self.root)
        except ValueError as e: messagebox.showerror("Lỗi", f"Dữ liệu không hợp lệ: {e}", parent=self.root)
        except Exception as e: messagebox.showerror("Lỗi", f"Không thể áp dụng cấu hình: {e}", parent=self.root)

    def add_row_to_table(self, record):
        self.sheet.insert_row(list(record))
        self.sheet.deselect("all"); self.sheet.dehighlight_all()
        new_row_index = self.sheet.get_total_rows() - 1
        if new_row_index >= 0:
            self.sheet.highlight_rows([new_row_index], bg='#D2EAF8')
            self.sheet.see(row=new_row_index, column=0)

    def clear_chart_data(self):
        self.current_start_index = 0
        self.update_plot()

    def clear_table_gui(self):
        self.sheet.set_sheet_data([]); self.backend.clear_all_data()
        if self.chart_window and self.chart_window.winfo_exists(): self.clear_chart_data()
        print("Đã xóa dữ liệu từ giao diện.")

    def save_to_csv(self):
        if not self.backend.sensor_data: messagebox.showinfo("Thông báo", "Không có dữ liệu để lưu.", parent=self.root); return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")], parent=self.root)
        if path: self.save_csv_button.config(state="disabled"); threading.Thread(target=self._write_csv_in_background, args=(path, list(self.backend.sensor_data)), daemon=True).start()

    def _write_csv_in_background(self, path, data_to_save):
        try:
            import csv
            with open(path, "w", newline="", encoding='utf-8') as f:
                writer = csv.writer(f); writer.writerow(["Tên", "Giá trị (m)", "Trạng thái", "Thời gian"]); writer.writerows(data_to_save)
            if self.root.winfo_exists(): self.root.after(0, lambda: messagebox.showinfo("Thành công", f"Đã lưu dữ liệu vào {path}", parent=self.root))
        except Exception as e:
            if self.root.winfo_exists(): self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Không thể lưu file: {e}", parent=self.root))
        finally:
            if self.root.winfo_exists(): self.root.after(0, lambda: self.save_csv_button.config(state="normal"))

    # --- Các hàm điều khiển biểu đồ ---
    
    def show_chart_window(self):
        if self.chart_window is not None and self.chart_window.winfo_exists():
            self.chart_window.lift(); return
        
        self.chart_window = tk.Toplevel(self.root)
        self.chart_window.title("Biểu đồ Dữ liệu Cảm biến")
        self.chart_window.geometry("1200x800")
        self.chart_window.protocol("WM_DELETE_WINDOW", self.on_chart_close)
        
        # --- Khung điều khiển ---
        top_frame = ttk.Frame(self.chart_window)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=5)
        
        ttk.Label(top_frame, text="Chọn đơn vị:").pack(side=tk.LEFT, padx=(0, 5))
        self.unit_selector_combobox = ttk.Combobox(top_frame, state="readonly", values=list(self.CONVERSION_FACTORS.keys()))
        self.unit_selector_combobox.set("m")
        self.unit_selector_combobox.pack(side=tk.LEFT, padx=5)
        self.unit_selector_combobox.bind("<<ComboboxSelected>>", lambda e: self.update_plot())
        
        self.auto_follow_var = tk.BooleanVar(value=True)
        auto_follow_check = ttk.Checkbutton(top_frame, text="Tự động theo dõi", variable=self.auto_follow_var, command=self.on_auto_follow_toggle)
        auto_follow_check.pack(side=tk.LEFT, padx=20)
        
        # --- Khung biểu đồ ---
        chart_frame = ttk.Frame(self.chart_window)
        chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.fig = Figure(figsize=(12, 6), dpi=100, facecolor='white')
        self.ax = self.fig.add_subplot(111)
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.draw(); self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        # --- Thanh trượt ---
        slider_frame = ttk.Frame(self.chart_window)
        slider_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=5)
        
        self.info_label = ttk.Label(slider_frame, text="Tổng điểm: 0 | Hiển thị: 0-0", font=("Arial", 9))
        self.info_label.pack(side=tk.TOP)
        
        self.position_var = tk.DoubleVar()
        self.position_scale = ttk.Scale(slider_frame, from_=0, to=100, orient=tk.HORIZONTAL, variable=self.position_var, command=self.on_slider_change)
        self.position_scale.pack(side=tk.TOP, fill=tk.X, expand=True, pady=(5,0))
        
        self.setup_plot(); self.update_plot()

    def setup_plot(self):
        self.ax.clear()
        self.ax.set_title('Dữ liệu Cảm biến Theo Thời Gian', fontsize=14, fontweight='bold', pad=20)
        self.ax.set_xlabel('Thời gian', fontsize=12)
        self.ax.set_ylabel('Giá trị', fontsize=12)
        self.ax.grid(True, which='major', linestyle='--', alpha=0.6)

    def update_plot(self):
        if not hasattr(self, 'ax') or not self.chart_window.winfo_exists(): return
        
        all_data = list(self.backend.plot_data_points)
        total_points = len(all_data)

        self.update_slider_range(total_points)

        # Nếu đang ở chế độ tự động, luôn trượt về cuối
        if self.auto_follow_var.get():
            self.current_start_index = max(0, total_points - self.points_per_view)

        # Xác định "cửa sổ" dữ liệu cần hiển thị
        start = self.current_start_index
        end = min(total_points, start + self.points_per_view)
        display_data_slice = all_data[start:end]

        if not display_data_slice:
            self.ax.clear(); self.setup_plot()
            self.ax.text(0.5, 0.5, 'Chưa có dữ liệu', ha='center', va='center', transform=self.ax.transAxes, fontsize=16, alpha=0.5)
            self.info_label.config(text="Tổng điểm: 0 | Hiển thị: 0-0")
            self.canvas.draw(); return

        # Chuẩn bị dữ liệu để vẽ
        unit = self.unit_selector_combobox.get()
        conversion_factor = self.CONVERSION_FACTORS.get(unit, 1.0)
        
        # Trục X sẽ là chỉ số của dữ liệu trong toàn bộ dataset
        plot_indices = range(start, end)
        values = [item[1] * conversion_factor for item in display_data_slice]
        times = [item[0] for item in display_data_slice]
        
        self.ax.clear(); self.setup_plot()
        
        # --- Vẽ các thành phần biểu đồ ---
        threshold_value = self.backend.threshold * conversion_factor

        self.ax.plot(plot_indices, values, color='gray', linestyle='-', linewidth=1, alpha=0.5)
        
        safe_indices, safe_values, warn_indices, warn_values = [], [], [], []
        for i, value in zip(plot_indices, values):
            if value > threshold_value:
                warn_indices.append(i); warn_values.append(value)
            else:
                safe_indices.append(i); safe_values.append(value)
        
        self.ax.scatter(safe_indices, safe_values, color='green', s=50, label='An toàn', zorder=5)
        self.ax.scatter(warn_indices, warn_values, color='red', s=50, label='Vượt ngưỡng', zorder=5)

        ymin, _ = self.ax.get_ylim()
        self.ax.vlines(plot_indices, ymin=ymin, ymax=values, colors='gray', linestyles='dotted', lw=1, alpha=0.7)
        self.ax.axhline(y=threshold_value, color='red', linestyle='--', linewidth=2, alpha=0.7, label=f'Ngưỡng ({threshold_value:.2f} {unit})')
        
        # --- Cấu hình các trục và nhãn ---
        self.ax.set_ylabel(f'Giá trị ({unit})', fontsize=12)
        
        # Đặt các nhãn thời gian trên trục X
        num_ticks = min(len(display_data_slice), 7)
        if num_ticks > 1:
            tick_indices = np.linspace(0, len(display_data_slice) - 1, num_ticks, dtype=int)
            self.ax.set_xticks([plot_indices[i] for i in tick_indices])
            self.ax.set_xticklabels([times[i].strftime('%H:%M:%S') for i in tick_indices], rotation=45, ha='right')
        elif len(display_data_slice) == 1:
            self.ax.set_xticks(plot_indices)
            self.ax.set_xticklabels([t.strftime('%H:%M:%S') for t in times], rotation=45, ha='right')
        
        # Đặt giới hạn cho trục X để tạo hiệu ứng "cửa sổ trượt"
        self.ax.set_xlim(left=start - 0.5, right=start + self.points_per_view - 0.5)
        
        # --- Hoàn thiện ---
        handles, labels = self.ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        self.ax.legend(by_label.values(), by_label.keys(), loc='upper left', framealpha=0.9)
        self.info_label.config(text=f"Tổng điểm: {total_points} | Hiển thị: {start+1}-{end}")
        
        self.fig.tight_layout()
        self.canvas.draw()

    def on_auto_follow_toggle(self):
        if self.auto_follow_var.get():
            self.update_plot() # Tự động nhảy về cuối khi bật

    def update_slider_range(self, total_points):
        if total_points <= self.points_per_view:
            self.position_scale.config(state="disabled")
            self.position_var.set(100)
        else:
            self.position_scale.config(state="normal")
            max_start_index = total_points - self.points_per_view
            # Cập nhật vị trí thanh trượt mà không kích hoạt lệnh on_slider_change
            # để tránh vòng lặp vô hạn
            current_percentage = (self.current_start_index / max_start_index) * 100 if max_start_index > 0 else 100
            self.position_var.set(current_percentage)
            
    def on_slider_change(self, value_str):
        # Tắt chế độ tự động khi người dùng kéo thanh trượt
        if self.auto_follow_var.get():
            self.auto_follow_var.set(False)
        
        total_points = len(self.backend.plot_data_points)
        if total_points <= self.points_per_view:
            return
            
        max_start_index = total_points - self.points_per_view
        percentage = float(value_str)
        self.current_start_index = int((percentage / 100) * max_start_index)
        
        self.update_plot()

    def on_chart_close(self):
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
            if not self.backend.exiting: self.root.after(100, self.check_for_commands)

    def create_gui_window(self):
        if self.app_instance and self.app_instance.root.winfo_exists():
            print("Giao diện đã đang chạy rồi."); self.app_instance.root.lift(); return
        print("Đang khởi động giao diện người dùng...")
        toplevel_window = tk.Toplevel(self.root)
        self.app_instance = AppGUI(toplevel_window, self.backend, self.on_gui_close)

    def on_gui_close(self, shutdown=False):
        self.app_instance = None
        if shutdown: self.handle_shutdown()

    def handle_shutdown(self, silent=False):
        if not silent: print(" -> Nhận lệnh thoát...")
        if self.app_instance: self.app_instance.destroy_all_windows(); self.app_instance = None
        self.backend.shutdown(silent=silent)
        self.root.destroy()

    def handle_restart(self):
        print(" -> Nhận lệnh khởi động lại...")
        global needs_restart; needs_restart = True
        self.backend.save_session_data(silent=True)
        self.handle_shutdown(silent=True)

# ==============================================================================
# KHỐI THỰC THI CHÍNH
# ==============================================================================
needs_restart = False
command_queue = queue.Queue()

def console_input_listener(cmd_queue):
    while True:
        try:
            command = input().strip().lower()
            if command: cmd_queue.put(command)
            if command in ['exit', 'restart']: break
        except (EOFError, KeyboardInterrupt):
            cmd_queue.put('exit'); break

def signal_handler(signum, frame):
    print("\nNhận tín hiệu ngắt (Ctrl+C), đang thoát...")
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
