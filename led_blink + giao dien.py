import ttkbootstrap as ttk
import tkinter as tk
from ttkbootstrap.constants import *
from ttkbootstrap import Style
from ttkbootstrap.dialogs import Messagebox
from tkinter import filedialog, messagebox
from tksheet import Sheet
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import csv
import RPi.GPIO as GPIO
import threading
import time
import warnings
import configparser
import signal

warnings.filterwarnings("ignore", category=DeprecationWarning)

# GPIO setup
LED1_PIN = 3
LED2_PIN = 27
try:
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(LED1_PIN, GPIO.OUT)
    GPIO.setup(LED2_PIN, GPIO.OUT)
    print("GPIO setup successful.")
except Exception as e:
    print(f"Error setting up GPIO. Are you running on a Raspberry Pi with permissions? Error: {e}")

SHUTDOWN_REQUESTED = False
threshold = 1.3
sensor_data = []
listening = False
blink_mode = False # Thêm biến toàn cục để điều khiển chế độ blink
stop_event = threading.Event()
current_topics = []

# ==== Flash LED ====
def flash_led(pin, duration=0.3):
    GPIO.setwarnings(False)
    try:
        GPIO.output(pin, GPIO.HIGH)
        time.sleep(duration)
        GPIO.output(pin, GPIO.LOW)
    except Exception as e:
        print(f"Error flashing LED on pin {pin}: {e}")

# ==== GUI ====
# ... (Phần mã GUI của bạn không thay đổi, giữ nguyên) ...
root = ttk.Window(themename="flatly")
root.title("Giao diện Cảm biến & Điều khiển LED")
root.geometry(f"{root.winfo_screenwidth()}x{root.winfo_screenheight()}")
main = ttk.Frame(root)
main.pack(fill="both", expand=True, padx=10, pady=10)
main.grid_columnconfigure(1, weight=1)
main.grid_rowconfigure (0, weight=1)

style = ttk.Style()
style.configure("Treeview.Heading", font=("Arial", 10, "bold"))
style.configure("Treeview", rowheight=24, font=("Arial", 10))

# ==== LEFT PANEL ====
left = ttk.LabelFrame(main, text="Cài đặt MQTT & Ngưỡng")
left.grid(row=0, column=0, sticky="nsw", padx=(0, 15), pady=10)
left.grid_rowconfigure(8, weight=1) 
def add_labeled_entry(frame, label, row, default="", width=14, show=None):
    ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w")
    entry = ttk.Entry(frame, width=width, show=show)
    entry.insert(0, default)
    entry.grid(row=row, column=1, sticky="ew", pady=2)
    frame.grid_columnconfigure(1, weight=1)
    return entry

broker_entry = add_labeled_entry(left, "MQTT Broker:", 0, "aitogy.xyz")
port_entry = add_labeled_entry(left, "Port:", 1, "1883")
user_entry = add_labeled_entry(left, "Username:", 2, "abc")
pass_entry = add_labeled_entry(left, "Password:", 3, "xyz", show="*")
pub_entry   = add_labeled_entry(left, "Publish Topic:", 4, "",)
threshold_entry = add_labeled_entry(left, "Ngưỡng cảnh báo:", 5, str(threshold))

def save_config_apply(silent=False):
    config = configparser.ConfigParser()
    config['MQTT'] = {
        'broker': broker_entry.get(),
        'port': port_entry.get(),
        'username': user_entry.get(),
        'password': pass_entry.get(),
        'topics': topic_input.get("1.0", "end").strip(),
        'publish': pub_entry.get()
    }
    config['Settings'] = {
        'threshold': threshold_entry.get()
    }
    with open('config.ini', 'w') as f:
        config.write(f)
    if not silent:
        messagebox.showinfo("Lưu cấu hình", "Đã lưu cấu hình thành công")
    if listening: update_mqtt()

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    if "MQTT" in config:
        mqtt = config["MQTT"]
        broker_entry.delete(0, tk.END)
        broker_entry.insert(0, mqtt.get("broker", ""))

        port_entry.delete(0, tk.END)
        port_entry.insert(0, mqtt.get("port", "1883"))

        user_entry.delete(0, tk.END)
        user_entry.insert(0, mqtt.get("username", ""))

        pass_entry.delete(0, tk.END)
        pass_entry.insert(0, mqtt.get("password", ""))

        topic_input.delete("1.0", tk.END)
        topic_input.insert("1.0", mqtt.get("topics", ""))
        pub_entry.delete(0, tk.END)
        pub_entry.insert(0, mqtt.get("publish", "led_data"))
    if "Settings" in config and "threshold" in config["Settings"]:
        threshold_entry.delete(0, tk.END)
        threshold_entry.insert(0, config["Settings"]["threshold"])
    print("Đã tải cấu hình.")

def load_threshold():
    config = configparser.ConfigParser()
    config.read('config.ini')
    if "Settings" in config and 'threshold' in config['Settings']: 
        threshold_entry.delete(0, tk.END)
        threshold_entry.insert(0, config['Settings']['threshold'])
def toggle_pass():
    if pass_entry.cget("show") == "":
        pass_entry.config(show="*")
        show_btn.config(text="👁")
    else:
        pass_entry.config(show="")
        show_btn.config(text="🙈")
show_btn = ttk.Button(left, text="👁", command=toggle_pass, width=2)
show_btn.grid(row=3, column=2, sticky="w")
ttk.Label(left, text="Subscribe Topics (1 dòng mỗi topic):").grid(row=7, column=0, columnspan=2, sticky="w")
topic_input = tk.Text(left, width=22, height=6)
topic_input.grid(row=8, column=0, columnspan=3, pady=(0,5), sticky="nsew")
ttk.Button(left, text="Lưu cấu hình", command=save_config_apply, bootstyle="primary").grid(row=9, column=0, columnspan=3, sticky="ew", pady=5)
# ==== SỬA ĐỔI CHÍNH Ở ĐÂY ====
exiting = False
def on_connect(client, userdata, flags, rc):
    root.after(0, _on_connect_gui, rc)

def _on_connect_gui(rc):
    global current_topics
    if rc == 0:
        print("MQTT Connected successfully.")
        global current_topics
        topics_to_subscribe = topic_input.get("1.0", "end").strip().splitlines()
        if not topics_to_subscribe:
            print("No topics to subscribe to.")
            if listening:
                status_label.config(text="Trạng thái: TỰ ĐỘNG(Không có topic)", foreground="green")
                return
        if current_topics:
            for t in current_topics:
                client.unsubscribe(t)
        
        current_topics = topics_to_subscribe
        for t in current_topics:
            if t:
                client.subscribe(t)
                print(f"Subscribed to topic: {t}")
        if listening:
            status_label.config(text="Trạng thái: TỰ ĐỘNG", foreground="green")
    else:
        print(f"Failed to connect, return code {rc}\n")
        status_label.config(text="Trạng thái: LỖI KẾT NỐI", foreground="red")
        if listening:
            messagebox.showerror("MQTT Error", f"Không thể kết nối MQTT, mã lỗi: {rc}")

def update_mqtt():
    global exiting
    if exiting:
        return
    try:
        client.loop_stop()
        client.disconnect()
        client.loop_start() 
    except Exception as e:
        messagebox.showerror("Lỗi kết nối MQTT", f"Không thể kết nối: {e}"); 
        status_label.config(text="Trạng thái: MẤT KẾT NỐI", foreground="red")
        threading.Thread(target=_attempt_reconnect, args=(client,), daemon=True).start()

    broker = broker_entry.get().strip()
    port_text = port_entry.get().strip()
    pwd = pass_entry.get().strip()
    user = user_entry.get().strip()
    
    if listening: 
        status_label.config(text="Trạng thái: ĐANG KẾT NỐI...", foreground="orange")

    if not broker:
        messagebox.showerror("MQTT Error", "Vui lòng nhập địa chỉ MQTT Broker")
        toggle_off()
        return
    try:
        port = int(port_text)
    except ValueError:
        messagebox.showerror("Lỗi cấu hình", "Port phải là số nguyên.");
        toggle_off()
        return

    client.username_pw_set(user, pwd)
    try:
        print(f"Đang kết nối tới MQTT broker: {broker}:{port}...")
        client.connect(broker, port=port, keepalive=60)
        client.loop_start() # Bắt đầu vòng lặp mới
    except Exception as e:
        messagebox.showerror("Lỗi kết nối MQTT", f"Không thể kết nối: {e}"); 
        status_label.config(text="Trạng thái: MẤT KẾT NỐI", foreground="red")
        threading.Thread(target=_attempt_reconnect, args=(client,), daemon=True).start()

# ==== RIGHT PANEL ====
right = ttk.Frame(main, borderwidth=1, relief="solid")
right.grid(row=0, column=1, sticky="nsew")
right.grid_columnconfigure(0, weight=1)
right.grid_rowconfigure(1, weight=1)

status_label = ttk.Label(right, text="Trạng thái: THỦ CÔNG", foreground="red", font=("Arial", 11, "bold"))
status_label.grid(row=0, column=0, pady=5)

sheet_frame = ttk.Frame(right)
sheet_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
sheet_frame.grid_columnconfigure(0, weight=1)
sheet_frame.grid_rowconfigure(0, weight=1)
sheet = Sheet(sheet_frame,
              headers=["Tên", "Giá trị", "Trạng thái", "Thời gian"],
              show_row_index=False,
              column_widths=[150, 100, 100, 150],
)
sheet.enable_bindings()
sheet.set_options(
    font=("Arial", 10, "normal"),
    align="w",  # canh trái
    header_font=("Arial", 10, "bold"),
    table_bg="#ffffff",
    grid_color="#cccccc"
)
sheet.disable_bindings(["edit_cell", "arrowkeys", "drag_and_drop", "column_drag_and_drop", "rc_delete_row", "rc_insert_row", "rc_delete_column", "rc_insert_column"]) # Không chỉnh sửa
sheet.set_options(grid_color="#cccccc", table_bg="#ffffff", index_bg="#eeeeee")
sheet.grid(row=0, column=0, sticky="nsew")
# SỬA ĐỔI: Hàm resize vẫn giữ nguyên...
def resize_columns(event=None):
    # Lấy chiều rộng từ widget cha (right frame)
    width = right.winfo_width() - 15 # Trừ đi padx*2 và một chút lề
    if width <= 1: return
    ratios = [0.30, 0.20, 0.20, 0.30]
    new_widths = [int(width * r) for r in ratios]
    try:
        sheet.column_widths(new_widths)
    except: pass
right.bind("<Configure>", resize_columns)

def update_table(record):
    sheet.dehighlight_all()
    data_as_lists = [list(row) for row in sensor_data]
    sheet.set_sheet_data(data_as_lists)
    new_row_index  = len(sensor_data) - 1
    if new_row_index >= 0:
        sheet.highlight_rows([new_row_index], bg='#D2EAF8')
    sheet.see(row=new_row_index, column=0)  # Cuộn đến hàng mới
    sheet.redraw()

def clear_table():
    sensor_data.clear()
    root.after(0, lambda: sheet.set_sheet_data([]))
    print("Đã xóa bảng dữ liệu lúc 00:00")

def auto_clear_loop():
    while not stop_event.is_set():
        now = datetime.now()
        if now.hour == 0 and now.minute == 0:
            clear_table()
            time.sleep(60)  # Tránh lặp lại trong cùng phút
        time.sleep(10)


def on_message(client, userdata, msg):
    if not listening:
        return
    try:
        data = json.loads(msg.payload.decode())
        name = data.get("sensorname", msg.topic)
        value = float(data.get("value", 0))
        ts = float(data.get("timestamp", time.time()))
        current_threshold = float(threshold_entry.get())
        status = "VUOT MUC" if value > current_threshold else "AN TOAN"
        time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S %d-%m")
        record = (name, value, status, time_str)
        sensor_data.append(record)
        root.after(0, update_table, record)
        threading.Thread(target=flash_led, args=(LED1_PIN,), daemon=True).start()
        if value > current_threshold:
            threading.Thread(target=flash_led, args=(LED2_PIN,), daemon=True).start()
        pub_topic = pub_entry.get().strip()
        if pub_topic:
            client.publish(pub_topic, f"({value}, {status}, {int(ts)})")

    except json.JSONDecodeError:
        print(f"Error decoding JSON from topic '{msg.topic}'")
    except ValueError:
        print(f"Error converting value to float from topic '{msg.topic}'")
    except Exception as e:
        print(f"Lỗi trong on_message: {e}")

def on_disconnect(client, userdata, rc):
    global exiting
    if exiting or not listening:
        return
    if rc != 0:
        root.after(0, update_disconnect_status)
    reconnect_thread = threading.Thread(target=_attempt_reconnect, args=(client,), daemon=True)
    reconnect_thread.start()

def update_disconnect_status():   
        print("Mất kết nối MQTT, đang thử kết nối lại...")
        status_label.config(text="Trạng thái: MẤT KẾT NỐI", foreground="orange")

def _attempt_reconnect(client):
        time.sleep(2)  # Đợi một chút trước khi reconnect
        if listening:
            try:                
                print(" Đang thực hiện reconnect MQTT...")
                client.reconnect()
                print("Đã gửi yêu cầu reconnect MQTT.")
            except Exception as e:
                print(f" Reconnect thất bại: {e}")

client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

def toggle_on():
    global listening, blink_mode
    if blink_mode:
        toggle_blink() 

    listening = True
    status_label.config(text="Trạng thái: ĐANG KẾT NỐI...", foreground="orange")
    update_mqtt() 

def toggle_off():
    global listening
    listening = False
    try: client.loop_stop(force=True)
    except: pass
    if current_topics:
        for t in current_topics:
            client.unsubscribe(t)
        print(f"Đã hủy đăng ký các topics: {current_topics}")
    
    status_label.config(text="Trạng thái: THỦ CÔNG", foreground="red")

def toggle_led(pin):
    if not listening:
        try:
            GPIO.output(pin, not GPIO.input(pin))
        except Exception as e:
            print(f"Could not toggle LED on pin {pin}: {e}")
    else:
        messagebox.showwarning("Cảnh báo", "Vui lòng tắt chế độ tự động trước khi điều khiển LED thủ công")

def blink_loop():
    state = True
    while not stop_event.is_set():
        if blink_mode and not listening:
            try:
                GPIO.output(LED1_PIN, state)
                GPIO.output(LED2_PIN, state)
                state = not state
            except Exception as e:
                print(f"Lỗi trong blink_loop: {e}")
        time.sleep(0.5)

def toggle_blink():
    global blink_mode
    if listening:
        messagebox.showwarning("Cảnh báo", "Vui lòng tắt chế độ tự động trước khi bật BLINK")
        return

    blink_mode = not blink_mode
    if blink_mode:
        status_label.config(text="Trạng thái: BLINK", foreground="blue")
    else:
        try:
            GPIO.output(LED1_PIN, GPIO.LOW)
            GPIO.output(LED2_PIN, GPIO.LOW)
        except Exception as e:
            print(f"Could not turn off LEDs: {e}")
        status_label.config(text="Trạng thái: THỦ CÔNG", foreground="red")

def console_input_listener():
    global SHUTDOWN_REQUESTED
    while not stop_event.is_set():
        try:
            command = input("Gõ 'exit' và nhấn Enter để thoát ứng dụng:\n")
            if command.strip().lower() == 'exit':
                print("Lệnh 'exit' đã được nhận. Đang tắt ứng dụng...")
                SHUTDOWN_REQUESTED = True
                break
        except (EOFError, KeyboardInterrupt):
            SHUTDOWN_REQUESTED = True
            break

def _write_csv_in_background(path, data_to_save):
    try:
        with open(path, "w", newline="", encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Tên", "Giá trị", "Trạng thái", "Thời gian"])
            writer.writerows(data_to_save)
        root.after(0, lambda: messagebox.showinfo("Thành công", f"Đã lưu dữ liệu vào {path}"))
    except Exception as e:
        root.after(0, lambda: messagebox.showerror("Lỗi", f"Không thể lưu file: {e}"))
    finally:
        root.after(0, lambda: save_csv_button.config(state="normal"))

def save_to_csv():
    if not sensor_data:
        messagebox.showinfo("Thông báo", "Không có dữ liệu để lưu.")
        return
    path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    if path:
        save_csv_button.config(state="disabled")
        data_copy = list(sensor_data)
        threading.Thread(
            target=_write_csv_in_background, 
            args=(path, data_copy), 
            daemon=True
        ).start()

def exit_program():
    if messagebox.askokcancel("Xác nhận", "Bạn có muốn thoát chương trình?"):
        global exiting
        exiting = True
        if sensor_data and messagebox.askyesno("Lưu dữ liệu", "Bạn có muốn lưu bảng dữ liệu ra file CSV không?"):
            save_to_csv() # Gọi hàm lưu file TRƯỚC KHI hủy cửa sổ.
        print("Đang thoát chương trình...")
        save_config_apply()
        stop_event.set()
        try:
            client.loop_stop()
            client.disconnect()
            print("MQTT client disconnected.")
        except:
            pass
        try:
            GPIO.cleanup()
            print("GPIO cleanup successful.")
        except: pass
        root.destroy()

def force_exit_program():
    global exiting
    if exiting:  # Tránh gọi nhiều lần
        return
    print("Thực hiện thoát đột ngột...")
    exiting = True
    save_config_apply(silent=True)  
    stop_event.set()
    try:
        client.loop_stop()
        client.disconnect()
        print("MQTT client disconnected.")
    except Exception as e:
        print(f"Lỗi khi ngắt kết nối MQTT (có thể bỏ qua): {e}")
        pass
    try:
        GPIO.cleanup()
        print("GPIO cleanup successful.")
    except Exception as e:
        print(f"Lỗi khi dọn dẹp GPIO (có thể bỏ qua): {e}")
        pass
    print("Đóng ứng dụng.")
    root.destroy()

def signal_handler(signum, frame):
    global SHUTDOWN_REQUESTED
    SHUTDOWN_REQUESTED = True

# ==== Control Buttons ====
ctrl = ttk.Frame(right)
ctrl.grid(row=2, column=0, pady=5, sticky="ew")
ctrl.grid_columnconfigure((0, 1, 2, 3), weight=1)
right.grid_rowconfigure(2, weight=0)
# Thay đổi command của nút "Tự động"
ttk.Button(ctrl, text="Tự động (ON)", command=toggle_on, bootstyle="success").grid(row=0, column=0, padx=3)
ttk.Button(ctrl, text="Thủ công (OFF)", command=toggle_off, bootstyle="danger").grid(row=0, column=1, padx=3)
save_csv_button = ttk.Button(ctrl, text="Lưu CSV", command=save_to_csv, bootstyle="info")
save_csv_button.grid(row=0, column=2, padx=3)
ttk.Button(ctrl, text="Thoát", command=exit_program, bootstyle="secondary").grid(row=0, column=3, padx=3)

led_panel = ttk.LabelFrame(right, text="LED Thủ công")
led_panel.grid(row=3, column=0, pady=5, sticky="ew")
led_panel.grid_columnconfigure((0, 1, 2), weight=1)
right.grid_rowconfigure(3, weight=0)
ttk.Button(led_panel, text="LED1", width=10, command=lambda: toggle_led(LED1_PIN)).grid(row=0, column=0, padx=5, pady=5)
ttk.Button(led_panel, text="LED2", width=10, command=lambda: toggle_led(LED2_PIN)).grid(row=0, column=1, padx=5, pady=5)
ttk.Button(led_panel, text="BLINK", width=10, command=toggle_blink).grid(row=0, column=2, padx=5, pady=5)

# ==== Run ====
console_listener_thread = threading.Thread(target=console_input_listener, daemon=True)
console_listener_thread.start()
threading.Thread(target=blink_loop, daemon=True).start()
threading.Thread(target=auto_clear_loop, daemon=True).start()
signal.signal(signal.SIGINT, signal_handler)  # Bắt Ctrl+C
root.protocol("WM_DELETE_WINDOW", exit_program)
load_config() # Tải cấu hình khi khởi động
update_table(None)
print("Chương trình đã sẵn sàng. Đóng cửa sổ hoặc nhấn Ctrl+C để thoát.")
root.mainloop()


