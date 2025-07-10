import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import csv
import RPi.GPIO as GPIO
import threading
import time
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# GPIO setup
LED1_PIN = 3
LED2_PIN = 27
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)
GPIO.setup(LED1_PIN, GPIO.OUT)
GPIO.setup(LED2_PIN, GPIO.OUT)

# Globals
threshold = 1.3
sensor_data = []
listening = False
blink_mode = False
stop_event = threading.Event()
current_topics = []

client = mqtt.Client(protocol=mqtt.MQTTv311)
client.username_pw_set("abc", "xyz")

# ==== Flash LED ====
def flash_led(pin, duration=0.3):
    GPIO.output(pin, GPIO.HIGH)
    time.sleep(duration)
    GPIO.output(pin, GPIO.LOW)

# ==== GUI ====
root = tk.Tk()
root.title("Giao diện Cảm biến & Điều khiển LED")
root.geometry("800x480")
main = tk.Frame(root, padx=10, pady=10)
main.pack(fill="both", expand=True)

style = ttk.Style()
style.configure("Treeview.Heading", font=("Arial", 10, "bold"))
style.configure("Treeview", rowheight=24, font=("Arial", 10))

# ==== LEFT PANEL ====
left = tk.LabelFrame(main, text="Cài đặt MQTT", padx=10, pady=10)
left.grid(row=0, column=0, sticky="nsw", padx=(0, 15))

def add_labeled_entry(frame, label, row, default="", width=14, show=None):
    tk.Label(frame, text=label).grid(row=row, column=0, sticky="w")
    entry = tk.Entry(frame, width=width, show=show)
    entry.insert(0, default)
    entry.grid(row=row, column=1, sticky="w", pady=2)
    return entry

broker_entry = add_labeled_entry(left, "MQTT Broker:", 0, "aitogy.xyz")
port_entry = add_labeled_entry(left, "Port:", 1, "1883")
user_entry = add_labeled_entry(left, "Username:", 2, "abc")
pass_entry = add_labeled_entry(left, "Password:", 3, "xyz", show="*")
pub_entry   = add_labeled_entry(left, "Publish Topic:", 4, "led_data",)
threshold_entry = add_labeled_entry(left, "Ngưỡng cảnh báo:", 5, str(threshold))

def toggle_pass():
    if pass_entry.cget("show") == "":
        pass_entry.config(show="*")
        show_btn.config(text="👁")
    else:
        pass_entry.config(show="")
        show_btn.config(text="🙈")

show_btn = tk.Button(left, text="👁", command=toggle_pass, width=2)
show_btn.grid(row=3, column=2, sticky="w")

tk.Label(left, text="Subscribe Topics (1 dòng mỗi topic):").grid(row=6, column=0, columnspan=2, sticky="w")
topic_input = tk.Text(left, width=22, height=6)
topic_input.grid(row=8, column=0, columnspan=3, pady=(0,5))

def update_mqtt():
    global current_topics
    try:
        client.loop_stop()
        client.disconnect()
    except: 
        pass

    broker = broker_entry.get().strip()
    port_text = port_entry.get().strip()
    pwd = pass_entry.get().strip()
    user = user_entry.get().strip()
    topics = topic_input.get("1.0", "end").strip().splitlines()

    if not broker:
        messagebox.showerror("MQTT Error", "Vui lòng nhập địa chỉ MQTT Broker")
        return
    try:
        port= int(port_text)               
    except ValueError: 
        messagebox.showerror("MQTT Error", "Port phải là số nguyên")
        return
    if not topics:
        messagebox.showerror("MQTT Error", "Vui lòng nhập ít nhất một topic để subscribe")
        return
    client.username_pw_set(user, pwd)
    try:
        client.connect(broker, port=port, keepalive=60)
        client.loop_start()
    except Exception as e:
        messagebox.showerror("MQTT Error", f"Lỗi kết nối:{e}")
        return
    for t in current_topics:
        client.unsubscribe(t)
    current_topics = topics
    for t in current_topics:    
        client.subscribe(t)
    messagebox.showinfo("MQTT", "Đã cập nhật MQTT và topic")

tk.Button(left, text="Cập nhật MQTT", command=update_mqtt, bg="#e0e0e0").grid(row=8, column=0, columnspan=3, pady=5)

right = tk.Frame(main)
# ==== RIGHT PANEL ====
right.grid(row=0, column=1, sticky="nsew")
right.grid_columnconfigure(0, weight=1)

status_label = tk.Label(right, text="Trạng thái: THỦ CÔNG", fg="red", font=("Arial", 11, "bold"))
status_label.pack(pady=5)

tree = ttk.Treeview(right, columns=("Tên", "Giá trị", "Trạng thái", "Thời gian"), show="headings", height=10)
for col in tree["columns"]:
    tree.heading(col, text=col)
tree.pack(padx=5, pady=5, fill="x")

def update_table(record):
    color = "red" if record[2] == "VUOT MUC" else "green"
    tree.insert("", "end", values=record, tags=(color,))
    tree.tag_configure("red", background="#FFCCCC")
    tree.tag_configure("green", background="#CCFFCC")
    tree.yview_moveto(1.0)

def on_message(client, userdata, msg):
    if not listening:
        return
    try:
        data = json.loads(msg.payload.decode())
        name = data.get("sensorname", msg.topic)
        value = float(data.get("value", 0))
        ts = float(data.get("timestamp", time.time()))
        threshold = float(threshold_entry.get())
        status = "VUOT MUC" if value > threshold else "AN TOAN"
        time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S %d-%m")

        sensor_data.append((name, value, status, time_str))
        update_table((name, value, status, time_str))

        threading.Thread(target=flash_led, args=(LED1_PIN,), daemon=True).start()
        if value > threshold:
            threading.Thread(target=flash_led, args=(LED2_PIN,), daemon=True).start()

        client.publish(pub_entry.get().strip(), f"({value}, {status}, {int(ts)})")
    except Exception as e:
        print("MQTT error:", e)

client.on_message = on_message
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Mất kết nối MQTT, đang thử kết nối lại...")
        while not stop_event.is_set():
            try:
                time.sleep(5)
                client.reconnect()
                print("Đã kết nối lại MQTT")
                break
            except:
                print("Thử lại kết nối MQTT...")
                continue

client.on_disconnect = on_disconnect

def toggle_on():
    global listening
    listening = True
    for t in current_topics:
        client.subscribe(t)
    status_label.config(text="Trạng thái: TỰ ĐỘNG", fg="green")

def toggle_off():
    global listening
    listening = False
    for t in current_topics:
        client.unsubscribe(t)
    status_label.config(text="Trạng thái: THỦ CÔNG", fg="red")

def toggle_led(pin):
    GPIO.output(pin, not GPIO.input(pin))

def blink_loop():
    state = True
    while not stop_event.is_set():
        if blink_mode:
            GPIO.output(LED1_PIN, state)
            GPIO.output(LED2_PIN, state)
            state = not state
        time.sleep(0.5)

def toggle_blink():
    global blink_mode
    blink_mode = not blink_mode
    if not blink_mode:
        GPIO.output(LED1_PIN, GPIO.LOW)
        GPIO.output(LED2_PIN, GPIO.LOW)

def save_to_csv():
    if not sensor_data: return
    path = filedialog.asksaveasfilename(defaultextension=".csv")
    if path:
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Tên", "Giá trị", "Trạng thái", "Thời gian"])
            writer.writerows(sensor_data)

def exit_program():
    if messagebox.askokcancel("Xác nhận", "Bạn có muốn thoát chương trình?"):
        stop_event.set()
        try:
            client.loop_stop()
            client.disconnect()
        except: pass
        GPIO.cleanup()
        root.destroy()

# ==== Control Buttons ====
ctrl = tk.Frame(right)
ctrl.pack(pady=5)
tk.Button(ctrl, text="Tự động (ON)", command=toggle_on, bg="#CCFFCC").grid(row=0, column=0, padx=3)
tk.Button(ctrl, text="Thủ công (OFF)", command=toggle_off, bg="#FFCCCC").grid(row=0, column=1, padx=3)
tk.Button(ctrl, text="Lưu CSV", command=save_to_csv).grid(row=0, column=2, padx=3)
tk.Button(ctrl, text="Thoát", command=exit_program, bg="gray").grid(row=0, column=3, padx=3)

led_panel = tk.LabelFrame(right, text="LED Thủ công")
led_panel.pack(pady=5)
tk.Button(led_panel, text="LED1", width=10, command=lambda: toggle_led(LED1_PIN)).grid(row=0, column=0, padx=5, pady=5)
tk.Button(led_panel, text="LED2", width=10, command=lambda: toggle_led(LED2_PIN)).grid(row=0, column=1, padx=5, pady=5)
tk.Button(led_panel, text="BLINK", width=10, command=toggle_blink).grid(row=0, column=2, padx=5, pady=5)

# ==== Run ====
threading.Thread(target=blink_loop, daemon=True).start()
root.protocol("WM_DELETE_WINDOW", exit_program)
root.mainloop()
