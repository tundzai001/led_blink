import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sys
sys.path.append("/home/nam/.local/lib/python3.11/site-packages")
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import csv
import RPi.GPIO as GPIO
import threading
import time

# MQTT config
MQTT_BROKER = "aitogy.xyz"
MQTT_PORT = 1883
MQTT_USER = "abc"
MQTT_PASS = "xyz"
MQTT_TOPIC_PUB = "led_data"

# GPIO config
LED1_PIN = 3
LED2_PIN = 27
GPIO.setmode(GPIO.BCM)
GPIO.setup(LED1_PIN, GPIO.OUT)
GPIO.setup(LED2_PIN, GPIO.OUT)

# Global variables
threshold = 1.3
sensor_data = []
listening = False
led1_on = False
led2_on = False
blink_mode = False
stop_event = threading.Event()
connection_status = True
current_topics = []
blinking_pins = set()
# MQTT client
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.username_pw_set(MQTT_USER, MQTT_PASS)

# GUI root
root = tk.Tk()
root.title("Giao dien cam bien + Dieu khien LED")

# Main layout
main_frame = tk.Frame(root)
main_frame.pack(fill="both", expand=True)

# ==== LEFT SIDE: TOPIC INPUT ====
left_frame = tk.Frame(main_frame)
left_frame.grid(row=0, column=0, sticky="n")

tk.Label(left_frame, text="Nhap topic MQTT\n(1 topic moi dong):").pack(anchor="w", padx=5, pady=(5,0))
topic_input = tk.Text(left_frame, height=15, width=30)
topic_input.pack(padx=5, pady=5)

def update_topics():
    global current_topics
    new_topics = topic_input.get("1.0", "end").strip().splitlines()
    for t in current_topics:
        client.unsubscribe(t)
    current_topics = new_topics
    for t in current_topics:
        client.subscribe(t)
    print("Dang lang nghe cac topic:", current_topics)

tk.Button(left_frame, text="Cap nhat Topic", command=update_topics).pack(padx=5, pady=(0,10))

# ==== RIGHT SIDE ====
right_frame = tk.Frame(main_frame)
right_frame.grid(row=0, column=1, sticky="n")

status_label = tk.Label(right_frame, text="Trang thai: THU CONG", fg="red")
status_label.pack(pady=5)

warning_overlay = tk.Label(
    right_frame,
    text="? M?T K?T N?I MQTT ?",
    font=("Arial", 24, "bold"),
    fg="white",
    bg="red",
    padx=20,
    pady=20
)
warning_overlay.place(relx=0.5, rely=0.5, anchor="center")
warning_overlay.lower()

# Data table
tree = ttk.Treeview(right_frame, columns=("Ten", "Gia tri", "Trang thai", "Thoi gian"), show="headings")
for col in tree["columns"]:
    tree.heading(col, text=col)
tree.pack(padx=10, pady=10, fill="both", expand=True)

def update_table(record):
    color = "red" if record[2] == "VUOT MUC CANH BAO" else "green"
    tree.insert("", "end", values=record, tags=(color,))
    tree.tag_configure("red", background="#FFCCCC")
    tree.tag_configure("green", background="#CCFFCC")
    tree.yview_moveto(1.0)

def on_message(client, userdata, msg):
    if not listening:
        return
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        name = data.get("sensorname", msg.topic)
        value = float(data.get("value", 0))
        ts = float(data.get("timestamp", 0))
        status = "VUOT MUC CANH BAO" if value > threshold else "AN TOAN"
        time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S %d%m%y")

        sensor_data.append((name, value, status, time_str))
        update_table((name, value, status, time_str))

        def flash_led(pin):
            GPIO.output(pin, GPIO.HIGH)
            time.sleep(0.2)
            GPIO.output(pin, GPIO.LOW)

        threading.Thread(target=flash_led, args=(LED1_PIN,)).start()
        if value > threshold:
            threading.Thread(target=flash_led, args=(LED2_PIN,)).start()

        msg_send = f"({value} {status} ,{int(ts)})"
        client.publish(MQTT_TOPIC_PUB, msg_send)
    except Exception as e:
        print("Loi khi nhan tin MQTT:", e)

def toggle_on():
    global listening, blink_mode, blinking_pins
    print("DA NHAN NUT ON")
    listening = True
    blink_mode = False
    blinking_pins.clear()
    GPIO.output(LED1_PIN, GPIO.LOW)
    GPIO.output(LED2_PIN, GPIO.LOW)
    for t in current_topics:
        client.subscribe(t)
    if client.is_connected():
        print("MQTT DANG KET NOI")
        warning_overlay.lower()
        status_label.config(text="Trang thai: TU DONG", fg="green")
    else:
        print("MQTT CHUA KET NOI")
        warning_overlay.lift()
        status_label.config(text="Trang thai: MAT KET NOI MQTT", fg="dark red")

def toggle_off():
    global listening
    listening = False
    for t in current_topics:
        client.unsubscribe(t)
    status_label.config(text="Trang thai: THU CONG", fg="red")

def set_threshold():
    global threshold
    try:
        threshold = float(threshold_entry.get())
    except:
        pass

def clear_table():
    for row in tree.get_children():
        tree.delete(row)
    sensor_data.clear()

def save_to_csv():
    if not sensor_data:
        return
    file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    if file_path:
        with open(file_path, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Ten", "Gia tri", "Trang thai", "Thoi gian"])
            for record in sensor_data:
                writer.writerow(record)

def exit_program():
    if messagebox.askokcancel("Xac nhan", "Ban co muon thoat chuong trinh?"):
        stop_event.set()
        GPIO.cleanup()
        client.disconnect()
        root.destroy()

def toggle_led(pin):
    global led1_on, led2_on, blink_mode
    if pin == LED1_PIN:
        led1_on = not led1_on
        GPIO.output(LED1_PIN, GPIO.HIGH if led1_on else GPIO.LOW)
    elif pin == LED2_PIN:
        led2_on = not led2_on
        GPIO.output(LED2_PIN, GPIO.HIGH if led2_on else GPIO.LOW)
    if blink_mode and not led1_on and not led2_on:
        blink_mode = False

def toggle_blink():
    global blink_mode, led1_on, led2_on
    if not blink_mode:
        if not led1_on:
            led1_on = True
            GPIO.output(LED1_PIN, GPIO.HIGH)
        if not led2_on:
            led2_on = True
            GPIO.output(LED2_PIN, GPIO.HIGH)
        blink_mode = True
    else:
        blink_mode = False
        led1_on = led2_on = False
        GPIO.output(LED1_PIN, GPIO.LOW)
        GPIO.output(LED2_PIN, GPIO.LOW)

def blink_loop():
    state = True
    while not stop_event.is_set():
        if blink_mode:
            if led1_on:
                GPIO.output(LED1_PIN, GPIO.HIGH if state else GPIO.LOW)
            if led2_on:
                GPIO.output(LED2_PIN, GPIO.HIGH if state else GPIO.LOW)
        state = not state
        time.sleep(0.5)

threading.Thread(target=blink_loop, daemon=True).start()

def monitor_connection():
    global connection_status
    while not stop_event.is_set():
        connected = client.is_connected()
        if not connected and connection_status:
            connection_status = False
            status_label.config(text="Trang thai: MAT KET NOI MQTT", fg="dark red")
            warning_overlay.lift()
        elif connected and not connection_status:
            connection_status = True
            status_label.config(
                text="Trang thai: TU DONG" if listening else "Trang thai: THU CONG",
                fg="green" if listening else "red"
            )
            warning_overlay.lower()
        time.sleep(3)

# GUI Controls
frame = tk.Frame(right_frame)
frame.pack(pady=5)

threshold_entry = tk.Entry(frame, width=10)
threshold_entry.insert(0, str(threshold))
threshold_entry.grid(row=0, column=1)
tk.Label(frame, text="Nguong hien tai:").grid(row=0, column=0)
tk.Button(frame, text="Cap nhat", command=set_threshold).grid(row=0, column=2, padx=5)
tk.Button(frame, text="Xoa bang", command=clear_table).grid(row=0, column=3, padx=5)
tk.Button(frame, text="Luu CSV", command=save_to_csv).grid(row=0, column=4, padx=5)
tk.Button(frame, text="ON (tu dong)", bg="#CCFFCC", command=toggle_on).grid(row=0, column=5, padx=5)
tk.Button(frame, text="OFF (thu cong)", bg="#FFCCCC", command=toggle_off).grid(row=0, column=6, padx=5)
tk.Button(frame, text="EXIT", bg="gray", command=exit_program).grid(row=0, column=7, padx=5)
def manual_only(action):
    if not listening:
        action()
    else:
        messagebox.showinfo("CANH BAO", "BAN DANG O CHE DO TU DONG. HAY CHUYEN VE CHE DO THU CONG (OFF) DE DIEU KHIEN LED.")

def clear_table():
    for row in tree.get_children():
        tree.delete(row)
    sensor_data.clear()

def check_and_clear_table():
    now = datetime.now()
    if now.hour == 0 and now.minute == 0:
        clear_table()
        print(" Da xoa bang du lieu luc 00:00")

    root.after(60000, check_and_clear_table)
check_and_clear_table()

manual = tk.Frame(right_frame)
manual.pack(pady=10)
tk.Label(manual, text="Dieu khien LED thu cong:").grid(row=0, column=0, columnspan=3, pady=(0,5))
tk.Button(manual, text="LED1", command=lambda: manual_only(lambda: toggle_led(LED1_PIN))).grid(row=1, column=0, padx=10)
tk.Button(manual, text="LED2", command=lambda: manual_only(lambda: toggle_led(LED2_PIN))).grid(row=1, column=1, padx=10)
tk.Button(manual, text="BLINK", command=lambda: manual_only(toggle_blink)).grid(row=1, column=2, padx=10)

# MQTT setup and loop
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_start()
threading.Thread(target=monitor_connection, daemon=True).start()

# Exit protocol
root.protocol("WM_DELETE_WINDOW", exit_program)
root.mainloop()
