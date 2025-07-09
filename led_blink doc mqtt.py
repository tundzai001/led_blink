import RPi.GPIO as GPIO
import time
import threading
import sys
sys.path.append("/home/nam/.local/lib/python3.11/site-packages")
import json
import paho.mqtt.client as mqtt
import time as t

# Cau hinh MQTT
MQTT_BROKER = "aitogy.xyz"
MQTT_PORT = 1883
MQTT_TOPIC_SUB = "modbus_data_1"
MQTT_TOPIC_PUB = "led_data"
MQTT_USER = "abc"
MQTT_PASS = "xyz"

# Cau hinh GPIO
GPIO.setmode(GPIO.BCM)
LED1_PIN = 3
LED2_PIN = 27
GPIO.setup(LED1_PIN, GPIO.OUT)
GPIO.setup(LED2_PIN, GPIO.OUT)

# Bien trang thai
led1_on = True
led2_on = True
blink_led1 = False
blink_led2 = False
auto_mode = False
is_connected = False
running = True

# MQTT Client
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)

def try_connect():
    global is_connected
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print("Da ket noi toi MQTT broker")
        is_connected = True
    except Exception as e:
        print("Khong the ket noi MQTT:", e)
        is_connected = False

try_connect()

def auto_reconnect():
    global is_connected
    while running:
        if not is_connected:
            try_connect()
        time.sleep(5)

threading.Thread(target=auto_reconnect, daemon=True).start()

# Gui tin nhan
def publish_sensor_status(value):
    global is_connected
    unix_time = int(t.time())
    status = "VUOT MUC CANH BAO" if value > 1.3 else "AN TOAN"
    msg = f"({value} {status} ,{unix_time})"
    if is_connected:
        try:
            client.publish(MQTT_TOPIC_PUB, msg)
            print("Da gui:", msg)
        except:
            print("Gui that bai:", msg)
            is_connected = False
    else:
        print("Mat ket noi. Luu tam:", msg)

# Xu ly MQTT
def on_message(client, userdata, msg):
    global blink_led1, blink_led2
    if not auto_mode:
        return
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        value = float(data.get("value", 0))

        blink_led1 = True
        threading.Timer(1.5, lambda: setattr(sys.modules[__name__], 'blink_led1', False)).start()

        if value > 1.3:
            blink_led2 = True
            threading.Timer(1.5, lambda: setattr(sys.modules[__name__], 'blink_led2', False)).start()

        publish_sensor_status(value)

    except Exception as e:
        print("Loi xu ly MQTT:", e)

client.on_message = on_message
client.loop_start()

# Nhay led tu dong (che do tu dong)
def auto_blink():
    blink_state = True
    while running:
        if auto_mode:
            GPIO.output(LED1_PIN, GPIO.HIGH if blink_led1 and blink_state else GPIO.LOW)
            GPIO.output(LED2_PIN, GPIO.HIGH if blink_led2 and blink_state else GPIO.LOW)
            blink_state = not blink_state
            time.sleep(0.5)
        else:
            time.sleep(0.1)

threading.Thread(target=auto_blink, daemon=True).start()

# Nhay led thu cong (blink on)
def manual_blink_loop():
    blink_state = False
    while running:
        if not auto_mode and (blink_led1 or blink_led2):
            if blink_led1:
                GPIO.output(LED1_PIN, GPIO.HIGH if led1_on and blink_state else GPIO.LOW)
            else:
                GPIO.output(LED1_PIN, GPIO.HIGH if led1_on else GPIO.LOW)

            if blink_led2:
                GPIO.output(LED2_PIN, GPIO.HIGH if led2_on and blink_state else GPIO.LOW)
            else:
                GPIO.output(LED2_PIN, GPIO.HIGH if led2_on else GPIO.LOW)

            blink_state = not blink_state
            time.sleep(0.5)
        else:
            time.sleep(0.1)

threading.Thread(target=manual_blink_loop, daemon=True).start()

# Dieu khien led thu cong
def manual_control_loop():
    while running:
        if not auto_mode:
            if not blink_led1:
                GPIO.output(LED1_PIN, GPIO.HIGH if led1_on else GPIO.LOW)
            if not blink_led2:
                GPIO.output(LED2_PIN, GPIO.HIGH if led2_on else GPIO.LOW)
        time.sleep(0.1)

threading.Thread(target=manual_control_loop, daemon=True).start()

# Menu dieu khien
print("Nhap lenh: on / off / exit")

try:
    while True:
        cmd = input(">> ").strip().lower()

        if cmd == "on":
            auto_mode = True
            client.subscribe(MQTT_TOPIC_SUB)
            print(f"Da ket noi toi topic: {MQTT_TOPIC_SUB}")
            print("Da bat che do tu dong")

        elif cmd == "off":
            auto_mode = False
            print("Da chuyen sang che do thu cong")
            print("Nhap lenh: led1 on/off / led2 on/off / blink on/off / exit")

        elif cmd == "exit":
            break

        elif not auto_mode:
            if cmd == "led1 on":
                led1_on = True
                print("LED1 da bat")
            elif cmd == "led1 off":
                led1_on = False
                print("LED1 da tat")
            elif cmd == "led2 on":
                led2_on = True
                print("LED2 da bat")
            elif cmd == "led2 off":
                led2_on = False
                print("LED2 da tat")
            elif cmd == "blink on":
                led1_on = True
                led2_on = True
                blink_led1 = True
                blink_led2 = True
                print("Da bat che do nhay")
            elif cmd == "blink off":
                blink_led1 = False
                blink_led2 = False
                print("Da tat che do nhay")
            else:
                print("Lenh khong hop le. Nhap lai.")

        else:
            print("Lenh khong hop le. Chi nhap: on / off / exit")

except KeyboardInterrupt:
    print("Dang thoat...")

finally:
    running = False
    time.sleep(0.2)
    GPIO.cleanup()
    client.disconnect()
    print("Da thoat hoan toan.")
