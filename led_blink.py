import RPi.GPIO as GPIO
import time
import threading
import sys
sys.path.append("/home/nam/.local/lib/python3.11/site-packages")
import paho.mqtt.client as mqtt
import time as t
import os


MQTT_BROKER = "aitogy.xyz"
MQTT_PORT = 1883
MQTT_TOPIC ="led_data"
MQTT_USER = "abc"
MQTT_PASS = "xyz"

GPIO.setmode(GPIO.BCM)
LED1_PIN=4
LED2_PIN=27
GPIO.setup(LED1_PIN, GPIO.OUT)
GPIO.setup(LED2_PIN, GPIO.OUT)
sleep_blink = 0.5
sleep_manual = 2

led1_on = False
led2_on = False
blink_mode = False 
blink_thread_started = False
is_connected = False 

cilent = mqtt.Client()
cilent.username_pw_set("anything", "any_password")
def try_connect():
    global is_connected
    try:
        cilent.connect(MQTT_BROKER, MQTT_PORT, 60)
        is_connected = True
        print(" DA KET NOI MQTT")
    except:
        is_connected = False
        print(" KHONG KET NOI DUOC MQTT")

try_connect()

def auto_reconnect():
    global is_connected 
    while True: 
        if not is_connected:
            try_connect()
        time.sleep(5)
    
threading.Thread(target=auto_reconnect, daemon=True).start()
def publish(status1, status2):
    global is_connected
    unix_time = int(t.time())
    msg = f"led1:{status1.lower()},led2:{status2. lower()}, timestamp:{unix_time}"
    if is_connected:    
        try:
            cilent.publish(MQTT_TOPIC, msg)
            print(f" GUI MQTT: {msg}")
        except:
            print(f"GUI THAT BAI, LUU: {msg}")
            is_connected = False
    else:
        print(f"MAT KET NOI, LUU: {msg}")
def manual_control():
    while True:
        if not blink_mode:
            GPIO.output(LED1_PIN, GPIO.HIGH if state else GPIO.LOW)
            GPIO.output(LED2_PIN, GPIO.HIGH if state else GPIO.LOW)
            publish("ON" if state else "OFF", "ON" if state else "OFF")
            time.sleep(sleep_manual)
threading.Thread(target=manual_control, daemon=True).start()
        
def blink_launcher():
    global blink_thread_started
    state = True
    while True:
        if blink_mode:
            GPIO.output(LED1_PIN, GPIO.HIGH if led1_on else GPIO.LOW)
            GPIO.output(LED2_PIN, GPIO.LOW if led2_on else GPIO.HIGH)
            publish("ON" if led1_on else "OFF", "OFF" if led2_on else "ON")    
            time.sleep(sleep_blink)
        else: 
            time.sleep(0.1)

print(" NHAP LENH: led1 on/off, led2 on/off, blink on /off, exit")

try:
    while True:
        cmd = input(">>").strip().lower()
        if cmd == "led1 on":
            led1_on = True
            print ("LED1 DANG BAT... ")
        elif cmd == "led1 off":
            led1_on = False
            print ("LED1 DA TAT...")
        elif cmd == "led2 on":
            led2_on = True
            print ("LED2 DANG BAT...")
        elif cmd == "led2 off":
            led2_on = False
            print ("LED2 DA TAT")
        elif cmd == "blink on":
            blink_mode = True
            if not blink_thread_started:
                threading.Thread(target=blink_launcher, daemon=True).start()
                blink_thread_started = True
            print ("BAT CHE DO NHAY LUAN PHIEN")
        elif cmd == "blink off":
            blink_mode = False
            print ("TAT CHE DO NHAY LUAN PHIEN")
        elif cmd == "exit":
            break
        print("LENH KHONG HOP LE. NHAP led1 on/off, led2 on/off, blink on/off, exit")
            
except KeyboardInterrupt:
    pass
    
finally:
    GPIO.cleanup()
    cilent.disconnect()
    print("DA THOAT")
    
