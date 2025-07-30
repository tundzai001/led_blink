# train_and_pack_robust_model.py
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, RepeatVector, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
import joblib
import time
import os
import json

print(f"Bắt đầu quy trình huấn luyện và đóng gói MÔ HÌNH VỮNG VÀNG...")
start_time = time.time()

# --- CẤU HÌNH ---
SEQUENCE_LENGTH = 180
N_FEATURES = 4
DATA_FILE = 'normal_data_robust.csv' # Sử dụng file dữ liệu mới

# --- Tên file output ---
SCALER_PATH = 'universal_scaler.gz'
AUTOENCODER_TFLITE_PATH = 'foundation_model_autoencoder.tflite'
FORECASTER_TFLITE_PATH = 'foundation_model_forecaster.tflite'
AUTOENCODER_H5_PATH = 'temp_autoencoder.h5'
FORECASTER_H5_PATH = 'temp_forecaster.h5'

# 1. Tải và chuẩn bị dữ liệu
if not os.path.exists(DATA_FILE):
    print(f"LỖI: Không tìm thấy file '{DATA_FILE}'. Vui lòng chạy script tạo dữ liệu trước.")
    exit()
    
print(f"Đang tải dữ liệu từ '{DATA_FILE}'...")
df = pd.read_csv(DATA_FILE)
# Đổi tên cột height thành h để khớp với logic cũ nếu cần, hoặc cập nhật logic
df.rename(columns={'height': 'h'}, inplace=True)
features_df = df[['lat', 'lon', 'h', 'water_level']]

# 2. Scale dữ liệu
print("Đang scale dữ liệu...")
scaler = MinMaxScaler()
data_scaled = scaler.fit_transform(features_df)
joblib.dump(scaler, SCALER_PATH)
print(f"Đã lưu scaler vào '{SCALER_PATH}'")

# 3. Tạo chuỗi dữ liệu
print("Đang tạo chuỗi dữ liệu (có thể mất nhiều thời gian)...")
X_autoencoder, X_forecaster, y_forecaster = [], [], []
# Giảm bước nhảy để học chi tiết hơn
for i in range(0, len(data_scaled) - SEQUENCE_LENGTH, 5):
    X_autoencoder.append(data_scaled[i:i + SEQUENCE_LENGTH])
    X_forecaster.append(data_scaled[i:i + SEQUENCE_LENGTH - 1])
    y_forecaster.append(data_scaled[i + SEQUENCE_LENGTH - 1])
X_autoencoder = np.array(X_autoencoder)
X_forecaster = np.array(X_forecaster)
y_forecaster = np.array(y_forecaster)
print(f"Đã tạo {len(X_autoencoder)} chuỗi dữ liệu để huấn luyện.")

# --- 4. Huấn luyện Mô hình A: AUTOENCODER (Chuyên gia Lọc nhiễu) ---
print("\n--- Huấn luyện Autoencoder ---")
# Tăng độ phức tạp của mô hình để xử lý nhiễu
ae_inputs = Input(shape=(SEQUENCE_LENGTH, N_FEATURES))
ae_encoded = LSTM(128, activation='relu', return_sequences=True)(ae_inputs)
ae_encoded = LSTM(64, activation='relu')(ae_encoded)
ae_decoded = RepeatVector(SEQUENCE_LENGTH)(ae_encoded)
ae_decoded = LSTM(64, activation='relu', return_sequences=True)(ae_decoded)
ae_decoded = LSTM(N_FEATURES, activation='linear', return_sequences=True)(ae_decoded)
autoencoder = Model(ae_inputs, ae_decoded)
autoencoder.compile(optimizer='adam', loss=tf.keras.losses.Huber())
autoencoder.fit(X_autoencoder, X_autoencoder, epochs=25, batch_size=256, validation_split=0.1, callbacks=[tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=4)])
autoencoder.save(AUTOENCODER_H5_PATH)

# --- 5. Huấn luyện Mô hình B: FORECASTER (Chuyên gia Phân biệt) ---
print("\n--- Huấn luyện Forecaster ---")
fc_inputs = Input(shape=(SEQUENCE_LENGTH - 1, N_FEATURES))
fc_lstm_1 = LSTM(128, activation='relu', return_sequences=True)(fc_inputs)
fc_dropout = Dropout(0.3)(fc_lstm_1)
fc_lstm_2 = LSTM(64, activation='relu')(fc_dropout)
fc_outputs = Dense(N_FEATURES, activation='linear')(fc_lstm_2)
forecaster = Model(fc_inputs, fc_outputs)
forecaster.compile(optimizer='adam', loss='mae')
forecaster.fit(X_forecaster, y_forecaster, epochs=25, batch_size=256, validation_split=0.1, callbacks=[tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=4)])
forecaster.save(FORECASTER_H5_PATH)

# --- 6. Chuyển đổi sang TFLite ---
print("\n--- Đang chuyển đổi mô hình sang định dạng TFLite tối ưu...")
converter_ae = tf.lite.TFLiteConverter.from_keras_model(autoencoder)
converter_ae.optimizations = [tf.lite.Optimize.DEFAULT]
tflite_autoencoder_model = converter_ae.convert()
with open(AUTOENCODER_TFLITE_PATH, 'wb') as f:
    f.write(tflite_autoencoder_model)
print(f"-> Đã lưu Autoencoder TFLite vào '{AUTOENCODER_TFLITE_PATH}'")

converter_fc = tf.lite.TFLiteConverter.from_keras_model(forecaster)
converter_fc.optimizations = [tf.lite.Optimize.DEFAULT]
tflite_forecaster_model = converter_fc.convert()
with open(FORECASTER_TFLITE_PATH, 'wb') as f:
    f.write(tflite_forecaster_model)
print(f"-> Đã lưu Forecaster TFLite vào '{FORECASTER_TFLITE_PATH}'")

os.remove(AUTOENCODER_H5_PATH)
os.remove(FORECASTER_H5_PATH)

end_time = time.time()
print(f"\nQUY TRÌNH SẢN XUẤT MÔ HÌNH VỮNG VÀNG HOÀN TẤT trong {end_time - start_time:.2f} giây.")
