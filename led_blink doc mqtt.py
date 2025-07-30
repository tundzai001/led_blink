# generate_geotech_data_V3_Robust.py
import numpy as np
import pandas as pd

print("Bắt đầu tạo bộ dữ liệu huấn luyện THỬ THÁCH TỔNG HỢP...")

# --- CẤU HÌNH ---
N_SCENARIOS = 30  # Tăng số lượng kịch bản để đa dạng hơn
SAMPLES_PER_SCENARIO = 86400 * 2 # Mỗi kịch bản dài 2 ngày
BASE_LAT, BASE_LON, BASE_H = 21.0739, 105.7770, 25.0
BASE_WATER_LEVEL = -10.0

# --- SINH DỮ LIỆU ---
all_dfs = []
total_samples = 0

for i in range(N_SCENARIOS):
    print(f" -> Đang tạo Kịch bản Thử thách #{i+1}/{N_SCENARIOS}...")
    
    time_vector = np.arange(SAMPLES_PER_SCENARIO)
    
    # 1. Mô phỏng Nước ngầm và Dịch chuyển Vật lý (như cũ)
    rise_duration = int(SAMPLES_PER_SCENARIO * np.random.uniform(0.5, 0.8))
    stable_duration = SAMPLES_PER_SCENARIO - rise_duration
    water_rise = (1 / (1 + np.exp(- (np.linspace(-6, 6, rise_duration))))) * np.random.uniform(7, 9)
    water_stable = np.full(stable_duration, water_rise[-1]) + np.random.normal(0, 0.05, stable_duration)
    water_levels = BASE_WATER_LEVEL + np.concatenate([water_rise, water_stable])
    
    consolidation_strain = (water_levels - BASE_WATER_LEVEL) * np.random.uniform(-0.005, -0.01)
    water_pressure_factor = np.maximum(0, water_levels - (-3.0))
    creep_velocity = water_pressure_factor * np.random.uniform(1e-9, 5e-9)
    creep_displacement = np.cumsum(creep_velocity)
    
    hs = BASE_H + consolidation_strain + creep_displacement
    lat_creep = np.cumsum(creep_velocity * np.random.uniform(0.1, 0.3)) / 111111
    lon_creep = np.cumsum(creep_velocity * np.random.uniform(0.1, 0.3)) / 111111
    lats = BASE_LAT + lat_creep
    lons = BASE_LON + lon_creep
    
    # 2. THÊM CÁC THỬ THÁCH PHỨC TẠP
    
    # Thử thách A: Nhiễu địa chấn Tạm thời (Xe tải, Xây dựng)
    for _ in range(np.random.randint(5, 15)): # 5-15 sự kiện rung động mỗi kịch bản
        start = np.random.randint(0, SAMPLES_PER_SCENARIO - 300)
        duration = np.random.randint(60, 300) # Kéo dài 1-5 phút
        # Tạo một rung động tần số cao, tắt dần
        t = np.arange(duration)
        seismic_noise = (np.sin(t * 0.5) + np.sin(t * 1.5)) * np.exp(-t/100) * 0.008 # Rung động 8mm
        hs[start:start+duration] += seismic_noise

    # Thử thách B: Hiệu ứng Nhiệt độ Ngày-Đêm
    thermal_effect = -np.cos(2 * np.pi * time_vector / 86400) * 0.003 # Co giãn 3mm
    hs += thermal_effect
    
    # Thử thách C: Lỗi Cảm biến Tạm thời (Glitches)
    for _ in range(np.random.randint(1, 4)): # 1-4 lỗi mỗi kịch bản
        # Lỗi GNSS nhảy vọt
        idx = np.random.randint(0, SAMPLES_PER_SCENARIO)
        hs[idx] += np.random.uniform(-0.1, 0.1) # Nhảy vọt 10cm
        
        # Lỗi cảm biến nước bị kẹt
        start_kẹt = np.random.randint(0, SAMPLES_PER_SCENARIO - 600)
        duration_kẹt = np.random.randint(300, 600) # Kẹt trong 5-10 phút
        water_levels[start_kẹt:start_kẹt+duration_kẹt] = water_levels[start_kẹt]

    # Cuối cùng, thêm nhiễu RTK có độ chính xác cao
    lats += np.random.normal(0, 0.00000002, SAMPLES_PER_SCENARIO)
    lons += np.random.normal(0, 0.00000002, SAMPLES_PER_SCENARIO)
    hs += np.random.normal(0, 0.005, SAMPLES_PER_SCENARIO)

    # Tạo DataFrame cho kịch bản này
    df = pd.DataFrame({'lat': lats, 'lon': lons, 'h': hs, 'water_level': water_levels})
    all_dfs.append(df)
    total_samples += len(df)

print("Đang kết hợp các kịch bản và lưu file...")
final_df = pd.concat(all_dfs, ignore_index=True)
final_df['timestamp'] = np.arange(len(final_df)) + 1753872000
final_df.rename(columns={'h': 'height'}, inplace=True) # Đổi tên cột cho nhất quán
final_df = final_df[['timestamp', 'lat', 'lon', 'height', 'water_level']]

final_df.to_csv('normal_data_robust.csv', index=False)
print(f"Hoàn tất! Đã tạo 'normal_data_robust.csv' với {total_samples} dòng dữ liệu.")
print("File này chứa các kịch bản phức tạp để huấn luyện một AI vững vàng.")
