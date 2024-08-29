import rasterio
import numpy as np
from datetime import datetime, timedelta
import os
from scipy.interpolate import griddata
import glob

def create_hotspots(shape, num_hotspots):
    hotspots = []
    for _ in range(num_hotspots):
        x = np.random.randint(0, shape[1])
        y = np.random.randint(0, shape[0])
        hotspots.append((x, y))
    return hotspots

def interpolate_data(shape, hotspots, values, index):
    grid_x, grid_y = np.mgrid[0:shape[1], 0:shape[0]]
    points = np.array(hotspots)
    
    # Definiere Wertebereiche und typische Verteilungen für verschiedene Indizes
    ranges = {
        'temperature': (0, 40, 'normal', 20, 5),
        'bui': (0, 100, 'uniform', 0, 100),
        'dc': (0, 800, 'uniform', 0, 800),
        'dmc': (0, 100, 'uniform', 0, 100),
        'dsr': (0, 30, 'uniform', 0, 30),
        'ffmc': (0, 100, 'beta', 2, 2),
        'fwi': (0, 50, 'gamma', 2, 1),
        'gfmc': (0, 100, 'beta', 2, 2),
        'isi': (0, 50, 'gamma', 2, 1),
        'mixr': (0, 20, 'normal', 10, 3),
        'precipitation': (0, 50, 'exponential', 5),
        'radiation': (0, 1000, 'gamma', 2, 200),
        'relative_humidity': (0, 100, 'beta', 2, 2),
        'sdmc': (0, 100, 'uniform', 0, 100),
        't_msl': (900, 1100, 'normal', 1000, 30),
        'wind_speed': (0, 30, 'weibull', 2, 5)
    }
    
    min_val, max_val, dist_type, *params = ranges[index]
    
    if dist_type == 'normal':
        values = np.random.normal(*params, size=len(hotspots))
    elif dist_type == 'uniform':
        values = np.random.uniform(*params, size=len(hotspots))
    elif dist_type == 'beta':
        values = np.random.beta(*params, size=len(hotspots))
    elif dist_type == 'gamma':
        values = np.random.gamma(*params, size=len(hotspots))
    elif dist_type == 'exponential':
        values = np.random.exponential(*params, size=len(hotspots))

    
    scaled_values = np.clip(values, min_val, max_val)
    
    interpolated = griddata(points, scaled_values, (grid_x, grid_y), method='cubic', fill_value=np.mean(scaled_values))
    
    return np.clip(interpolated, min_val, max_val)

def create_sample_tifs(template_file, output_dir, start_date, num_days):
    indizes = [
        'temperature', 'bui', 'dc', 'dmc', 'dsr', 'ffmc', 'fwi', 'gfmc', 'isi',
        'mixr', 'precipitation', 'radiation', 'relative_humidity', 'sdmc',
        't_msl', 'wind_speed'
    ]

    with rasterio.open(template_file) as src:
        profile = src.profile
        shape = src.shape

    current_date = datetime.strptime(start_date, "%Y%m%d")
    
    for _ in range(num_days):
        for index in indizes:
            # Erstelle Hotspots für jeden Index separat
            num_hotspots = np.random.randint(10, 30)
            hotspots = create_hotspots(shape, num_hotspots)
            
            # Interpoliere Daten für jeden Index
            data = interpolate_data(shape, hotspots, np.random.rand(num_hotspots), index)
            
            # Runde auf eine Dezimalstelle
            data = np.round(data, decimals=1)
            
            # Konvertiere in den richtigen Datentyp
            data = data.astype(rasterio.float32)
            
            # Erstelle den Dateinamen
            filename = f"{index}{current_date.strftime('%Y%m%d')}.tif"
            filepath = os.path.join(output_dir, filename)
            
            # Aktualisiere das Profil für float32 Datentyp
            profile_updated = profile.copy()
            profile_updated.update(dtype=rasterio.float32, count=1)
            
            # Speichere die Daten als GeoTIFF
            with rasterio.open(filepath, 'w', **profile_updated) as dst:
                dst.write(data, 1)
            
            print(f"Erstellt: {filename}")
        
        # Gehe zum nächsten Tag
        current_date += timedelta(days=1)

# Beispielaufruf
template_file = "demofile.tif"
output_dir = "output"
start_date = str(datetime.now().strftime("%Y%m%d"))
# start_date = "20240825"
print(start_date)
num_days = 1

# Stelle sicher, dass das Ausgabeverzeichnis existiert
os.makedirs(output_dir, exist_ok=True)

# Remove all .tif files in the output directory
tif_files = glob.glob(os.path.join(output_dir, "*.tif"))
for file in tif_files:
    os.remove(file)

create_sample_tifs(template_file, output_dir, start_date, num_days)