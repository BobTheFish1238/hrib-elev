# Go to 'https://www.inaturalist.org/observations/export'
# Search the state and taxon
# Set 'Geoprivacy' and 'Taxon Geoprivacy' to open
# Disable all columns except url, created_at, observed_on, latitude, and longitude
# Eg. quality_grade=any&identifications=any&geoprivacy=open&taxon_geoprivacy=open&place_id=46&taxon_id=129328 Columns observed_on, created_at, url, latitude, longitude 
# Export and extract the zip
# Paste the extracted folder (eg. observations-665684) into the \Hrib-Habitat\ObservationProcessor\Observations\
# Run this python script with 'python "C:\Users\bobth\Documents\Hrib-Habitat\ObservationProcessor\HribHabitat_csv-ndjson.py"' (make sure path uses your username)

import csv
import json
import math
import os
import re
import shutil
import time
from datetime import datetime, timedelta
from io import BytesIO

import requests
from PIL import Image
from tqdm import tqdm

# =====================================================
# ROOT PATHS
# =====================================================

ROOT_OBSERVATIONS = r"C:\Users\bobth\Documents\Hrib-Habitat\ObservationProcessor\Observations"
COMPLETED_ROOT = r"C:\Users\bobth\Documents\Hrib-Habitat\ObservationProcessor\Completed"

TIMEZONE = "America/Los_Angeles"
API_DELAY = 0.1
DAYS = 14
ZOOM_LEVEL = 15

AWS_TERRAIN_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
METERS_TO_FEET = 3.28084
TILE_SIZE = 256

# =====================================================
# LOOKUPS (extend as needed)
# =====================================================

PLACE_MAP = {
    "46": "Washington",
    "10": "Oregon",
    "14": "California",
}

TAXON_MAP = {
    # Boletus
    "48701": "Boletus_edulis",
    "438025": "Boletus_rex-veris",
    "118150": "Boletus_fibrillosus",
    "129328": "Boletus_barrowsii",

    # Black Morels
    "133686": "Morchella_angusticeps",
    "487375": "Morchella_importuna",
    "492428": "Morchella_brunnea",
    "1063010": "Morchella_norvegiensis",
    "500023": "Morchella_populiphila",
    "473933": "Morchella_snyderi",
    "501698": "Morchella_tomentosa",
    "500004": "Morchella_tridentina",
}

SOIL_VARS = [
    "soil_temperature_0_to_7cm_mean",
    "soil_temperature_7_to_28cm_mean",
    "soil_temperature_28_to_100cm_mean",
    "soil_temperature_0_to_100cm_mean",
    "soil_moisture_0_to_7cm_mean",
    "soil_moisture_7_to_28cm_mean",
    "soil_moisture_28_to_100cm_mean",
    "soil_moisture_0_to_100cm_mean",
]

# =====================================================
# README PARSING
# =====================================================

def parse_readme(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        text = f.read()

    place_id = re.search(r"place_id=(\d+)", text)
    taxon_id = re.search(r"taxon_id=(\d+)", text)

    place_id = place_id.group(1) if place_id else "unknown"
    taxon_id = taxon_id.group(1) if taxon_id else "unknown"

    state = PLACE_MAP.get(place_id, f"place_{place_id}")
    species = TAXON_MAP.get(taxon_id, f"taxon_{taxon_id}")

    return state, species

# =====================================================
# OPEN-METEO
# =====================================================

def get_elevation(lat, lon):
    url = f"https://api.open-meteo.com/v1/elevation?latitude={lat}&longitude={lon}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return round(r.json()["elevation"][0] * METERS_TO_FEET)

def fetch_soil_data(lat, lon, end_date):
    start_date = end_date - timedelta(days=DAYS - 1)
    vars_csv = ",".join(SOIL_VARS)

    api_url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily={vars_csv}&timezone={TIMEZONE}"
        f"&temperature_unit=fahrenheit"
    )

    r = requests.get(api_url, timeout=30)
    r.raise_for_status()
    return r.json(), api_url

# =====================================================
# TERRAIN TILE MATH
# =====================================================

def latlon_to_pixel(lat, lon, z):
    lat_rad = math.radians(lat)
    n = 2 ** z
    x = (lon + 180.0) / 360.0 * n * TILE_SIZE
    y = (1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n * TILE_SIZE
    return int(x), int(y)

def decode_terrarium(r, g, b):
    return (r * 256 + g + b / 256) - 32768

def fetch_tile(z, x, y, cache):
    key = (z, x, y)
    if key in cache:
        return cache[key]

    url = AWS_TERRAIN_URL.format(z=z, x=x, y=y)
    r = requests.get(url, timeout=10)
    r.raise_for_status()

    img = Image.open(BytesIO(r.content)).convert("RGB")
    cache[key] = img
    return img

def get_elevation_grid(lat, lon, z, cache):
    px, py = latlon_to_pixel(lat, lon, z)
    tx, ty = px // TILE_SIZE, py // TILE_SIZE
    ix, iy = px % TILE_SIZE, py % TILE_SIZE

    grid = [[0]*3 for _ in range(3)]

    for dy in range(-1, 2):
        for dx in range(-1, 2):
            x, y = ix + dx, iy + dy
            ttx, tty = tx, ty

            if x < 0:
                ttx -= 1; x += TILE_SIZE
            if y < 0:
                tty -= 1; y += TILE_SIZE
            if x >= TILE_SIZE:
                ttx += 1; x -= TILE_SIZE
            if y >= TILE_SIZE:
                tty += 1; y -= TILE_SIZE

            tile = fetch_tile(z, ttx, tty, cache)
            r, g, b = tile.load()[x, y]
            grid[dy+1][dx+1] = decode_terrarium(r, g, b)

    return grid

def calculate_slope_aspect(e):
    dzdx = ((e[0][2] + 2*e[1][2] + e[2][2]) - (e[0][0] + 2*e[1][0] + e[2][0])) / 8
    dzdy = ((e[2][0] + 2*e[2][1] + e[2][2]) - (e[0][0] + 2*e[0][1] + e[0][2])) / 8

    slope = math.degrees(math.atan(math.sqrt(dzdx**2 + dzdy**2)))
    aspect = math.degrees(math.atan2(dzdy, -dzdx))
    if aspect < 0:
        aspect += 360

    return round(slope, 2), round(aspect, 1)

# =====================================================
# MAIN PIPELINE
# =====================================================

for folder in os.listdir(ROOT_OBSERVATIONS):
    folder_path = os.path.join(ROOT_OBSERVATIONS, folder)
    if not folder.startswith("observations-"):
        continue

    print(f"\n📁 Processing {folder}")

    readme = os.path.join(folder_path, "README.txt")
    csv_path = os.path.join(folder_path, f"{folder}.csv")

    if not os.path.exists(readme) or not os.path.exists(csv_path):
        print("⚠️ Missing README or CSV, skipping.")
        continue

    state, species = parse_readme(readme)
    label = f"{state}_{species}"

    ndjson_path = os.path.join(folder_path, f"{label}.ndjson")

    tile_cache = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    with open(ndjson_path, "w", encoding="utf-8") as fout, tqdm(total=len(rows), desc=label) as pbar:
        for row in rows:
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                date_str = row["observed_on"] or row["created_at"].split(" ")[0]
                obs_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                elevation_ft = get_elevation(lat, lon)
                soil, api_url = fetch_soil_data(lat, lon, obs_date)

                grid = get_elevation_grid(lat, lon, ZOOM_LEVEL, tile_cache)
                aws_ft = round(grid[1][1] * METERS_TO_FEET)
                slope, aspect = calculate_slope_aspect(grid)

                record = {
                    "observation_url": row["url"],
                    "date_used": date_str,
                    "coordinates": f"{lat}, {lon}",
                    "elevation_ft": elevation_ft,
                    "dates": soil["daily"]["time"],

                    "soil_temperature": {
                        k.replace("soil_temperature_", "").replace("_mean", ""): soil["daily"].get(k, [])
                        for k in SOIL_VARS
                        if k.startswith("soil_temperature_")
                    },

                    "soil_moisture": {
                        k.replace("soil_moisture_", "").replace("_mean", ""): soil["daily"].get(k, [])
                        for k in SOIL_VARS
                        if k.startswith("soil_moisture_")
                    },

                    "open_meteo_api_url": api_url,
                    "aws_elevation_ft": aws_ft,
                    "slope_deg": slope,
                    "slope_aspect_deg": aspect,
                }

                fout.write(json.dumps(record) + "\n")

            except Exception as e:
                tqdm.write(f"⚠️ Failed row: {e}")

            time.sleep(API_DELAY)
            pbar.update(1)

    completed_name = f"{folder} ({label})"
    shutil.move(folder_path, os.path.join(COMPLETED_ROOT, completed_name))
    print(f"✅ Moved to Completed → {completed_name}")

print("\n🎉 ALL OBSERVATIONS PROCESSED")
