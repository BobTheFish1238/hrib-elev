# Go to 'https://www.inaturalist.org/observations/export'
# Search the state and taxon
# Set 'Geoprivacy' and 'Taxon Geoprivacy' to open
# Disable all columns except url, created_at, observed_on, quality_grade, latitude, longitude, positional_accuracy, place_state_name, scientific_name, common_name, taxon_id
# Eg. quality_grade=any&identifications=any&geoprivacy=open&taxon_geoprivacy=open&place_id=46&taxon_id=129328 Columns observed_on, created_at, url, quality_grade, latitude, longitude, positional_accuracy, place_state_name, scientific_name, common_name, taxon_id  
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
from pathlib import Path

import requests
from PIL import Image
from tqdm import tqdm

# =====================================================
# ROOT PATHS
# =====================================================

ROOT_OBSERVATIONS = r"C:\Users\bobth\Documents\Hrib-Habitat\ObservationProcessorV2\Observations"
NDJSON2_ROOT = r"C:\Users\bobth\Documents\Hrib-Habitat\ndjson2\Morels"

TIMEZONE = "America/Los_Angeles"
API_DELAY = 0.1
DAYS = 14
ZOOM_LEVEL = 15

AWS_TERRAIN_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
METERS_TO_FEET = 3.28084
TILE_SIZE = 256

# =====================================================
# SOIL VARIABLES
# =====================================================

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
# HELPER FUNCTIONS
# =====================================================

def get_folder_name_from_scientific_name(scientific_name, common_name):
    """
    Determine the folder name based on scientific name and common name.
    This follows the pattern: "Common Name (Scientific Name)" or just "Scientific Name" if no common name
    """
    if common_name and common_name != "true morels" and common_name != "true morels" and common_name.strip():
        return f"{common_name} ({scientific_name})"
    else:
        # Just use the scientific name without adding "(Genus)"
        return scientific_name

def extract_ids_from_readme(readme_path):
    """Extract place_id and taxon_id from README.txt"""
    with open(readme_path, "r", encoding="utf-8") as f:
        text = f.read()
    
    place_id = re.search(r"place_id=(\d+)", text)
    taxon_id = re.search(r"taxon_id=(\d+)", text)
    
    place_id = place_id.group(1) if place_id else None
    taxon_id = taxon_id.group(1) if taxon_id else None
    
    return place_id, taxon_id

def get_species_folder_name_from_row(row):
    """
    Get the species folder name from the CSV row data
    """
    scientific_name = row.get("scientific_name", "").strip()
    common_name = row.get("common_name", "").strip()
    
    # Handle empty or missing values
    if not scientific_name or scientific_name == "Morchella":
        # For genus-level observations, just use "Morchella"
        return "Morchella"
    
    return get_folder_name_from_scientific_name(scientific_name, common_name)

def find_existing_observations(ndjson2_root, place_state_name, species_folder, filename):
    """
    Check if an observation file already exists and return existing URLs
    """
    state_folder = place_state_name if place_state_name else "Unknown"
    species_path = os.path.join(ndjson2_root, species_folder)
    ndjson_path = os.path.join(species_path, filename)
    
    existing_urls = set()
    
    if os.path.exists(ndjson_path):
        with open(ndjson_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if "observation_url" in data:
                        existing_urls.add(data["observation_url"])
                except:
                    continue
    
    return existing_urls, ndjson_path

def ensure_directory_exists(path):
    """Create directory if it doesn't exist"""
    os.makedirs(path, exist_ok=True)

def make_request_with_retry(url, max_retries=3, delay=2):
    """Make HTTP request with retry on failure"""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r
        except (requests.exceptions.SSLError, requests.exceptions.Timeout, 
                requests.exceptions.ConnectionError) as e:
            if attempt == max_retries - 1:  # Last attempt
                raise e
            print(f"    ⚠️ Request failed, retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)
    return None

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
    csv_path = os.path.join(folder_path, folder)

    if not os.path.exists(readme) or not os.path.exists(csv_path):
        print("⚠️ Missing README or CSV, skipping.")
        continue

    # Get place_id and taxon_id from README (for reference only now)
    place_id, taxon_id = extract_ids_from_readme(readme)
    
    # Read CSV to get place_state_name and species information from each row
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    
    # Group observations by state and species
    observations_by_group = {}
    
    for row in rows:
        place_state_name = row.get("place_state_name", "").strip()
        if not place_state_name:
            continue  # Skip rows without state information
        
        species_folder = get_species_folder_name_from_row(row)
        scientific_name = row.get("scientific_name", "").strip()
        
        # Handle genus-level or missing scientific names
        if scientific_name == "Morchella" or not scientific_name:
            scientific_part = "Morchella"
            common_part = "Genus"
            filename = f"{place_state_name}_Morchella.ndjson"
        else:
            # Extract the species part for filename
            species_part = scientific_name.split()[-1] if len(scientific_name.split()) > 1 else scientific_name
            filename = f"{place_state_name}_{scientific_name.replace(' ', '_')}.ndjson"
        
        # Create group key
        group_key = f"{place_state_name}||{species_folder}"
        
        if group_key not in observations_by_group:
            observations_by_group[group_key] = {
                "place_state_name": place_state_name,
                "species_folder": species_folder,
                "filename": filename,
                "rows": [],
                "existing_urls": set()
            }
        
        observations_by_group[group_key]["rows"].append(row)
    
    # Process each group separately
    for group_key, group_info in observations_by_group.items():
        place_state_name = group_info["place_state_name"]
        species_folder = group_info["species_folder"]
        filename = group_info["filename"]
        
        print(f"\n  📍 Processing {place_state_name} - {species_folder}")
        
        # Create target directory path
        target_dir = os.path.join(NDJSON2_ROOT, species_folder)
        ensure_directory_exists(target_dir)
        
        # Check for existing observations
        ndjson_path = os.path.join(target_dir, filename)
        existing_urls = set()
        
        if os.path.exists(ndjson_path):
            with open(ndjson_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        if "observation_url" in data:
                            existing_urls.add(data["observation_url"])
                    except:
                        continue
            
            print(f"    📊 Found {len(existing_urls)} existing observations")
            
            # Open file in append mode
            fout = open(ndjson_path, "a", encoding="utf-8")
        else:
            # Create new file
            fout = open(ndjson_path, "w", encoding="utf-8")
        
        tile_cache = {}
        new_observations_count = 0
        skipped_count = 0
        
        # Process rows for this group
        with tqdm(total=len(group_info["rows"]), desc=f"    {place_state_name[:10]}...", leave=False) as pbar:
            for row in group_info["rows"]:
                observation_url = row["url"]
                
                # Skip if observation already exists
                if observation_url in existing_urls:
                    skipped_count += 1
                    pbar.update(1)
                    continue
                
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

                    quality_grade = row.get("quality_grade", "").strip() or "NA"

                    positional_accuracy = row.get("positional_accuracy", "").strip()
                    positional_accuracy = int(positional_accuracy) if positional_accuracy.isdigit() else 0

                    record = {
                        "observation_url": observation_url,
                        "quality_grade": quality_grade,
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

                        "positional_accuracy": positional_accuracy,
                    }

                    fout.write(json.dumps(record) + "\n")
                    fout.flush()  # Ensure it's written immediately
                    new_observations_count += 1

                except Exception as e:
                    tqdm.write(f"    ⚠️ Failed row: {e}")

                time.sleep(API_DELAY)
                pbar.update(1)
        
        fout.close()
        
        print(f"    ✅ Added {new_observations_count} new observations, skipped {skipped_count} existing")
        print(f"    📁 Saved to: {ndjson_path}")
    
    # After processing all groups, move the original folder to a backup location
    backup_folder = r"C:\Users\bobth\Documents\Hrib-Habitat\ObservationProcessor\Processed"
    ensure_directory_exists(backup_folder)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{folder}_processed_{timestamp}"
    shutil.move(folder_path, os.path.join(backup_folder, backup_name))
    print(f"📦 Moved original folder to: {backup_folder}\\{backup_name}")

print("\n🎉 ALL OBSERVATIONS PROCESSED")
