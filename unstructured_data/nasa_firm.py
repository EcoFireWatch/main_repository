import requests
from PIL import Image
import math
import boto3
import uuid
from decimal import Decimal
from datetime import datetime

# ==================== CONFIGURAÇÕES ====================
map_key = "a13967d84366e23da9095779b7233aa4"
layer = "fires_viirs_24"
image_width = 500
image_height = 500

# BBOX: minLon, minLat, maxLon, maxLat
min_lon = -44.42
min_lat = -19.16
max_lon = -43.42
max_lat = -18.16

output_file = f"nasa_{layer}.png"

# ==================== BAIXA IMAGEM ====================
base_url = "https://firms.modaps.eosdis.nasa.gov/mapserver/wms/fires"
url = (
    f"{base_url}/{map_key}/{layer}/"
    f"?REQUEST=GetMap"
    f"&WIDTH={image_width}"
    f"&HEIGHT={image_height}"
    f"&BBOX={min_lon},{min_lat},{max_lon},{max_lat}"
    f"&SRS=EPSG:4326"
    f"&FORMAT=image/png"
    f"&TRANSPARENT=true"
)

print("🔄 Baixando imagem...")
resp = requests.get(url, timeout=30)
if resp.status_code != 200:
    print(f"Erro ao baixar imagem ({resp.status_code}). URL usada:\n{url}")
    raise SystemExit(1)

with open(output_file, "wb") as f:
    f.write(resp.content)
print(f"Imagem salva como '{output_file}'")

# ==================== PROCURA PIXEL VERMELHO ====================
img = Image.open(output_file).convert("RGB")
pixels = img.load()

center_x = image_width // 2
center_y = image_height // 2

min_distance_px = float("inf")
closest_pixel = None

RED_THRESHOLD = 150

for y in range(img.height):
    for x in range(img.width):
        r, g, b = pixels[x, y]
        if r > RED_THRESHOLD and r > g * 2 and r > b * 2:
            dist_px = math.hypot(x - center_x, y - center_y)
            if dist_px < min_distance_px:
                min_distance_px = dist_px
                closest_pixel = (x, y)

if not closest_pixel:
    print("Nenhum pixel vermelho encontrado na imagem.")
    raise SystemExit(0)

print(f"Pixel vermelho mais próximo: {closest_pixel} (distância em pixels = {min_distance_px:.2f})")

# ==================== FUNÇÃO HAVERSINE ====================
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088  # raio médio da Terra em km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ==================== CONVERSÃO PIXEL -> LON/LAT ====================
def pixel_to_lonlat(x_pixel, y_pixel, min_lon, min_lat, max_lon, max_lat, img_w, img_h):
    x_center = x_pixel + 0.5
    y_center = y_pixel + 0.5
    lon = min_lon + (x_center / img_w) * (max_lon - min_lon)
    lat = max_lat - (y_center / img_h) * (max_lat - min_lat)
    return lon, lat

center_lon, center_lat = pixel_to_lonlat(center_x, center_y, min_lon, min_lat, max_lon, max_lat, image_width, image_height)
px_x, px_y = closest_pixel
pixel_lon, pixel_lat = pixel_to_lonlat(px_x, px_y, min_lon, min_lat, max_lon, max_lat, image_width, image_height)

distance_km = haversine_km(center_lat, center_lon, pixel_lat, pixel_lon)

print(f"Centro (lon, lat): ({center_lon:.6f}, {center_lat:.6f})")
print(f"Pixel vermelho (lon, lat): ({pixel_lon:.6f}, {pixel_lat:.6f})")
print(f"Distância geográfica: {distance_km:.3f} km")

# ==================== ENVIA PARA DYNAMODB ====================
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamodb.Table("efw-unstructured-data")

item_id = str(uuid.uuid4())
item = {
    "id": item_id,
    "distance_km": Decimal(f"{distance_km:.2f}"),
    "date_insert": datetime.now().isoformat(),
    "type": "NASA FIRM"
}

response = table.put_item(Item=item)
print("✅ Dado enviado para DynamoDB:", response)
