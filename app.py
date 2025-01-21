import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Your API key
api_key_weather = os.getenv("API_KEY")

# Base URL for WeatherAPI
base_url = "http://api.weatherapi.com/v1"
# Location or Postal Code for WeatherAPI
location = 'London'
# Endpoint for WeatherAPI
endpoint = '/marine.json'

# Construct the URL with f-string
url = f"{base_url}{endpoint}?key={api_key_weather}&q={location}"
print(url)


response_json = requests.get(url)
data = response_json.json()
print(data)

# Function to call different endpoints if needed (marine/json & forecast/json)

#def fetch_weather_data(endpoint, params):
    #url = f"{BASE_URL}/{endpoint}"
        
    #response = requests.get(url, params=params)
        
    #response_json = print(response_json.json())
        
    #if response.status_code == 200:
        #return response_json
    #else:
        #print("Error fetching error: {}".format(response.status_code))
        #return None


# ------------------------------ LOAD DATA INTO DATABASE ---------------------------------------
import psycopg2
import os
from dotenv import load_dotenv
import requests
from datetime import datetime

# Load environment variables
load_dotenv()
api_key_weather = os.getenv("API_KEY")


# Parameters
db_user = os.getenv("SOURCE_DB_USER")
db_password = os.getenv("SOURCE_DB_PASSWORD")
db_host = os.getenv("SOURCE_DB_HOST")
db_port = os.getenv("SOURCE_DB_PORT")
db_name = os.getenv("SOURCE_DB_NAME")
    
# Set up the connection
try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    # Create a cursor object to interact with the database
    cursor = conn.cursor()

except Exception as e:
    print(f"Error connecting to database: {e}")
    
    
create_table_query = """
CREATE TABLE IF NOT EXISTS student.vedi_marine_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    region VARCHAR(255),
    country VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    timezone_id VARCHAR(100),
    localtime_epoch BIGINT,
    temp_c FLOAT,
    condition_text VARCHAR(100),
    wind_kph FLOAT,
    wind_dir VARCHAR(10),
    wave_height_m FLOAT,
    tide_time TIMESTAMP,
    tide_height_m FLOAT,
    water_temp_c FLOAT,
    visibility_miles FLOAT,
    uv_index FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
    
cursor.execute(create_table_query)
conn.commit() 
    
#Another API request for marine and tide data

base_url = "http://api.weatherapi.com/v1"

location = 'London'

endpoint = '/marine.json'

# Construct the URL with f-string
url = f"{base_url}{endpoint}?key={api_key_weather}&q={location}"
print(url)

response = response.get(url)
response_data = response_json.json()


# Parse location data
location_data = response_data["location"]
city = location_data["name"]
region = location_data["region"]
country = location_data["country"]
latitude = location_data["lat"]
longitude = location_data["lon"]
timezone_id = location_data["tz_id"]
localtime_epoch = location_data["localtime_epoch"]

# Parse daily marine data
forecast_data = response_data["forecast"]["forecastday"][0]
day_data = forecast_data["day"]
temp_c = day_data["avgtemp_c"]
condition_text = day_data["condition"]["text"]
wind_kph = day_data["maxwind_kph"]
wind_dir = None  # No daily wind direction available
wave_height_m = None  
visibility_miles = day_data["avgvis_m"] * 0.621371  
uv_index = day_data["uv"]

# Assuming the forecast_data always contains tide data
tide_data = forecast_data.get("astro", {}).get("tide", [])
# Extract tide time and height from the first item
tide_time = datetime.strptime(tide_data[0]["time"], "%H:%M")  
tide_height_m = tide_data[0]["height_m"]  



data = (
    response_data['location']['name'],
    response_data['location']['region'],
    response_data['location']['country'],
    response_data['location']['lat'],
    response_data['location']['long'],
    response_data['location']['tz_id'],
    response_data['location']['localtime_epoch'],
    response_data['current']['temp_c'],
    response_data['current']['condition']['text'],
    response_data['current']['wind_mph'],
    response_data['current']['wind_dir'],
    response_data['current']['pressure_mb'],
    response_data['current']['precip_mm'],
    response_data['current']['humidity'],
    response_data['current']['feelslike_c'],
    response_data['current']['visibility_miles'],
    response_data['current']['uv_index']
    reponse_data['current']['tidal_time']
    reponse_data['current']['tidal_height_m']
)

# Insert 
insert_stmt = """
INSERT INTO student.vedi_marine_data (
    city, region, country, latitude, longitude, timezone_id, localtime_epoch,
    temp_c, condition_text, wind_kph, wind_dir, wave_height_m,
    tide_time, tide_height_m, water_temp_c, visibility_miles, uv_index
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Execute the insertion
cursor.execute(insert_stmt, data)
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Data inserted successfully.")


