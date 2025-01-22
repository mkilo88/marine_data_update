import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
api_key_weather = os.getenv("API_KEY")


# .env variables
db_user = os.getenv("SOURCE_DB_USER")
db_password = os.getenv("SOURCE_DB_PASSWORD")
db_host = os.getenv("SOURCE_DB_HOST")
db_port = os.getenv("SOURCE_DB_PORT")
db_name = os.getenv("SOURCE_DB_NAME")


# Base URL for WeatherAPI
base_url = "http://api.weatherapi.com/v1"
# Location or Postal Code for WeatherAPI
location = 'Manitoba'
# Endpoint for WeatherAPI
endpoint = '/marine.json'

# Construct the URL with f-string
url = f"{base_url}{endpoint}?key={api_key_weather}&q={location}"
print(url)


response_json = requests.get(url)
js_data = response_json.json()


# Flatten location data
loc_data = pd.json_normalize(js_data['location'])

# Flatten forecast data if it exists
tides = js_data['forecast']['forecastday'][0]['day']['tides'][0]['tide']
tides_df = pd.DataFrame(tides)

# Flatten hourly weather data
hourly_data = js_data['forecast']['forecastday'][0]['hour']
hourly_df = pd.DataFrame(hourly_data)

# Flatten forecast data
forecast_data = js_data['forecast']['forecastday'][0]['day']
forecast_df = pd.DataFrame(forecast_data)

# Add location info to hourly data
hourly_df = hourly_df.assign(
    name=js_data['location']['name'],
    region=js_data['location']['region'],
    country=js_data['location']['country']
)

# Fix the times in proper format
tides_df['tide_time'] = pd.to_datetime(tides_df['tide_time'])
hourly_df['time'] = pd.to_datetime(hourly_df['time'])

combined_df = pd.merge(hourly_df, tides_df, left_on='time', right_on='tide_time', how='outer')

# Add the rest of the data to the forecast data
combined_df = combined_df.assign(
    name=js_data['location']['name'],
    region=js_data['location']['region'],
    country=js_data['location']['country'],
    forecast_maxi_tem=forecast_df['maxtemp_c'][0],
    forecast_mini_tem=forecast_df['mintemp_c'][0],
    forecast_vis=forecast_df['avgvis_miles'][0]
)

# Clean up column order and missing values (optional)
combined_df = combined_df.sort_values(by='time').reset_index(drop=True)


# ------------------------------ CREATE A TABLE IN THE DATABASE ---------------------------------------

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
# Load environment variables
load_dotenv()
api_key_weather = os.getenv("API_KEY")
# .env variables
db_user = os.getenv("SOURCE_DB_USER")
db_password = os.getenv("SOURCE_DB_PASSWORD")
db_host = os.getenv("SOURCE_DB_HOST")
db_port = os.getenv("SOURCE_DB_PORT")
db_name = os.getenv("SOURCE_DB_NAME")

try:
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
    print("Succesfully connected to database")
except Exception as e:
    print(f"Failed to connect to database: {e}")



#----------------------------------- LOAD DATA INTO THE DATABASE  -------------------------------------

def save_to_db():
    try:
        combined_df.to_sql('vedi_marine_data', engine, schema='student', if_exists='replace', index=False)
        print("Data saved to database successfully")
    except Exception as e:
        print(f"Failed to save data to database: {e}")
        
save_to_db()
    





    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    