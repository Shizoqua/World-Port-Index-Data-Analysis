import requests
import zipfile
import io
import os
import pandas as pd
import pyodbc
import geopy.distance
import psycopg2
import math
from sqlalchemy import create_engine
from dotenv import  dotenv_values
dotenv_values()
from util import get_database_conn

#Function to download a zip file from a given URL
def download_zip_file(url, destination_folder):
    response = requests.get(url) 
    if response.status_code == 200:
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        zip_file.extractall(destination_folder)
        print(f"Downloaded and extracted to {destination_folder}")
    else:
        print(f"Failed to download zip file. Status code: {response.status_code}")
zip_url = "https://drive.google.com/uc?export=download&id=1VyCGCAfFuEK7vB1C9Vq8iPdgBdu-LDM4"
project_folder = r"C:\Users\HP\OneDrive\Documents\Worldport_data"
download_zip_file(zip_url, project_folder)

def read_wpi_data(access_file_path, table_name):
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_file_path}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # Query to select all data from the "Wpi Data" table with double quotes around the table name
    query = f'SELECT * FROM "{table_name}"'
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
mdb_file_path = os.path.join(project_folder, "WPI.mdb")
table_name = "Wpi Data"
wpi_data = read_wpi_data(mdb_file_path, table_name)
wpi_data.columns = wpi_data.columns.str.lower() 
wpi_data.to_csv('raw/worldport.csv', index=False)
print('worldport data written successfully to csv file')


def load_worldport_data():
    wpi_data = pd.read_csv('raw/worldport.csv')
    wpi_data.to_sql('wpi_data', con =get_database_conn(), if_exists = 'replace', index = False)
    print('Data loaded successfully to postgres')

load_worldport_data()


# Question 1: Nearest Ports to Singapore's JURONG ISLAND port
def nearest_singapore_port(csv_path='raw/worldport.csv'):
    wpi_data = pd.read_csv(csv_path)
    singapore_jurong_data = wpi_data[
        (wpi_data['wpi_country_code'] == 'SG') & 
        (wpi_data['main_port_name'] == 'JURONG ISLAND')
    ]
    # Extracting coordinates of JURONG ISLAND
    jurong_coords = (
        singapore_jurong_data['latitude_degrees'].iloc[0] + singapore_jurong_data['latitude_minutes'].iloc[0] / 60,
        singapore_jurong_data['longitude_degrees'].iloc[0] + singapore_jurong_data['longitude_minutes'].iloc[0] / 60
    )
    # Calculating distances to all ports
    wpi_data['distance_in_meters'] = wpi_data.apply(
        lambda row: geopy.distance.distance(jurong_coords, (row['latitude_degrees'] + row['latitude_minutes'] / 60, row['longitude_degrees'] + row['longitude_minutes'] / 60)).meters,
        axis=1
    )
    #The 5 nearest ports
    nearest_ports = wpi_data.sort_values('distance_in_meters').head(5)[['main_port_name', 'distance_in_meters']]
    return nearest_ports
result = nearest_singapore_port()
conn = get_database_conn()
result.to_sql(name='nearest_port_to_singapore', con=conn, if_exists='replace', index=False)
print('Five nearest ports to Singapore have been written to PostgreSQL successfully.')


# Question2:Which country has the largest number of ports with a cargo_wharf? Your answer should
# include the columns country and port_count only.


def largest_no_cargowharf_port():
    wpi_data = pd.read_csv('raw/worldport.csv') 
    cargo_wharf_ports = wpi_data[wpi_data['load_offload_wharves'] == 'Y']
    if cargo_wharf_ports.empty:
        print("No ports with a cargo_wharf found.")
        return pd.DataFrame(columns=['wpi_country_code', 'port_count'])
    # Group by country and count the number of ports with a cargo_wharf for each country
    country_port_counts = cargo_wharf_ports.groupby('wpi_country_code').size().reset_index(name='port_count')
    # Find the country with the maximum number of ports with a cargo_wharf
    max_ports_country = country_port_counts.loc[country_port_counts['port_count'].idxmax()].copy()
    max_ports_country['wpi_country_code'] = str(max_ports_country['wpi_country_code'])
    max_ports_country['port_count'] = int(max_ports_country['port_count'])
    return max_ports_country[['wpi_country_code', 'port_count']]
result = largest_no_cargowharf_port()
conn = get_database_conn()
result.to_sql(name='largest_no_of_cargowharf_port', con=conn, if_exists='replace', index=True)
print('Country with the largest number of cargowharf has been written to PostgreSQL successfully.')



def distance(lat1, lon1, lat2, lon2):
    earth_radius = 6371.0  # Earth radius in kilometers
    latitude_distance = math.radians(lat2 - lat1)
    longitude_distance = math.radians(lon2 - lon1)
    a = math.sin(latitude_distance / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(longitude_distance / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = earth_radius * c
    return distance

