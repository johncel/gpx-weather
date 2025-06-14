#%%
from datetime import datetime, timedelta, timezone
import gpx
import hrrr
#%%
import importlib
import gpx  # import your module

# Reload the module to reflect changes
importlib.reload(gpx)
from gpx import load_gpx

#%%
from datetime import datetime
# start_time = datetime(2024, 6, 3, 13, 0, 0)
# start_time = datetime(2025, 5, 18, 13, 0, 0)
# start_time = datetime(2023, 5, 3, 13, 0, 0)
# start_time = datetime(2023, 6, 6, 13, 0, 0)
start_time = datetime(2025, 6, 2, 13, 0, 0)
hrrr_dt = datetime.now(timezone.utc) - timedelta(hours=2)
# hrrr_dt = start_time
#%%
hrrr_dt
#%%

# FNAME="../assets/gpx/ALC_2024_Day_1_-_San_Francisco_-_Santa_Cruz.gpx"
FNAME="../assets/gpx/ALC_2024_Day_2_-_Santa_Cruz_-_King_City_CA.gpx"
# FNAME="../assets/gpx/ALC_2024_Day_3_-_King_City_-_Paso_Robles.gpx"
# FNAME="../assets/gpx/ALC_2024_Day_4_-_Paso_Robles_to_Santa_Maria.gpx"
# FNAME="../assets/gpx/ALC_2024_Day_5_-_Santa_Maria_-_Lompoc.gpx"
# FNAME="../assets/gpx/ALC_2024_Day_6_-_Lompoc_-_Ventura_CA.gpx"
# FNAME="../assets/gpx/ALC_2024_Day_7_-_Ventura_-_Santa_Monica.gpx"
print(f"loading {FNAME}")
df_wpt, df_trk = load_gpx(FNAME)
df_trk

#%%
# only take 1 of 10 df_trk points
df_trk = df_trk.iloc[::75, :]
# reindex
df_trk = df_trk.reset_index(drop=True)
df_trk
df_trk["lat"] = df_trk["lat"].astype(float)
df_trk["lon"] = df_trk["lon"].astype(float)

# %%
df_trk = gpx.calculate_elapsed_time(df_trk, 4, start_time=start_time)
# df_trk = gpx.calculate_elapsed_time(df_trk, 5, start_time=start_time)
df_trk
# %%
# load the hrrr data
import hrrr
importlib.reload(hrrr)

# %%
# quantize to the nearest 6 hours
hrrr_dt = hrrr_dt.replace(hour=hrrr_dt.hour - hrrr_dt.hour % 6)
hrrr_dt = hrrr_dt.replace(minute=0, second=0, microsecond=0)
hrrr_ds = hrrr.hrrr_to_ds(hrrr_dt)

# %%
hrrr_ds

# %%
import dask

# dask.config.set(scheduler='threads')

# df_trk_with_forecast = hrrr.add_ds_to_df(hrrr_ds, df_trk)
# %%
# add forecast to the track
# from dask.distributed import Client, progress
# df_trk_with_forecast = hrrr.add_ds_to_df(hrrr_ds, df_trk)
import numpy as np
from dask.diagnostics import ProgressBar
import pandas as pd

def chunked_df_processing(ds, df, chunk_size=10):
    chunks = np.array_split(df, max(1, len(df) // chunk_size))
    results = [dask.delayed(hrrr.add_ds_to_df)(ds, chunk.reset_index(drop=True)) for chunk in chunks]  # Reset index here
    
    with ProgressBar():
        results = dask.compute(*results)
    
    return pd.concat(results, ignore_index=True)  # Ignore index to avoid issues with duplicate indices

df_trk_with_forecast = chunked_df_processing(hrrr_ds, df_trk, chunk_size=100)
# df_trk_with_forecast = hrrr.add_ds_to_df(hrrr_ds, df_trk)

# %%
df_trk_with_forecast

# %%
df_trk_with_forecast["wind_speed_mps"] = np.sqrt(df_trk_with_forecast["UGRD"]**2 + df_trk_with_forecast["VGRD"]**2)

# %%
def wind_dir_from_u_v(u, v):
    # Calculate the angle in radians using atan2
    theta = np.arctan2(v, u)
    
    # Convert from radians to degrees
    theta_degrees = theta * 180 / np.pi
    
    # Calculate the meteorological wind direction
    wind_direction = (270 - theta_degrees) % 360
    
    return wind_direction

wind_dir_from_u_v(1, 0), wind_dir_from_u_v(0, 1), wind_dir_from_u_v(-1, 0), wind_dir_from_u_v(0, -1), wind_dir_from_u_v(1, 1), wind_dir_from_u_v(-1, 1), wind_dir_from_u_v(-1, -1), wind_dir_from_u_v(1, -1)

#%%

df_trk_with_forecast["wind_direction_degrees"] = [wind_dir_from_u_v(u, v) for u, v in zip(df_trk_with_forecast["UGRD"], df_trk_with_forecast["VGRD"])]

# %%
df_trk_with_forecast
# %%
# add the path direction using the lat lon at each track point
from geopy.distance import geodesic
import math

def calculate_initial_compass_bearing(pointA, pointB):
    """
    Calculates the bearing between two points.
    
    Parameters:
    pointA : tuple : (lat, lon) for the first point
    pointB : tuple : (lat, lon) for the second point
    
    Returns:
    float : bearing in degrees
    """
    lat1 = math.radians(pointA[0])
    lat2 = math.radians(pointB[0])
    
    diffLong = math.radians(pointB[1] - pointA[1])
    
    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(diffLong))
    
    initial_bearing = math.atan2(x, y)
    
    # Convert from radians to degrees and normalize to 0-360
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360
    
    return compass_bearing

def calculate_path_direction(df):
    path_directions = []
    for i in range(1, len(df)):
        coords_1 = (df.loc[i-1, 'lat'], df.loc[i-1, 'lon'])
        coords_2 = (df.loc[i, 'lat'], df.loc[i, 'lon'])
        path_direction = calculate_initial_compass_bearing(coords_1, coords_2)
        path_directions.append(path_direction)
    
    df["path_direction_degrees"] = [0] + path_directions

    return df

df_trk_with_forecast = calculate_path_direction(df_trk_with_forecast)

# %%
df_trk_with_forecast

# %%
# add the angle between the path direction and the wind direction
def calculate_wind_path_angle(df):
    wind_path_angles = [(wind_dir - path_dir + 360) % 360 for wind_dir, path_dir in zip(df["wind_direction_degrees"], df["path_direction_degrees"])]
    df["wind_path_angle_degrees"] = wind_path_angles
    # take the smaller angle if greater than 180
    df["wind_path_angle_degrees"] = [angle if angle <= 180 else 360 - angle for angle in df["wind_path_angle_degrees"]]

    return df
df_trk_with_forecast = calculate_wind_path_angle(df_trk_with_forecast)
df_trk_with_forecast

# add the tail and cross wind components, where tail wind is a wind_path_angle of 180 and cross wind is 90
# compute the component in the same direction as tail and cross
def compute_tail_cross_wind(df):
    tail_wind = -1 * df["wind_speed_mps"] * np.cos(np.radians(df["wind_path_angle_degrees"]))
    cross_wind = df["wind_speed_mps"] * np.sin(np.radians(df["wind_path_angle_degrees"]))
    df["tail_wind_mps"] = tail_wind
    df["cross_wind_mps"] = cross_wind

    return df

# %%
df_trk_with_forecast = compute_tail_cross_wind(df_trk_with_forecast)
df_trk_with_forecast

# %%
# average tail wind
average_tail_wind = df_trk_with_forecast["tail_wind_mps"].mean()
average_tail_wind
#%%
#average cross wind
average_cross_wind = df_trk_with_forecast["cross_wind_mps"].mean()
average_cross_wind

# %%
# plot the df_trk lat lon positions on a map
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point

#%%
import os
def plot_natural_earth(fig, ax):
    # Define the files and their draw order
    layers = [
        # ("ne_110m_admin_0_boundary_lines_land", {"color": "black", "linewidth": 0.5}),
        # ("ne_110m_admin_0_countries", {"edgecolor": "gray", "facecolor": "none", "linewidth": 1}),
        # ("ne_110m_populated_places", {"color": "red", "markersize": 5}),
        ("ne_10m_land", {"color": "green", "linewidth": 0.5}),
    ]
    # print the current working directory
    print(os.getcwd())

    # Base path to the unzipped shapefiles
    base_path = "../assets/natural_earth"

    # Load and plot each shapefile
    for layer_name, plot_kwargs in layers:
        shp_path = f"{base_path}/{layer_name}.shp"
        gdf = gpd.read_file(shp_path)
    
        # Use .plot() or .boundary.plot() depending on geometry type or desired style
        gdf.plot(ax=ax, **plot_kwargs)

#%%

fig, ax = plt.subplots(figsize=(24, 16), dpi=600)

# Create a GeoDataFrame
geometry = [Point(xy) for xy in zip(df_trk["lon"], df_trk["lat"])]
gdf = gpd.GeoDataFrame(df_trk, geometry=geometry)


plot_natural_earth(fig, ax)

gdf.plot(ax=ax, color='red')

# set the extents to the track
lat_buffer = 0.25
lon_buffer = 0.25
max_lat = df_trk["lat"].max() + lat_buffer
min_lat = df_trk["lat"].min() - lat_buffer
max_lon = df_trk["lon"].max() + lon_buffer
min_lon = df_trk["lon"].min() - lon_buffer
ax.set_xlim(min_lon, max_lon)
ax.set_ylim(min_lat, max_lat)

# add wind arrows at each track point
scale_factor = 0.01
for i, row in df_trk_with_forecast.iterrows():
    ax.arrow(row["lon"], row["lat"], row["UGRD"] * scale_factor, row["VGRD"] * scale_factor, head_width=0.00001, head_length=0.00001, fc='k', ec='k')

# # plot tcdc as a text number at each track point
# for i, row in df_trk_with_forecast.iterrows():
#     ax.text(row["lon"], row["lat"] + 0.05, f"{row['TCDC']}", fontsize=8)
#     ax.text(row["lon"] + 0.05, row["lat"] + 0.05, f"{int((row['TMP']-273.15) * 1.8 + 32)}", fontsize=8, color='red')
#     ax.text(row["lon"] + 0.15, row["lat"] + 0.05, f"{int(row['wind_speed_mps']*2)}", fontsize=8, color='red')
# plot tcdc as a text number at each track point
for i, row in df_trk_with_forecast.iterrows():
    ax.text(row["lon"], row["lat"] + 0.05, f"{row['TCDC']}", fontsize=7)
    ax.text(row["lon"], row["lat"] + 0.10, f"{int((row['TMP']-273.15) * 1.8 + 32)}", fontsize=7, color='red')
    ax.text(row["lon"], row["lat"] + 0.15, f"{int(row['wind_speed_mps']*2)}", fontsize=7, color='red')

plt.show()
# %%


# %%
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point

# Create the GeoDataFrame
geometry = [Point(xy) for xy in zip(df_trk["lon"], df_trk["lat"])]
gdf = gpd.GeoDataFrame(df_trk, geometry=geometry)

# Start figure
fig, ax = plt.subplots(figsize=(24, 16), dpi=600)

# Plot base map
plot_natural_earth(fig, ax)

# Plot only every 5th point to reduce visual density
gdf.iloc[::5].plot(ax=ax, color='red', markersize=8, label='Track points')

# Set map extent
lat_buffer = 0.25
lon_buffer = 0.25
max_lat = df_trk["lat"].max() + lat_buffer
min_lat = df_trk["lat"].min() - lat_buffer
max_lon = df_trk["lon"].max() + lon_buffer
min_lon = df_trk["lon"].min() - lon_buffer
ax.set_xlim(min_lon, max_lon)
ax.set_ylim(min_lat, max_lat)

# Plot wind arrows with better visibility and arrowheads
scale_factor = 0.01
for i, row in df_trk_with_forecast.iloc[::5].iterrows():
    ax.arrow(
        row["lon"], row["lat"],
        row["UGRD"] * scale_factor,
        row["VGRD"] * scale_factor,
        head_width=0.02,
        head_length=0.03,
        fc='black',
        ec='black',
        alpha=0.8
    )

# Add weather labels: cloud cover (TCDC), temperature, wind speed
for i, row in df_trk_with_forecast.iloc[::5].iterrows():
    lat = row["lat"]
    lon = row["lon"]
    temp_f = int((row["TMP"] - 273.15) * 1.8 + 32)
    wind_mph = int(row["wind_speed_mps"] * 2.237)

    ax.text(lon, lat + 0.01, f"Cloud: {row['TCDC']}", fontsize=8, ha='center', va='bottom')
    ax.text(lon, lat + 0.02, f"{temp_f}°F", fontsize=8, color='red', ha='center', va='bottom')
    ax.text(lon, lat + 0.03, f"{wind_mph} mph", fontsize=8, color='blue', ha='center', va='bottom')


# Optional: add legend or grid
ax.set_title("Wind Vectors & Forecast Along Track", fontsize=16)
ax.axis("off")
plt.tight_layout()
plt.show()

# %%
