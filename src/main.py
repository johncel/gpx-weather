#%%
import gpx
import hrrr
#%%
import importlib
import gpx  # import your module

# Reload the module to reflect changes
importlib.reload(gpx)
from gpx import load_gpx, calculate_elapsed_time

#%%

FNAME="../assets/gpx/ALC_2024_Day_1_-_San_Francisco_-_Santa_Cruz.gpx"
print(f"loading {FNAME}")
df_wpt, df_trk = load_gpx(FNAME)
df_trk

#%%
# only take 1 of 10 df_trk points
df_trk = df_trk.iloc[::50, :]
# reindex
df_trk = df_trk.reset_index(drop=True)
df_trk
df_trk["lat"] = df_trk["lat"].astype(float)
df_trk["lon"] = df_trk["lon"].astype(float)

# %%
df_trk = gpx.calculate_elapsed_time(df_trk, 10)
df_trk
# %%
# load the hrrr data
import hrrr
importlib.reload(hrrr)

# %%
from datetime import datetime, timedelta
hrrr_dt = datetime.now() - timedelta(hours=1)
# quantize to the nearest 6 hours
hrrr_dt = hrrr_dt.replace(hour=hrrr_dt.hour - hrrr_dt.hour % 6)
hrrr_dt = hrrr_dt.replace(minute=0, second=0, microsecond=0)
hrrr_ds = hrrr.hrrr_to_ds(hrrr_dt)

# %%
hrrr_ds

# %%
import dask

# dask.config.set(scheduler='threads')

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
