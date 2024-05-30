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

dask.config.set(scheduler='threads')

# %%
# add forecast to the track
# from dask.distributed import Client, progress
# df_trk_with_forecast = hrrr.add_ds_to_df(hrrr_ds, df_trk)
import numpy as np
from dask.diagnostics import ProgressBar
import pandas as pd

def chunked_df_processing(ds, df, chunk_size=100):
    chunks = np.array_split(df, max(1, len(df) // chunk_size))
    results = [dask.delayed(hrrr.add_ds_to_df)(ds, chunk.reset_index(drop=True)) for chunk in chunks]  # Reset index here
    
    with ProgressBar():
        results = dask.compute(*results)
    
    return pd.concat(results, ignore_index=True)  # Ignore index to avoid issues with duplicate indices

df_trk_with_forecast = chunked_df_processing(hrrr_ds, df_trk, chunk_size=100)

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

np.arctan2(df_trk_with_forecast["UGRD"], df_trk_with_forecast["VGRD"]) * 180 / np.pi