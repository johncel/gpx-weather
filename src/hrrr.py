import cartopy.crs as ccrs
import numpy as np
import pandas as pd
import xarray as xr
import zarr
hrrr_bucket_url_formatter_fct = "s3://hrrrzarr/sfc/%Y%m%d/%Y%m%d_%Hz_fcst.zarr"

projection = ccrs.LambertConformal(central_longitude=262.5, 
    central_latitude=38.5, 
    standard_parallels=(38.5, 38.5),
    globe=ccrs.Globe(semimajor_axis=6371229,
        semiminor_axis=6371229)
)


def hrrr_to_ds(date, formatter=hrrr_bucket_url_formatter_fct, hour=0):

    urls = ["s3://hrrrzarr/grid/HRRR_chunk_index.zarr/",
            date.strftime(formatter),
            f'{date.strftime(formatter)}/10m_above_ground/UGRD/10m_above_ground',
            f'{date.strftime(formatter)}/10m_above_ground/VGRD/10m_above_ground',
            f'{date.strftime(formatter)}/2m_above_ground/TMP/2m_above_ground',
            f'{date.strftime(formatter)}/entire_atmosphere/TCDC/entire_atmosphere',
            ]
    
    # fs = s3fs.S3FileSystem(anon=True)
    # try:
    #     dataset = xr.open_mfdataset([s3fs.S3Map(url, s3 = fs) for url in urls], engine='zarr')
    # except FileNotFoundError as e:
    #     return None
    # except Exception as e:
    #     # raise e
    #     return None

    try:
        storage_options = {
            "anon": True,
        }
        zarr_store_list = [
            zarr.storage.FsspecStore.from_url(url, storage_options=storage_options)
            for url in urls
        ]
        dataset = xr.open_mfdataset(zarr_store_list, engine="zarr")
    except FileNotFoundError as e:
        return None

    ds = dataset.rename(projection_x_coordinate="x", projection_y_coordinate="y")
    # Create a time array (replace this with actual time data you have)
    time_array = pd.date_range(start=date, periods=48, freq='h')  # Example: 48 hourly periods

    # Add the time coordinate to your dataset
    ds = ds.assign_coords(time=("time", time_array))

    # remove all but a few vars
    vars = ["UGRD", "VGRD", "TMP", "TCDC", "latitude", "longitude"]
    ds = ds[vars]

    return ds


def latlon_to_xy(lat, lon, projection):
    print(f"lat: {lat}, lon: {lon}")
    print(f"projection: {projection}")
    x, y = projection.transform_point(lon, lat, ccrs.Geodetic())
    return x, y


def get_nearest_from_latlon(ds, lat, lon, projection):
    print(f"lat: {lat}, lon: {lon}")
    lat = float(lat)
    lon = float(lon)
    x, y = latlon_to_xy(lat, lon, projection)

    
    # Use xarray's sel method to fetch nearest data point
    nearest_data = ds.sel(x=x, y=y, method='nearest')
    print(f"nearest_data: {nearest_data}")
    return nearest_data


def add_ds_to_df(ds, df):
    vars = ["UGRD", "VGRD", "TMP", "TCDC", "latitude", "longitude"]
    var_dict = {variable: [] for variable in vars}
    var_dict["hrrr_time"] = []
    
    latitudes = [float(lat) for lat in df["lat"].values]
    longitudes = [float(lon) for lon in df["lon"].values]
    times = pd.to_datetime(df["time"]).values

    coords = []
    for lat, lon in zip(latitudes, longitudes):
        x, y = latlon_to_xy(lat, lon, projection)
        coords.append((x, y))
    
    def fetch_data(x, y, time):
        print(f"Fetching data for x: {x}, y: {y}, time: {time}")
        ll_ds = ds.sel(x=x, y=y, method='nearest')
        ll_ds = ll_ds.sel(time=time, method='nearest')
        return_dict = {variable: float(ll_ds[variable].values) for variable in vars}
        return_dict["hrrr_time"] = ll_ds.time.values

        return return_dict
    
    for (x, y), time in zip(coords, times):
        try:
            result = fetch_data(x, y, time)
            for variable in result:
                var_dict[variable].append(result[variable])
        except InvalidIndexError:
            # Handle the case where indexing fails
            for variable in var_dict:
                var_dict[variable].append(None)
    
    for variable in var_dict:
        df[variable] = var_dict[variable]

    return df

    def get_x_y_t_idx(x, y, time):
        x_idx = np.argmin(np.abs(ds.x - x))
        y_idx = np.argmin(np.abs(ds.y - y))
        t_idx = np.argmin(np.abs(ds.time - time))
        return x_idx, y_idx, t_idx

