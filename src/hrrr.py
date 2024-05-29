from datetime import datetime, timedelta
import xarray as xr
import s3fs
import pandas as pd
import cartopy.crs as ccrs


projection = ccrs.LambertConformal(central_longitude=262.5, 
    central_latitude=38.5, 
    standard_parallels=(38.5, 38.5),
    globe=ccrs.Globe(semimajor_axis=6371229,
        semiminor_axis=6371229)
)
hrrr_bucket_url_formatter_fct = "s3://hrrrzarr/sfc/%Y%m%d/%Y%m%d_%Hz_fcst.zarr"


def hrrr_to_ds(date, formatter=hrrr_bucket_url_formatter, hour=0):

    urls = ["s3://hrrrzarr/grid/HRRR_chunk_index.zarr/",
            date.strftime(formatter),
            f'{date.strftime(hrrr_bucket_url_formatter)}/10m_above_ground/UGRD/10m_above_ground',
            f'{date.strftime(hrrr_bucket_url_formatter)}/10m_above_ground/VGRD/10m_above_ground',
            f'{date.strftime(formatter)}/2m_above_ground/TMP/2m_above_ground',
            ]
    
    fs = s3fs.S3FileSystem(anon=True)
    try:
        dataset = xr.open_mfdataset([s3fs.S3Map(url, s3 = fs) for url in urls], engine='zarr')
    except FileNotFoundError as e:
        return None
    except Exception as e:
        raise e

    ds = dataset.rename(projection_x_coordinate="x", projection_y_coordinate="y")

    return dataset


def latlon_to_xy(lat, lon, projection):
    x, y = projection.transform_point(lon, lat, ccrs.Geodetic())
    return x, y


def get_nearest_from_latlon(ds, lat, lon, projection):
    x, y = latlon_to_xy(lat, lon, projection)
    
    # Use xarray's sel method to fetch nearest data point
    nearest_data = ds.sel(x=x, y=y, method='nearest')
    return nearest_data


def add_ds_to_df(ds, df, start_dt=datetime.now()):
    for variable in ds.variables:
        var_values = []
        for i, row in df.iterrows():
            lat = row["lat"]
            lon = row["lon"]
            value = get_nearest_from_latlon(ds, lat, lon, projection)

            var_values.append(value) 
        df[variable] = var_values

    return df
