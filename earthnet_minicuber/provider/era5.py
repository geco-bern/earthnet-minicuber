
import os
import pystac_client
import stackstac
import rasterio

import planetary_computer as pc
from rasterio import RasterioIOError
import time
import numpy as np
import xarray as xr
import random
from contextlib import nullcontext

from shapely.geometry import Polygon, box
from . import provider_base

ERA5BANDS_DESCRIPTION = {
    'sp': 'surface_air_pressure', 
    'tp': 'precipitation_amount_1hour_Accumulation',
    'sr': 'integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation',
    't': 'air_temperature_at_2_metres',
    'maxt': 'air_temperature_at_2_metres_1hour_Maximum',
    'mint': 'air_temperature_at_2_metres_1hour_Minimum',
    'sea_t': 'sea_surface_temperature', 
    'east_wind_10': 'eastward_wind_at_10_metres',
    'east_wind_100':'eastward_wind_at_100_metres', 
    'north_wind_10':'northward_wind_at_10_metres', 
    'north_wind_100': 'northward_wind_at_100_metres', 
    'ap': 'air_pressure_at_mean_sea_level', 
    'dp': 'dew_point_temperature_at_2_metres'
}

class ERA5(provider_base.Provider):

    def __init__(self, bands = ['t2m', 'pev', 'slhf', 'ssr', 'sp', 'sshf', 'e', 'tp'], best_orbit_filter = True, five_daily_filter = False, brdf_correction = True, aws_bucket = "planetary_computer"):
        
        self.is_temporal = True

        self.bands = bands
        self.best_orbit_filter = best_orbit_filter
        self.five_daily_filter = five_daily_filter
        self.aws_bucket = aws_bucket

        if aws_bucket == "planetary_computer":
            URL = 'https://planetarycomputer.microsoft.com/api/stac/v1'

        else:
            raise Exception("Bucket not supported.")
        
        self.catalog = pystac_client.Client.open(URL)

        os.environ['AWS_NO_SIGN_REQUEST'] = "TRUE"


    def get_attrs_for_band(self, band):

        attrs = {}
        attrs["provider"] = "ERA5"
        attrs["interpolation_type"] = "nearest" 
        attrs["description"] = ERA5BANDS_DESCRIPTION[band]

        return attrs
        


    def load_data(self, bbox, time_interval, **kwargs):

        cm = nullcontext()
        gdal_session = stackstac.DEFAULT_GDAL_ENV.updated(always=dict(session=rasterio.session.AWSSession(aws_unsigned = True, endpoint_url = None)))

        with cm as gs:
        
            search = self.catalog.search(
                        bbox = bbox,
                        collections=["era5-pds"],
                        datetime=time_interval
            )            
       
            if self.aws_bucket == "planetary_computer":
                for attempt in range(10):
                    try:
                        items_era5 = pc.sign(search)
                    except pystac_client.exceptions.APIError:
                        print(f"ERA5: Planetary computer time out, attempt {attempt}, retrying in 60 seconds...")
                        time.sleep(random.uniform(30,90))
                    else:
                        break
                else:
                    print("Loading ERA5 failed after 10 attempts...")
                    return None
            else:
                items_era5 = search.get_all_items()

            if len(items_era5.to_dict()['features']) == 0:
                return None

            # Extract assets of interest 

            datasets = []
            for item in items_era5:
                signed_item = pc.sign(item)
                datasets += [
                    xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
                    for b in self.bands
                    if (ERA5BANDS_DESCRIPTION[b] in signed_item.assets.keys()) and (asset := signed_item.assets[ERA5BANDS_DESCRIPTION[b]])
                ]

            stack = xr.combine_by_coords(datasets, join="exact")

            # Drop the extra time variable
            stack = stack.drop_vars('time1_bounds') if 'time1_bounds' in stack.data_vars else stack
            
            return stack

            """
            metadata = items_era5.to_dict()['features'][0]["properties"]
            epsg = int(metadata['cube:dimensions']['lat']['reference_system'].split(':')[-1])
            
           
            #stack = stackstac.stack(items_era5, epsg = epsg, assets = self.bands, dtype = "float32", properties = ["era5:product_id"], band_coords = False, bounds_latlon = bbox, xy_coords = 'center', chunksize = 2048,errors_as_nodata=(RasterioIOError('.*'), ), gdal_env=gdal_session)
            stack = stackstac.stack(items_era5, epsg = epsg, dtype = "float32", assets = self.bands, properties = ["era5:product_id"], band_coords = False, bounds = bbox, xy_coords = 'center', resolution = (20,20), chunksize = 1024)
            


            stack = stack.drop_vars(["id_old", "era5:data_coverage", "sentinel:sequence"], errors = "ignore")

            stack.attrs["epsg"] = epsg

                      
            
            elif self.five_daily_filter:

                if "full_time_interval" in kwargs:
                    full_time_interval = kwargs["full_time_interval"]
                else:
                    full_time_interval = time_interval

                min_date, max_date = np.datetime64(full_time_interval[:10]), np.datetime64(full_time_interval[-10:])

                dates = np.arange(min_date, max_date+1, 5)

                stack = stack.sel(time = stack.time.dt.date.isin(dates))

            if len(stack.time) == 0:
                return None
                    
            bands = stack.band.values
            stack["band"] = [f"era5_{b}" for b in stack.band.values]

            stack = stack.to_dataset("band")


            stack = stack.drop_vars(["epsg", "id", "id_old", "era5:data_coverage", "era5:sequence", "era5:product_id"], errors = "ignore")
            
            stack["time"] = np.array([str(d) for d in stack.time.values], dtype="datetime64[D]")

            if len(stack.time) > 0:
                stack = stack.groupby("time.date").last(skipna = False).rename({"date": "time"})
            else:
                return None
            
            for band in bands:
                stack[f"era5_{band}"].attrs = self.get_attrs_for_band(band)
            
            stack.attrs["epsg"] = epsg

            return stack

            """


    
