
import os
import pystac_client
import stackstac
import rasterio

import planetary_computer as pc
import s3fs
import boto3
import botocore
import zarr
from rasterio import RasterioIOError
import time
import numpy as np
import xarray as xr
import random
from contextlib import nullcontext
import os.path
import datetime

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

    def __init__(self, bands = ['t'], n_daily_filter = None, aws_bucket = "planetary_computer", match_s2 = True, agg_list=None):
        
        self.is_temporal = True
        self.name = 'e5'
        self.bands = bands
        self.n_daily_filter = n_daily_filter
        self.agg_list = agg_list
        self.match_s2 = match_s2
        self.aws_bucket = aws_bucket

        if aws_bucket == "planetary_computer":
            URL = 'https://planetarycomputer.microsoft.com/api/stac/v1'
            self.catalog = pystac_client.Client.open(URL)
        elif aws_bucket == "s3":
            client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED)) # No AWS keys required
            # Create an S3FileSystem instance
            #self.catalog = s3fs.S3FileSystem(anon=True)  # Not really a catalog, actually a filesystem   
            self.s3 = s3fs.S3FileSystem(anon=True)  
        else:
            raise Exception("Bucket not supported.")
        
        
        os.environ['AWS_NO_SIGN_REQUEST'] = "TRUE"


    def get_attrs_for_band(self, band):

        attrs = {}
        attrs["provider"] = "ERA5"
        attrs["interpolation_type"] = "nearest" 
        attrs["description"] = ERA5BANDS_DESCRIPTION[band]

        return attrs
        


    def load_data(self, bbox, time_interval, **kwargs):

        if self.aws_bucket == "planetary_computer":

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

                epsg = int(items_era5.to_dict()['features'][0]['properties']['cube:dimensions']['lat']['reference_system'].split('epsg:')[1])
                
                """
                for item in items_era5:
                    signed_item = pc.sign(item)
                    for b in self.bands:
                        asset = signed_item.assets[ERA5BANDS_DESCRIPTION[b]]
                        print(asset.extra_fields["xarray:open_kwargs"])
                # Extract assets of interest 
                """
                datasets = []
                for item in items_era5:
                    signed_item = pc.sign(item)
                    datasets += [
                        xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])
                        for b in self.bands
                        if (ERA5BANDS_DESCRIPTION[b] in signed_item.assets.keys()) and (asset := signed_item.assets[ERA5BANDS_DESCRIPTION[b]])
                    ]
    
                

        if self.aws_bucket == "s3":
            year = time_interval.split('-')[0]
            month = time_interval.split('-')[1]
            date = datetime.date(int(year), int(month), 1)

            datasets = []

            for b in self.bands:
                if b in ERA5BANDS_DESCRIPTION.keys():
                    v = ERA5BANDS_DESCRIPTION[b]
                
                    # file path patterns for remote S3 objects
                    #s3_data_key = f'{year}/{month}/data/{v}.nc' 
                    s3_zarr_store = f'era5-pds/zarr/{year}/{month}/data/{v}.zarr'

                    # Open the Zarr store using s3fs
                    store = s3fs.S3Map(s3_zarr_store, s3=self.s3)
                    # Wrap the store in KVStore
                    kvstore = zarr.storage.KVStore(store)
                    # Open the dataset using xr.open_dataset()
                    ds = xr.open_dataset(kvstore, engine='zarr')

                    # Fix the time variable here
                    time_coord = [coord for coord in list(ds.coords.keys()) if coord.startswith('time')][0]
                    if 'time'!= time_coord:
                        ds = ds.rename({time_coord: 'time'})

                    # Convert data variables to Dask arrays (following what is done with planetary_computer)
                    num_chunks_time = 2
                    num_chunks_lat = 5
                    num_chunks_lon = 10
                    chunk_sizes = {'time': len(ds['time'])//num_chunks_time, 'lon': len(ds['lon'])//num_chunks_lon, 'lat': len(ds['lat'])//num_chunks_lat} 
                    ds = ds.chunk(chunk_sizes)
                
                    datasets.append(ds)
                else:
                    print(f'{b} not found for {time_interval}, skipping.')
                         
        stack = xr.combine_by_coords(datasets, join="exact")

        # Drop the extra time variable
        stack = stack.drop_vars('time1_bounds') if 'time1_bounds' in stack.data_vars else stack
        
        # Rename bands with short names
        key_list = list(ERA5BANDS_DESCRIPTION.keys())
        val_list = list(ERA5BANDS_DESCRIPTION.values())

        stack = stack.rename({b:'era5_'+key_list[val_list.index(b)] for b in list(stack.data_vars)})
        
    
        if self.n_daily_filter and not self.match_s2:
            if self.agg_list:
                if (len(self.agg_list) == len(self.bands)):
                    # Perform variable-wise resampling
                    resampled_stack = xr.Dataset()
                    for i, var_name in enumerate(self.bands):
                        agg_type = self.agg_list[i]
                        var_resampled = stack['era5_'+var_name]
                        if agg_type == 'sum':
                            var_resampled = var_resampled.resample(time=f'{self.n_daily_filter}D').sum()
                        if agg_type == 'mean':
                            var_resampled = var_resampled.resample(time=f'{self.n_daily_filter}D').mean()
                        if agg_type == 'median':
                            var_resampled = var_resampled.resample(time=f'{self.n_daily_filter}D').median()
                        if agg_type == 'min':
                            var_resampled = var_resampled.resample(time=f'{self.n_daily_filter}D').min()
                        if agg_type == 'max':
                            var_resampled = var_resampled.resample(time=f'{self.n_daily_filter}D').max()
                        
                        resampled_stack['era5_'+var_name] = var_resampled

                    resampled_stack.attrs = stack.attrs
                    stack = resampled_stack

                else:
                    raise Exception('agg_list does not have same number of elements as there are bands!')

            else:
                # All resampled using mean
                stack = stack.resample(time=f'{self.n_daily_filter}D').mean()

        if self.n_daily_filter and self.match_s2:
            print('Provided both n_daily filter and match_s2! Will only use match_s2.')


        if len(stack.time) == 0:
            return None

        stack = stack.drop_vars(["epsg", "id", "id_old", "era5:data_coverage", "era5:sequence", "era5:product_id"], errors = "ignore")
        
        stack["time"] = np.array([str(d) for d in stack.time.values], dtype="datetime64[h]")
        
        for band in self.bands:
            stack[f"era5_{band}"].attrs = self.get_attrs_for_band(band)
        
        stack.attrs["epsg"] = 4326

        if 'spatial_ref' in stack.dims:
            stack = stack.drop('spatial_ref')
        if 'angle' in stack.dims:
            stack = stack.drop('angle')
        if 'angle' in stack.coords:
            stack = stack.reset_coords('angle', drop=True)


        return stack




    def match_to_sentinel(self, cube, first_date):
        cube_filtered = cube.sel(time=cube.time>first_date)
        
        # Then resample either using agg list or mean
        if self.agg_list:
            if (len(self.agg_list) == len(self.bands)):
                resampled_stack = xr.Dataset()
                for i, var_name in enumerate(self.bands):
                    agg_type = self.agg_list[i]
                    var_resampled = cube_filtered['era5_'+var_name]
                    if agg_type == 'sum':
                        var_resampled = var_resampled.resample(time='5D').sum()
                    if agg_type == 'mean':
                        var_resampled = var_resampled.resample(time='5D').mean()
                    if agg_type == 'median':
                        var_resampled = var_resampled.resample(time='5D').median()
                    if agg_type == 'min':
                        var_resampled = var_resampled.resample(time='5D').min()
                    if agg_type == 'max':
                        var_resampled = var_resampled.resample(time='5D').max()
                    
                    resampled_stack['era5_'+var_name] = var_resampled
            else:
                raise Exception('agg_list does not have same number of elements as there are bands!')
    

            resampled_stack.attrs = cube.attrs
            cube = resampled_stack

        else:
            # All resampled using mean
            cube = cube_filtered.resample(time='5D').mean()

        # Put time index to datetime.date() format to match S2
        cube["time"] = cube.time.to_index().date

        return cube




    
