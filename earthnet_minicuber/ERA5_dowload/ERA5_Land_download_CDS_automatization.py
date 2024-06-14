#!/usr/bin/env python3
import cdsapi
import numpy as np
from datetime import date,timedelta
import os
import glob
import sys

args = sys.argv

year = args[1]
month = args[2]
if int(month) < 10:
    month = "0" + month

os.makedirs(str(year), exist_ok=True)

c = cdsapi.Client()

print("=========================================================")
print("Downloading {year}_{month}".format(year=year, month=month))
c.retrieve(
    'reanalysis-era5-single-levels',
    {
	'product_type': 'reanalysis',
        'variable': [
            '100m_u_component_of_wind', '100m_v_component_of_wind', '10m_u_component_of_wind',
            '10m_v_component_of_wind', '2m_dewpoint_temperature', '2m_temperature',
            'maximum_2m_temperature_since_previous_post_processing', 'mean_sea_level_pressure', 'minimum_2m_temperature_since_previous_post_processing',
            'sea_surface_temperature', 'surface_net_solar_radiation', 'surface_pressure',
            'surface_solar_radiation_downwards', 'total_precipitation',
        ],
        # 'date': date_dl,
        'year': str(year),
        'month': month,
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00',  '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        # 'grid': '0.25/0.25',
        'area': [49, 5, 45, 11,],
        'format': 'netcdf',
    }, 
    "./{year_f}/an_sfc_ERA5_{year}_{month}.netcdf".format(year_f=str(year), year=str(year), month=str(month))
)
