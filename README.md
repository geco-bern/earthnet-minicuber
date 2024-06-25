
# GECO EarthNet Minicuber

*Code for creating EarthNet-style minicubes.*

This package creates minicubes from cloud storage using STAC catalogues. A minicube usually contains a satellite image time series of Sentinel 2 imagery alongside other complementary information, all re-gridded to a common grid. This package implements a cloud mask based on deep learning, which allows for analysis-ready Sentinel 2 imagery.

It is currently under development, thus do expect bugs and please report them!

## Additions to original earthnet-minicuber

The modifications to the code now allow more flexibility in querying and downloading, specifically regarding ERA-5 climate reanalysis data. Whereas previously the package had limited spatial coverage of ERA-5 data, it is now possible to request any region globally.\
ERA-5 has a hourly temporal resolution, but is often aggregated to coarser resolutions when combined with other spatio-temporal datasets (e.g. Sentinel-2 with a 5-daily frequency). The code allows for the temporal aggregation of ERA-5 variables to any desired resolution, each according to their statistic (mean, minimum, maximum...). An automatic matching to the Sentinel-2 timeseries is also possible if the two data sources are requested together. 


## Tutorial

1. Loading the code as a package
- Download/Clone the repository from Github
- Install and activate the conda environment
```sh
pip install conda
cd /path/to/Repo
conda env create -f environment.yml
conda activate mc
```
If not done already, install and configure the planetary computer api access:https://planetarycomputer.developer.azure-api.net/, if you need access to data from a planetary computer bucket.

Then load the minicuber:
```Python
# Add the path to the repository
import sys
sys.path.insert(0, '/Absolute_path_to_repo/earthnet-minicuber/')
# Import the module
from earthnet_minicuber.minicuber import *
```

2. Creating a dictionary with specifications of the desired minicube
```Python
specs = {
    "lon_lat": (43.598946, 3.087414), # center pixel
    "xy_shape": (256, 256), # width, height of cutout around center pixel
    "resolution": 10, # in meters.. will use this on a local UTM grid..
    "time_interval": "2021-07-01/2021-07-31",
    "providers": [
        {
            "name": "s2",
            "kwargs": {"bands": ["B02", "B03", "B04", "B8A"], "best_orbit_filter": True, "five_daily_filter": False, "brdf_correction": True, "cloud_mask": True, "aws_bucket": "planetary_computer"}
        },
        {
           "name": "era5",
            "kwargs": {"bands": ['sr', 't', 'mint'], "aws_bucket": "planetary_computer", "n_daily_filter": None, "agg_list": ['min', 'max', 'sum'], "match_s2": True} 
        },
        {
            "name": "s1",
            "kwargs": {"bands": ["vv", "vh"], "speckle_filter": True, "speckle_filter_kwargs": {"type": "lee", "size": 9}, "aws_bucket": "planetary_computer"} 
        },
        {
            "name": "ndviclim",
            "kwargs": {"bands": ["mean", "std"]}
        },
        {
            "name": "cop",
            "kwargs": {}
        },
        {
            "name": "esawc",
            "kwargs": {"bands": ["lc"], "aws_bucket": "planetary_computer"}
        }
        ]
}
```

3. Downloading the minicube
```Python
mc = emc.load_minicube(specs, compute = True)
```

4. Plotting cloud-masked Sentinel 2 RGB imagery
```Python
emc.plot_rgb(mc)
```

See `notebooks/example.ipynb` for a more detailed usage example.



## Data Providers

The minicuber is centered around the concept of data providers, which wrap a data source and handle data loading of that source. The `emc.Minicuber` class then manages these data providers, by telling them the spatio-temporal range for which data needs to be loaded and afterwards re-gridding all data to a common reference frame (UTM grid).

### Sentinel 2

The Sentinel 2 provider loads and processes Copernicus Sentinel 2 imagery.

Kwargs:
- `bands`: choose any subset from `["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12", "WVP", "AOT", "SCL"]`.
- `aws_bucket`: We currently support data loading from three cloud buckets: Microsoft Planetary Computer (`"planetary_computer"`), Element84 AWS bucket (`element84`) and DigitalEarthAfrica AWS bucket (`dea`). We recommend using the Microsoft planetary computer with the keyword argument `aws_bucket = "planetary_computer"`.
- `best_orbit_filter`: Sentinel 2 has a regular overpass frequency of 5 days. However, sometimes it can be smaller due to off-nadir captures. Such captures change the viewing angle of the scene. If `True`, this filter finds the best orbit and then only returns imagery from a regular 5-daily cycle.
- `five_daily_filter`: If `True` returns a regular 5-daily cycle starting with the first date in `full_time_interval`. It has no effect, if `best_orbit_filter` is used.
- `brdf_correction`: If `True`, does BRDF correction based on the Sentinel 2 Metadata (illumination angles).
- `cloud_mask`: If `True`, creates a cloud and cloud shadow mask based on deep learning. It automatically finds the best available cloud mask for the requested `bands`.
- `cloud_mask_rescale_factor`: If using cloud mask and a lower resolution than 10m, set this rescaling factor to the multiple of 10m that you are requesting. E.g. if `resolution = 20`, set `cloud_mask_rescale_factor = 2`.
- `correct_processing_baseline`: If `True` (default): corrects the shift of +1000 that exists in Sentinel 2 data with processing baseline >= 4.0


### ERA5

The ERA5 provider loads and processes hourly ECMWF climate reanalysis data.

Kwargs:
- `bands`: choose any subset from `["sp", "tp", "sr", "t", "maxt", "mint", "sea_t", "east_wind_10", "east_wind_100", "north_wind_10", "north_wind_100", "ap", "dp"]`.
    - sp = surface pressure
    - tp = total precipitation 
    - sr = solar radation 
    - (min/max)t = (min/max) temperature 
    - sea_t = sea surface temperature
    - east/north_wind_10/100 = eastward/northward wind at 10/100 metres
    - ap = air pressure at sea level
    - dp = dew point temperature\
    More on the variables here: https://planetarycomputer.microsoft.com/dataset/era5-pds
- `aws_bucket`: We currently support data loading from two cloud buckets: Microsoft Planetary Computer ("planetary_computer") and AWS bucket ("s3"). Because AWS allows downloading more recent dates, we advise using "s3".
Alternativly, the data can be loaded from local files. specify this with "load_local". The data should be downloaded directly from ecmwf as provided in the ERA5_dowload/download.sh. For this, you need to configure the api key: https://cds.climate.copernicus.eu/api-how-to. This is needed for data after 2020 as of June 2024, the other buckets do not host this data.
- `n_daily_filter`: Integer. Will aggregate (mean) the data to n-daily, starting form the first date available in the data. 
- `agg_list`: List of aggregation functions for each variable among `['min', 'max', 'mean', 'median', 'sum']`. The list must be as long as the number of bands, and in the same order as the bands. For example if querying ['t', 'sp', 'sr'] with agg_list = ['min', 'sum', 'mean'] then 't' will be aggregate using 'min' and so forth. If None and `n_daily_filter` provided, all variables aggregated with 'mean' by default.
- `match_s2`: If True, match the timestamps to those of Sentinel-2 (5-daily), using as first date the first occurrence of Sentinel-2 data. This will override `n_daily_filter`. All variables aggregated using 'mean' unless provided otherwise with `agg_list`. **Attention: Sentinel-2 must be provided first in specs in this case.**


## Similar Packages

This package is build on top of [stackstac](https://stackstac.readthedocs.io/en/latest/), which allows accessing data stored in cloud-optimized geotiffs with xarray.

Similar to this package, [cubo](https://github.com/davemlz/cubo) provides a high-level interface to stackstac.


## Acknowledgement

This project is a continuation of the work developed by Requena-Mesa et al. https://github.com/earthnet2021/earthnet-minicuber
