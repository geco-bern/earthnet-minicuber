#!/bin/bash

echo "Hello"

start_year=2016
end_year=2023

years=$(seq $start_year $end_year)

for year in $years;
do
  echo $year
  
  months=$(seq 1 12)
  
  for month in $months;
  do
    echo $month
    # python3 testing_python.py $year $month &
    python3 ERA5_Land_download_CDS_automatization.py $year $month &
    sleep 10
  done
  
  # python3 testing_python.py $year $month &
done
