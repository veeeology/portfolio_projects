import os, zipfile, time, datetime, re
import datetime as dt
import rasterio
import rasterio.mask
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
    

# File locations and configuration
raster_crs = '+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +ellps=sphere +units=m +no_defs +type=crs'
raster_dir = 'eMODIS_NDVI_v6/'
states_shp_file = 'Culture/USA_States__Generalized_.geojson'

# Load in the Texas outline to crop the rasters
states_shp = gpd.read_file(states_shp_file)
shp_crs = states_shp.crs
texas = states_shp.loc[states_shp['STATE_NAME'] == 'Texas']
texas = texas.to_crs(raster_crs)
# Because the state outline includes lakes, etc, locate the largest of the
# multipolygons
texas = texas.iloc[0].geometry
texas_areas = [t.area for t in texas]
texas = [texas[i] for i in range(len(texas)) if texas_areas[i]==max(texas_areas)][0]


# # If needed, make sure the zip files have been extracted.
# zip_files = [f for f in os.listdir(raster_dir) if f.endswith('.zip')]
# for f in zip_files:
#     data_zip = zipfile.ZipFile(raster_dir + f)
#     data_zip.extractall(raster_dir)

# Get the unique list of file prefixes and dates to use for each raster
file_prefixes = dict()
for f in os.listdir(raster_dir):
    # Look for the date part of the string
    date_pattern = '\d{4}.\d{3}-\d{3}'
    date_match = re.search(date_pattern, f)
    
    # If the date is present, store it in the dictionary with the file prefix
    if date_match != None:
        date_str = date_match.group()
        year = int(date_str[0:4])
        start_days = int(date_str[5:8])
        end_days = int(date_str[9:12])
        
        # Convert from days since the beginning of the year to a date object
        start_date = dt.date(year, 1, 1) + dt.timedelta(days=start_days)
        end_date = dt.date(year, 1, 1) + dt.timedelta(days=end_days)
        
        # Construct the file prefix
        prefix = 'US_eMAH_NDVI_' + date_str + '.HKM.'
        if prefix not in file_prefixes.keys():
            file_prefixes[prefix] = dict()
        file_prefixes[prefix]['start_date'] = start_date
        file_prefixes[prefix]['end_date'] = end_date
        if 'HKM.VI_NDVI' in f and f.endswith('.tif'):
            file_prefixes[prefix]['NDVI_file'] = f
        if 'HKM.VI_QUAL' in f and f.endswith('.tif'):
            file_prefixes[prefix]['QUAL_file'] = f

# Iterate through each group of files to extract the data and 
for fp in file_prefixes:
    
    # First, open the NDVI raster and crop it to the state of Texas
    img = rasterio.open(raster_dir + file_prefixes[fp]['NDVI_file'])
    ndvi_masked, transform = rasterio.mask.mask(img, [texas]\
                                                ,crop=True, filled=False)
    ndvi_masked = ndvi_masked[0]
    # Plot the figure for display
    f1 = plt.figure()
    plt.title('NDVI from ' + str(file_prefixes[fp]['start_date']) + ' to ' \
              + str(file_prefixes[fp]['end_date']))
    frame = plt.gca()
    frame.axes.get_xaxis().set_visible(False)
    frame.axes.get_yaxis().set_visible(False)
    plt.imshow(ndvi_masked, cmap='gist_earth_r', vmin=-2000, vmax=10000)
    plt.colorbar()

    
    # Then open the quality file so that we can use it to filter our data
    img = rasterio.open(raster_dir + file_prefixes[fp]['QUAL_file'])
    qual_masked, transform = rasterio.mask.mask(img, [texas] \
                                                ,crop=True, filled=False)
    qual_masked = qual_masked[0]
    f1 = plt.figure()
    plt.title('QUAL from ' + str(file_prefixes[fp]['start_date']) + ' to ' \
              + str(file_prefixes[fp]['end_date']))
    frame = plt.gca()
    frame.axes.get_xaxis().set_visible(False)
    frame.axes.get_yaxis().set_visible(False)
    plt.imshow(qual_masked, cmap='viridis', vmin=0, vmax=4)
    plt.colorbar()
    
    # Use the data quality layer to exclude pixels from the ndvi map
    qual_flag = np.where(qual_masked>0.1, np.nan, 1.0)
    ndvi_masked_filtered = np.multiply(ndvi_masked, qual_flag)
    f1 = plt.figure()
    plt.title('NDVI from ' + str(file_prefixes[fp]['start_date']) + ' to ' \
              + str(file_prefixes[fp]['end_date']))
    frame = plt.gca()
    frame.axes.get_xaxis().set_visible(False)
    frame.axes.get_yaxis().set_visible(False)
    plt.imshow(ndvi_masked_filtered, cmap='gist_earth_r', vmin=-2000, vmax=10000)
    plt.colorbar()
    
    file_prefixes[fp]['cropped_filtered_map'] = ndvi_masked_filtered

# Final step: Build the difference maps to calculate the difference in NDVI
# from before and after the freeze
start_comparison = 'US_eMAH_NDVI_2021.033-039.HKM.'
for fp in file_prefixes:
    if fp != start_comparison:
        diffmap = file_prefixes[fp]['cropped_filtered_map'] \
            - file_prefixes[start_comparison]['cropped_filtered_map']
        f1 = plt.figure()
        plt.title('Difference in NDVI: pre-freeze (Feb 3-9) vs \n' \
                  + file_prefixes[fp]['start_date'].strftime('%b %d') \
                  + ' - ' + file_prefixes[fp]['end_date'].strftime('%b %d'))
        
        frame = plt.gca()
        frame.axes.get_xaxis().set_visible(False)
        frame.axes.get_yaxis().set_visible(False)
        plt.imshow(diffmap, cmap='RdYlGn', vmin=-1000, vmax=1000)
        plt.colorbar()