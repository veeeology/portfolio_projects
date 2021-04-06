import SQL_python_integration.sql_utils as sql
import geopandas as gpd
from shapely import wkt
import os

geom_folder = 'Culture/'
files = [f for f in os.listdir(geom_folder) if f.endswith('.geojson')]

for f in files:
    geo_df = gpd.read_file(geom_folder + f)
    print(geo_df.crs)


# shp_files = pd.read_excel(map_dir + file_list)
# tables = shp_files['Table_Name'].values
# for t in tables:
#   # Retrieve all the correct metadata from the file loading index
#   shp_fname = shp_files.loc[shp_files['Table_Name']==t]['File'].values[0]
#   shp_epsg = shp_files.loc[shp_files['Table_Name']==t]['file_epsg'].values[0]
#   old_ID_col = shp_files.loc[shp_files['Table_Name']==t]['name_col'].values[0]
#   new_ID_col = shp_files.loc[shp_files['Table_Name']==t]['ID_COL'].values[0]
#   print(shp_fname)#, shp_epsg, old_ID_col, new_ID_col)
  
#   # Read in the shapefile data and convert to EPSG 32020
#   gdf = gpd.read_file(map_dir + shp_fname)
#   gdf = gdf.to_crs(32020)
  
#   # Calculate the WKT (Well-known text) representation of each object
#   gdf['WKT'] = gdf['geometry'].apply(wkt.dumps)
  
#   # Set the CRS column that specifies the EPSG code for the object
#   gdf['CRS'] = 32020
  
#   # Rename the ID column, if necessary
#   gdf[new_ID_col] = gdf[old_ID_col]
  
#   # Calculate the bounding box columns
#   gdf[['XMin', 'YMin', 'XCenter', 'YCenter', 'XMax', 'YMax']] = gdf.apply(lambda x: get_min_max_center(x['geometry']), axis=1, result_type='expand')
  
#   columns_for_export = [new_ID_col, 'WKT', 'CRS', 'XMin', 'YMin', 'XCenter', 'YCenter', 'XMax', 'YMax']
#   write_to_remote_sql(gdf[columns_for_export], t, url=url, driver=driver, mode='overwrite')


# CREATE TABLE ana_bwp.shp_Bakken_Study_AOI(

# Bakken_Study_AOI nvarchar(500),
# [geometry] geometry,
# WKT nvarchar(max),
# CRS bigint,
# date_updated datetime default getdate(),
# XMin decimal(10, 2),
# YMin decimal(10, 2),
# XCenter decimal(10, 2),
# YCenter decimal(10, 2),
# XMax decimal(10, 2),
# YMax decimal(10, 2)
# )



# CREATE TRIGGER [ana_bwp].[trg_shp_Bakken_Study_AOI]
#    ON  [ana_bwp].[shp_Bakken_Study_AOI]
#    INSTEAD OF INSERT
# AS 
# BEGIN
# 	-- SET NOCOUNT ON added to prevent extra result sets from
# 	-- interfering with SELECT statements.
# 	SET NOCOUNT ON;
# 	   INSERT INTO [ana_bwp].[shp_Bakken_Study_AOI]

# 	   SELECT [Bakken_Study_AOI]
# 			,geometry::STGeomFromText([WKT], cast([CRS] as int)) as [geometry]
# 			,[WKT]
# 			,cast([CRS] as int) as [CRS]
# 			,getdate() AS date_updated
# 			,XMin
# 			,YMin
# 			,XCenter
# 			,YCenter
# 			,XMax
# 			,YMax
# 		FROM INSERTED
	

# END
# GO