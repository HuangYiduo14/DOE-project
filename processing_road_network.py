#%%

import pandas as pd
import os
import fiona
import geopandas as gpd
import os
# Create paths
network_path = os.path.join(os.path.dirname(__file__), 'Data/Network/')
traffic_path = os.path.join(os.path.dirname(__file__), 'Data/Traffic_Data/')


#road network needs to be preprocessed because there are no unique idenfiers for the nodes

# Open the geodatabase
gdb_path = os.path.join(os.path.dirname(__file__), 'Data/Network/Road FAF5/Freight_Analysis_Framework_(FAF5)_Network_Links.gdb/_ags_data5FBBB17ABC214E3C9AEFF8E84045B8DE.gdb')
layers = fiona.listlayers(gdb_path)
link_gdf = gpd.read_file(gdb_path, driver='FileGDB', layer=layers[0])

# %%
# create a dataframe that has all the endpoints of the link gdf
link_gdf['start_x'] = link_gdf['geometry'].apply(lambda x: x.geoms[0].coords[0][0])
link_gdf['start_y'] = link_gdf['geometry'].apply(lambda x: x.geoms[0].coords[0][1])
link_gdf['end_x'] = link_gdf['geometry'].apply(lambda x: x.geoms[0].coords[-1][0])
link_gdf['end_y'] = link_gdf['geometry'].apply(lambda x: x.geoms[0].coords[-1][1])

#%%create unique ID for each node

link_gdf['start_node_ID'] = link_gdf['id']
link_gdf['end_node_ID'] = link_gdf['id']*-1

# %%
#create a combined dataframe with the start and end points of the links
gdf_start = link_gdf[['start_x', 'start_y', 'start_node_ID']]
gdf_end = link_gdf[['end_x', 'end_y', 'end_node_ID']]
gdf_start.columns = ['x', 'y', 'node_ID']
gdf_end.columns = ['x', 'y', 'node_ID']
node_gdf = pd.concat([gdf_start, gdf_end], axis=0)
node_gdf.drop_duplicates(subset=['x', 'y'], inplace=True)
node_gdf = node_gdf.reset_index(drop=True)

#%%
# assign unique ID to nodes with same coordinates
node_gdf['coordinates'] = node_gdf[['x', 'y']].apply(lambda row: f"{row['x']},{row['y']}", axis=1)
coordinate_to_new_index = {coord: idx+1 for idx, coord in enumerate(node_gdf['coordinates'].unique())}
node_gdf['new_ID'] = node_gdf['coordinates'].map(coordinate_to_new_index)
node_gdf.drop(columns=['coordinates'], inplace=True)

#%%
# now translate those new IDs to the link_gdf
link_gdf['start_coordinates'] = link_gdf[['start_x', 'start_y']].apply(lambda row: f"{row['start_x']},{row['start_y']}", axis=1)
link_gdf['end_coordinates'] = link_gdf[['end_x', 'end_y']].apply(lambda row: f"{row['end_x']},{row['end_y']}", axis=1)

#%%
link_gdf['start_node_ID'] = link_gdf['start_coordinates'].map(coordinate_to_new_index)
link_gdf['end_node_ID'] = link_gdf['end_coordinates'].map(coordinate_to_new_index)
link_gdf.drop(columns=['start_coordinates', 'end_coordinates'], inplace=True)
# %%
link_gdf.to_csv( network_path + 'link_gdf.csv')
node_gdf.to_csv(network_path + 'node_gdf.csv')
# %%
