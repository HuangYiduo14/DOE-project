#%%
import networkx as nx
import numpy as np
import pandas as pd
import os
import fiona
import geopandas as gpd
import os

# Create paths
network_path = os.path.join(os.path.dirname(__file__), 'Data/Network/')
traffic_path = os.path.join(os.path.dirname(__file__), 'Data/Traffic_Data/')

# load the data on dataframes
#%% RAIL NETWORK

# universal link and node IDs
new_node_id_start = 1
new_link_id_start = 1
#%% ROAD NETWORK
#road links
#load dataframe
road_link_df = pd.read_csv(network_path + 'Road FAF5/link_gdf.csv')

#identify the "Original_ID"
road_link_df.rename(columns={'id': 'Original_ID'}, inplace=True)
# assign universal ID for links
road_link_df['new_ID'] = range(new_link_id_start, new_link_id_start + len(road_link_df))
road_link_final = road_link_df[['new_ID', 'Original_ID', 'start_node_ID', 'end_node_ID', 'shape_Length']]
road_link_final.rename(columns={'shape_Length': 'Length'}, inplace=True)
# create "type" attribute
road_link_final['type'] = 'road'

# road nodes have no identifyer, so I just create it from the link df
road_nodes_df = pd.read_csv(network_path + 'Road FAF5/node_gdf.csv')
road_node_final = road_nodes_df[['new_ID', 'x', 'y']]
road_node_final.rename(columns={'x': 'X', 'y': 'Y'}, inplace=True)
road_node_final['type'] = 'road'
road_node_final['Original_ID'] = road_node_final['new_ID']
# reorder 
road_node_final = road_node_final[['new_ID', 'Original_ID', 'X', 'Y', 'type']]

# update universal IDs
new_node_id_start = max(road_nodes_df['new_ID']) + 1
new_link_id_start = max(road_link_df['new_ID']) + 1

#%% RAIL NETWORK

#similar procedure with rail network
rail_node_df = pd.read_csv(network_path + 'Rail/Nodes.csv')
rail_node_df['new_ID'] = range(new_node_id_start, new_node_id_start + len(rail_node_df))
rail_node_df.rename(columns={'FRANODEID': 'Original_ID'}, inplace=True)
rail_node_final = rail_node_df[['new_ID', 'Original_ID', 'X', 'Y']]
rail_node_final['type'] = 'rail'
rail_node_mapping = {row['Original_ID']: row['new_ID'] for index, row in rail_node_df.iterrows()}

#%%
rail_link_df = pd.read_csv(network_path + 'Rail/Lines.csv')
rail_link_df['new_ID'] = range(new_link_id_start, new_link_id_start + len(rail_link_df))

rail_link_df['start_node_ID'] = rail_link_df['FRFRANODE'].map(rail_node_mapping)
rail_link_df['end_node_ID'] = rail_link_df['TOFRANODE'].map(rail_node_mapping)

new_node_id_start = max(rail_node_df['new_ID']) + 1
rail_link_df.rename(columns={'FRAARCID': 'Original_ID'}, inplace=True)

rail_link_final = rail_link_df[['new_ID', 'Original_ID', 'start_node_ID', 'end_node_ID', 'MILES']]
rail_link_final.rename(columns={'MILES': 'Length'}, inplace=True)
rail_link_final['type'] = 'rail'
#%% WATER NETWORK

# repeat similar procedure with water network
water_nodes_df = pd.read_csv(network_path + 'Waterway/Nodes.csv')
water_nodes_df['new_ID'] = range(new_node_id_start, new_node_id_start + len(water_nodes_df))
water_nodes_df.rename(columns={'ID': 'Original_ID'}, inplace=True)

water_link_df = pd.read_csv(network_path + 'Waterway/Lines.csv')
water_link_df['new_ID'] = range(new_link_id_start, new_link_id_start + len(water_link_df))
water_link_df.rename(columns={'ID': 'Original_ID', 'ANODE': 'start_node_ID', 'BNODE': 'end_node_ID'}, inplace=True)
water_link_df = water_link_df.dropna(subset=['start_node_ID', 'end_node_ID'])


water_node_final = water_nodes_df[['new_ID', 'Original_ID', 'X', 'Y']]
water_node_final['type'] = 'waterway'
water_link_final = water_link_df[['new_ID', 'Original_ID', 'start_node_ID', 'end_node_ID', 'LENGTH1']]
water_link_final.rename(columns={'LENGTH1': 'Length'}, inplace=True)
water_link_final['type'] = 'waterway'

#change node_id of water to integer type as well as start_node_ID and end_node_Id in the link df
water_node_final['new_ID'] = water_node_final['new_ID'].astype(int)
water_link_final['start_node_ID'] = water_link_final['start_node_ID'].astype(int)
water_link_final['end_node_ID'] = water_link_final['end_node_ID'].astype(int)
# %% COMBINE ALL NETWORKS

#combine all final dataframes for nodes and links separately

node_final = pd.concat([road_node_final, rail_node_final, water_node_final], axis=0)
link_final = pd.concat([road_link_final, rail_link_final, water_link_final], axis=0)

#remove diplicates of new_ID
node_final = node_final.drop_duplicates(subset='new_ID')
link_final = link_final.drop_duplicates(subset='new_ID')

# %% Saving everything on csv to have updated csv files with Original and new IDs

node_final.to_csv('C:/Users/Zeus/Desktop/All_Nodes.csv', index=False)
link_final.to_csv('C:/Users/Zeus/Desktop/All_Links.csv', index=False)
road_link_df.to_csv('C:/Users/Zeus/Desktop/Road_Links.csv', index=False)
rail_link_df.to_csv('C:/Users/Zeus/Desktop/Rail_Links.csv', index=False)
water_link_df.to_csv('C:/Users/Zeus/Desktop/Water_Links.csv', index=False)
road_nodes_df.to_csv('C:/Users/Zeus/Desktop/Road_Nodes.csv', index=False)
rail_node_df.to_csv('C:/Users/Zeus/Desktop/Rail_Nodes.csv', index=False)
water_nodes_df.to_csv('C:/Users/Zeus/Desktop/Water_Nodes.csv', index=False)
# %% CREATE SQLITE DATABASE

# now create a sqlite database using new_ID as the primary key for the nodes and links tables
import sqlite3
# create a connection to the database
conn = sqlite3.connect('network.db')

# create a cursor
cur = conn.cursor()
# create a table for the nodes make new_ID the primary key
cur.execute('''CREATE TABLE nodes
                (new_ID INTEGER PRIMARY KEY,
                Original_ID INTEGER,
                X REAL,
                Y REAL,
                type TEXT)''')

# create a table for the links make new_ID the primary key
cur.execute('''CREATE TABLE links
                (new_ID INTEGER PRIMARY KEY,
                Original_ID INTEGER,
                start_node_ID INTEGER,
                end_node_ID INTEGER,
                Length REAL,
                type TEXT)''')

# add the data to the database
node_final.to_sql('nodes', conn, if_exists='append', index=False)
link_final.to_sql('links', conn, if_exists='append', index=False)

#add all the road, rail and water dfs to the database and make them relate through new_ID
road_link_df.to_sql('road_links', conn, if_exists='append', index=False)
rail_link_df.to_sql('rail_links', conn, if_exists='append', index=False)
water_link_df.to_sql('water_links', conn, if_exists='append', index=False)

road_nodes_df.to_sql('road_nodes', conn, if_exists='append', index=False)
rail_node_df.to_sql('rail_nodes', conn, if_exists='append', index=False)
water_nodes_df.to_sql('water_nodes', conn, if_exists='append', index=False)

# commit the changes
conn.commit()

# close the connection
conn.close()


# %%
#test the network where I want all the road links that have a start_node_ID of a certain node

conn = sqlite3.connect('network.db')

# create a cursor
cur = conn.cursor()

# create a query
query = '''SELECT * FROM road_links WHERE start_node_ID = 1'''

# execute the query
cur.execute(query)

# fetch the results
results = cur.fetchall()

# print the results
print(results)

conn.close()

# %%
