import pandas as pd
import sqlite3
# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("unified_network.db")

size_new_nodes = pd.read_sql('select count(*) from nodes',con).values[0][0]
size_rail_nodes = pd.read_sql('select count(*) from rail_nodes',con).values[0][0]
size_road_nodes = pd.read_sql('select count(*) from road_nodes',con).values[0][0]
size_water_nodes = pd.read_sql('select count(*) from water_nodes',con).values[0][0]

print(size_new_nodes-size_road_nodes-size_rail_nodes-size_water_nodes)

