DOE projects
The code need to be used with "unified_network.db". Due to the size limit, after pulling, you should manually place the file in your working project folder.
All_Nodes -> columns = ['new_ID', 'Original_ID' , 'X', 'Y', 'type']
All_links -> columns = ['new_ID', 'Original_ID', 'start_node_ID', 'end_node_ID', 'length', 'type']
where type -> {'road', 'waterway', 'rail'}
See "db_col_name.csv" for the full db details.

"data_ana.py": pre-process waterway data so that X,Y unit is the same from different data files. Only need to run once.
"merge_nodes.py": merge nodes to create OD nodes. a new column 'set_id' is added to table 'nodes' indicating the clustering results.
