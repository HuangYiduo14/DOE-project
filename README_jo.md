

JO: 03/13/2024
- processing_road_network.py: creates two csv files. One for road nodes and road links. The road network needs to be processed because there are no unique identifiers for road nodes. I am taking the extreme points of each road link because some links are composed of many smaller links for curves. The outputs are link_gdf.csv and node_gdf.csv.


- create_database.py: combines all the data from Rail, Waterway and Roadway datasets into a unified dataframe for nodes and links with unique "New_ID" identifiers. Then, it builds a very simple sql database. 