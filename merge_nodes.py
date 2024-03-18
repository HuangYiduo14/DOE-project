import pandas as pd
import sqlite3
import pickle
from utils import manhattan
from tqdm import tqdm

radius = 200 #<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< change parameter here
# connect sqlite and create index
conn = sqlite3.connect('unified_network.db')
cur = conn.cursor()
cur.execute('''CREATE INDEX IF NOT EXISTS index_X ON nodes(X)''')
cur.execute('''CREATE INDEX IF NOT EXISTS index_Y ON nodes(Y)''')
cur.execute('''CREATE INDEX IF NOT EXISTS index_XY ON nodes(X, Y)''')
cur.execute('''CREATE INDEX IF NOT EXISTS index_new_id ON nodes(new_ID)''') # create index to speedup searching
# get data from db
node_data = pd.read_sql("SELECT * FROM nodes WHERE NOT type='waterway'", conn)
node_data.set_index('new_ID', inplace=True)
if not ('set_id' in node_data.columns):
    cur.execute('''ALTER TABLE nodes ADD set_id INTEGER DEFAULT -1''')
    node_data['set_id'] = -1
conn.commit()

update = True
setid_to_id = {} # store set info in this dict {set_id: [node_id1, node_id2,...],...}
id_to_setid = {} # for reverse checkup {node_id: set_id,...}
for i in range(node_data.shape[0]):
    setid_to_id[i+1] = [i+1] # initialize: single-element node set for all the nodes
    id_to_setid[i+1] = i+1

# find neighbor for each node
neighbor_sets = {node_id: [] for node_id in node_data.index} # keep record of all the neighbors for all the nodes
print('create neighbor info')

try:
    neighbor_sets = pickle.load(open('neighbor_info_r{0}.p'.format(radius),'rb'))
    print('using existing neighbor data file')
except:
    print('creating new neighbor data file')
    for node_id in tqdm(node_data.index):
        x_i = node_data.loc[node_id, 'X']
        y_i = node_data.loc[node_id, 'Y']
        x_lb = x_i - radius  # search upper bound and lower bound: for each node, only consider nodes in a rectangle
        x_ub = x_i + radius
        y_lb = y_i - radius
        y_ub = y_i + radius
        sql_find_neighbor_one_node = '''
                    SELECT * FROM nodes
                    WHERE new_ID > {0} AND
                    X >= {1} AND X <= {2} AND Y >= {3} AND Y <= {4} AND (NOT type='waterway‘);
                '''.format(node_id, x_lb, x_ub, y_lb, y_ub) # only contains node_id>current_node_id
        neighbor_nodes = pd.read_sql(sql_find_neighbor_one_node, conn)
        neighbor_sets[node_id] = neighbor_nodes['new_ID'].tolist()
    pickle.dump(neighbor_sets, open('neighbor_info_r{0}.p'.format(radius),'wb'))

# merging process
while update:
    update = False
    print('one update')
    number_merges = 0
    for setid_i, node_set in tqdm(setid_to_id.items()):
        if node_set == [-1]:
              continue
        first_node_id = node_set[0]
        neighbor_first_node = neighbor_sets[first_node_id]
        for id_j in neighbor_first_node:
            setid_j = id_to_setid[id_j]
            all_nodes_in_neighbor_set = setid_to_id[setid_j]
            assert id_j in all_nodes_in_neighbor_set
            if setid_i == setid_j:
                continue
            distance_ij = []
            for node_i in node_set:
                for node_j in all_nodes_in_neighbor_set:
                    node_i_loc = [node_data.loc[node_i,'X'], node_data.loc[node_i,'Y']]
                    node_j_loc = [node_data.loc[node_j,'X'], node_data.loc[node_j,'Y']]
                    distance_ij.append(manhattan(node_i_loc, node_j_loc))
            if all(d<radius for d in distance_ij): # if we can merge two sets
                setid_to_id[setid_i] = node_set+all_nodes_in_neighbor_set
                setid_to_id[setid_j] = [-1]
                for idx in all_nodes_in_neighbor_set:
                    id_to_setid[idx] = setid_i
                update = True
                number_merges+=1
                break

    print('\n merges:',number_merges)


print('record changes to db')
for idx in tqdm(node_data.index):
    node_data.loc[idx,'set_id'] = id_to_setid[idx]
    cur.execute('''UPDATE nodes SET set_id = {0} WHERE new_ID = {1};'''.format(id_to_setid[idx], idx))
conn.commit()
node_data.to_csv('updated_nodes_with_sets.csv')
conn.close()

print('number of nodes:', node_data.columns[0])
print('number of sets:',node_data['set_id'].nunique())
