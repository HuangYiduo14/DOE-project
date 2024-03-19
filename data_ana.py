import pandas as pd
import sqlite3
# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("unified_network.db")
cur = con.cursor()
cur.execute('''UPDATE nodes SET X=X*100000 WHERE type='waterway';''')
cur.execute('''UPDATE nodes SET Y=Y*100000 WHERE type='waterway';''')
cur.execute('''UPDATE water_nodes SET X=X*100000;''')
cur.execute('''UPDATE water_nodes SET Y=Y*100000;''')
con.commit()