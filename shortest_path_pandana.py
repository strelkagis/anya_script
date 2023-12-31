# -*- coding: utf-8 -*-
"""Untitled14.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1-loS1yWsV1_OUZuZPJA01gSKwlpmz4V5
"""

import pandana
import osmnx as ox
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import geoalchemy2

# функция для чтения того, что получили в запросе
def select_pg(sql, geom):
    return gpd.read_postgis(sql, engine, geom_col = geom, crs = 3857)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_graph')

sql = "SELECT geom, lng, lat, id, in_out, category, owner_id, date, city, home_town FROM public.photo_main "
poi = select_pg(sql, 'geom')
# poi = poi.
poi = poi.sort_values(['owner_id','date'])
# poi = gpd.GeoDataFrame(poi, geometry = poi['geom'], crs  = 3857)

edge_graph_moscow_spb = "SELECT * FROM public.edge_graph_moscow_spb "
node_graph_moscow_spb = "SELECT * FROM public.node_graph_moscow_spb "

node_graph = select_pg(node_graph_moscow_spb, 'geom')
edge_graph = select_pg(edge_graph_moscow_spb, 'geometry')

node_graph['x']= node_graph['geom'].to_crs(crs=4326).x
node_graph['y']= node_graph['geom'].to_crs(crs=4326).y

edge_graph = edge_graph[edge_graph['u'].isin(node_graph['osmid']) & edge_graph['v'].isin(node_graph['osmid'])]
node_graph.set_index('osmid', inplace= True)

edge_graph['speed_kph'] = 40

# create network with pandana
graph = pandana.Network(node_graph['x'], node_graph['y'],
                          edge_graph['u'], edge_graph['v'], edge_graph[['speed_kph']])

# ближайший узел для каждой точки
nodes_nearest = graph.get_node_ids(poi.lng, poi.lat).tolist()

nodes_nearest

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

len(nodes_nearest)-1

poi['lat_next'] = poi.lat.shift(-1)
poi['lng_next'] = poi.lng.shift(-1)

poi.drop(poi.tail(1).index,inplace=True) # drop last n rows

origin_nodes = graph.get_node_ids(poi.lng, poi.lat).tolist()
dests_nodes = graph.get_node_ids(poi.lng_next, poi.lat_next).tolist()

poi = poi.sort_values(['owner_id','date'])
# poi

graph.shortest_path(nodes_nearest[i], nodes_nearest[i+1])

poi['array'] = graph.shortest_path(poi['lat_next'],poi['array'])

origin_nodes

nodes_nearest[-1]

nodes_list_2 = []
i_list_2 = []

for i in nodes_nearest:
    if i != 999582:
        #кратчайший путь между точками
        k = graph.shortest_path(nodes_nearest[i], nodes_nearest[i+1])
    # # print(k)
        nodes_list_2.append(k)
        i_list_2.append(i)

nodes_list_2 = []
origin_nodes_list = []
dests_nodes_list = []
# k = 1



for i, k in zip(origin_nodes, dests_nodes):



    p = graph.shortest_path(i,k)
    nodes_list_2.append(p)
    origin_nodes_list.append(i)
    dests_nodes_list.append(k)

path = pd.DataFrame(list(zip(nodes_list_2, origin_nodes_list, dests_nodes_list)))

path.head(2)

path = path.explode(0)
path[0] = pd.to_numeric(path[0], downcast='integer')

path

path.to_sql('path_msk_spb', engine, if_exists='replace')