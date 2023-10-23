import pandana
import osmnx as ox
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import geoalchemy2
import numpy as np

# функция для чтения того, что получили в запросе
def select_pg(sql, geom):
    return gpd.read_postgis(sql, engine, geom_col = geom, crs = 3857)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_graph')

#аттракторы
sql = "SELECT geom, id FROM public.buffered "
attractor = select_pg(sql, 'geom')
attractor = attractor.to_crs(3857)

sql = "SELECT * FROM public.photo_in_buffer_new "
photo_in_buffer = select_pg(sql, 'geom')
photo_in_buffer = photo_in_buffer.to_crs(3857)
photo_in_buffer = photo_in_buffer.sort_values(['owner_id','date_time'])


edge_graph_moscow_spb = "SELECT * FROM public.all_edge_msk_spb_new "
node_graph_moscow_spb = "SELECT * FROM public.all_node_msk_spb_new "

node_graph = select_pg(node_graph_moscow_spb, 'the_geom')
edge_graph = select_pg(edge_graph_moscow_spb, 'geom')

node_graph['lng']= node_graph['the_geom'].to_crs(crs=4326).x
node_graph['lat']= node_graph['the_geom'].to_crs(crs=4326).y

edge_graph = edge_graph[edge_graph['source'].isin(node_graph['id']) & edge_graph['target'].isin(node_graph['id'])]
node_graph.set_index('id', inplace= True)

edge_graph['speed_kph'] = 40

# create network with pandana
graph = pandana.Network(node_graph['lng'], node_graph['lat'], 
                          edge_graph['source'], edge_graph['target'], edge_graph[['speed_kph']])


# photo_in_buffer
photo_in_buffer['lng']= photo_in_buffer['geom'].to_crs(crs=4326).x
photo_in_buffer['lat']= photo_in_buffer['geom'].to_crs(crs=4326).y

photo_in_buffer['lat_next'] = photo_in_buffer.lat.shift(-1)
photo_in_buffer['lng_next'] = photo_in_buffer.lng.shift(-1)

photo_in_buffer.drop(photo_in_buffer.tail(1).index,inplace=True) # drop last n rows 

origin_nodes = graph.get_node_ids(photo_in_buffer.lng, photo_in_buffer.lat).values
dests_nodes = graph.get_node_ids(photo_in_buffer.lng_next, photo_in_buffer.lat_next).values

nodes_list_2 = []
origin_nodes_list = []
dests_nodes_list = []

for i, k in zip(origin_nodes, dests_nodes):

    p = graph.shortest_path(i,k)
    nodes_list_2.append(p)
    origin_nodes_list.append(i)
    dests_nodes_list.append(k)
    


path = pd.DataFrame(list(zip(nodes_list_2, origin_nodes_list, dests_nodes_list)))
path = path.explode(0)
path[0] = pd.to_numeric(path[0], downcast='integer')
path.to_sql('path_msk_spb', engine, if_exists='replace')