# -*- coding: utf-8 -*-
"""path_photo (1).ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/18ViLpx7BkpNBQ3uHyV9oqU7YaC6dEKCx
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

sql = "SELECT geom, id FROM public.buffered "
attractor = select_pg(sql, 'geom')
# poi = gpd.GeoDataFrame(poi, geometry = poi['geom'], crs  = 3857)

edge_graph_moscow_spb = "SELECT * FROM public.edge_msk_spb "
node_graph_moscow_spb = "SELECT * FROM public.edge_msk_spb_vertices_pgr "

edge_graph_moscow_spb = "SELECT * FROM public.edge_msk_spb "
node_graph_moscow_spb = "SELECT * FROM public.edge_msk_spb_vertices_pgr "

node_graph = select_pg(node_graph_moscow_spb, 'the_geom')
edge_graph = select_pg(edge_graph_moscow_spb, 'geom')

node_graph['lng']= node_graph['the_geom'].to_crs(crs=4326).x
node_graph['lat']= node_graph['the_geom'].to_crs(crs=4326).y

edge_graph = edge_graph[edge_graph['source'].isin(node_graph['id']) & edge_graph['target'].isin(node_graph['id'])]
node_graph.set_index('id', inplace= True)

edge_graph['speed_kph'] = 40

# create network with pandana
graph = pandana.Network(node_graph['x'], node_graph['y'],
                          edge_graph['source'], edge_graph['target'], edge_graph[['speed_kph']])

node_graph['x']= node_graph['the_geom'].to_crs(crs=4326).x
node_graph['y']= node_graph['the_geom'].to_crs(crs=4326).y

node_graph

edge_graph = edge_graph[edge_graph['source'].isin(node_graph['id']) & edge_graph['target'].isin(node_graph['id'])]
node_graph.set_index('id', inplace= True)

edge_graph['speed_kph'] = 40

# create network with pandana
graph = pandana.Network(node_graph['x'], node_graph['y'],
                          edge_graph['source'], edge_graph['target'], edge_graph[['speed_kph']])

# функция для чтения того, что получили в запросе
def select_pg(sql, geom):
    return gpd.read_postgis(sql, engine, geom_col = geom, crs = 3857)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_geo')

photo = '''select id,
                id_gis,
                in_out,
                category,
                owner_id,
                date_time,
                st_transform(geom, 3857) as geom,
                city,
                user_region,
                photo_region,
                photo_city from strelka_data_russia.vk_photo_2022_meta where photo_region in
(
'Владимирская область',
'Архангельская область',
'Республика Карелия',
'Тульская область',
'Москва',
'Ярославская область',
'Ивановская область',
'Ленинградская область',
'Тверская область',
'Новгородская область',
'Псковская область',
'Нижегородская область',
'Вологодская область',
'Рязанская область',
'Костромская область',
'Смоленская область',
'Мурманская область',
'Санкт-Петербург',
'Калужская область',
'Московская область',
'Московская область')  and city != photo_city '''

photo = 'select * from vk_photo_2022_meta'
photo_df = select_pg(photo, 'geom')

photo_df = photo_df.to_crs(3857)

attractor = attractor.to_crs(3857)

photo_in_buffer = gpd.sjoin(photo_df, attractor, how='inner', predicate='intersects', lsuffix='')

photo_in_buffer = photo_in_buffer[['id_', 'id_gis', 'in_out', 'category', 'owner_id', 'date_time', 'geom', 'city', 'user_region', 'photo_region', 'photo_city']]

photo_in_buffer = photo_in_buffer.drop_duplicates()

photo_in_buffer = photo_in_buffer.dropna(subset=['city'])

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_geo')

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_graph')

photo_df.to_postgis('photo_spb_msk', engine, if_exists = 'replace')

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_graph')

photo_df.to_postgis('photo_df', engine, if_exists = 'replace')



# poi = poi.
photo_in_buffer = photo_in_buffer.sort_values(['owner_id','date_time'])

edge_graph['speed_kph'] = 40

#нашли блиажайший узел к каждой точке
photo_in_buffer['nx_node'], photo_in_buffer['dist'] = ox.distance.nearest_nodes(G_simplified, poi.lng,poi.lat, return_dist=True)

poi['lat_next'] = poi.lat.shift(-1)
poi['lng_next'] = poi.lng.shift(-1)

poi.drop(poi.tail(1).index,inplace=True) # drop last n rows

origin_nodes

origin_nodes = graph.get_node_ids(poi.lng, poi.lat).values
dests_nodes = graph.get_node_ids(poi.lng_next, poi.lat_next).values

poi['distances'] = pd.Series(graph.shortest_path_lengths(origin_nodes, dests_nodes))

poi.to_sql('poi_pskov', engine)

# ближайший узел для каждой точки
nodes_nearest = graph.get_node_ids(poi.lng, poi.lat).tolist()

nodes_list_2 = []
i_list_2 = []

for i in nodes_nearest:
    #кратчайший путь между точками
    k = graph.shortest_path(nodes_nearest[i], nodes_nearest[i+1])
# # print(k)
    nodes_list_2.append(k)
    i_list_2.append(i)

path = pd.DataFrame(list(zip(nodes_list_2, i_list_2)))

path = pd.DataFrame(list(zip(nodes_list_2, i_list_2)))
path = path.explode(0)
path[0] = pd.to_numeric(path[0], downcast='integer')
path.to_sql('path_pskov', engine)

path.to_sql('path_pskov', engine)

path = path.rename(columns={0: "node", 1: "nearest_node"})
# path.to_sql()

poi_with_path = path.merge(poi, how='inner', left_on='nearest_node', right_on='nx_node')

poi_with_path.to_sql('poi_with_path', engine)