import pandana
import osmnx as ox
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import geoalchemy2
import numpy as np

# функция для чтения того, что получили в запросе
def select_pg(sql):
    return pd.read_sql(sql, engine)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_graph')

#узлы графа
node_graph = select_pg('select id, \
                       st_x(st_transform(the_geom,4326)) as lng, \
                        st_y(st_transform(the_geom,4326)) as lat \
                       from edge_msk_spb_vertices_pgr')

#ребра графа
edge_graph = select_pg('select id, source, target, distant from edge_msk_spb')

# Данная функция фильтрует граф ребер (edge_graph) и оставляет только те ребра, 
# у которых исходная (source) и целевая (target) вершины присутствуют в графе вершин (node_graph).
edge_graph = edge_graph[edge_graph['source'].isin(node_graph['id']) & edge_graph['target'].isin(node_graph['id'])]
#нужно установить индекс, чтобы создался граф
node_graph.set_index('id', inplace= True)


#задаем рандомую скорость (так как мы берем в расчет расстояние)
edge_graph['speed_kph'] = 40

# создаем граф pandana
graph = pandana.Network(node_graph['lng'], node_graph['lat'], 
                          edge_graph['source'], edge_graph['target'], edge_graph[['speed_kph']], twoway = False)

#читаем файл с предварительно обработанными фотографиями 
# (поделенными на кластеры, поделенными на туристы и не туристы)
df = pd.read_csv('C:/Users/apikuleva/Documents/photo/photo_in_buffer_4326')

# функция для построения кратчайшего маршрута между двумя точками 
def my_function(row):
    path = graph.shortest_path(row['source'],row['target'])
    return path

#ближайшие узлы на графе
df['nodes'] = graph.get_node_ids(df.lng, df.lat).values
# Удаление дубликатов
photo = df.drop_duplicates(subset=['nodes', 'owner_id', 'cluster'], keep='first')
# Группировка по 'owner_id' и 'cluster' с вычислением размаха значений 'nodes'
filter = photo.groupby(['owner_id', 'cluster'])['nodes'].agg(np.ptp)
filter = filter.reset_index()
# filter[filter['nodes'] == 0]
# Группировка по двум столбцам и сложение значений третьего столбца в массив
grouped = photo.groupby(['owner_id', 'cluster']).agg({'nodes': list})
grouped = grouped.reset_index()

# оставляем только те строки, там где разные узлы в одном маршруте
merged = grouped.merge(filter[filter['nodes'] == 0], on=['owner_id', 'cluster'], how='left', indicator=True)
filtered = merged[merged['_merge'] == 'left_only']
result = filtered.drop(columns='_merge')

# Группировка по 'owner_id' и 'cluster' с вычислением размаха значений 'nodes'
count = df.groupby(['owner_id', 'cluster'])['nodes'].count()
count = count.reset_index()
count_two = count[count['nodes'] == 2]
count_more_two = count[count['nodes'] > 2]

count_two_merged = count_two.merge(filter[filter['nodes'] == 0], on=['owner_id', 'cluster'], how='left', indicator=True)
count_two_filtered = count_two_merged[count_two_merged['_merge'] == 'left_only']
count_two_result = count_two_filtered.drop(columns='_merge')

two_nodes = result.merge(count_two_result, on=['owner_id', 'cluster'], how='left', indicator=True)
two_nodes_filtered = two_nodes[two_nodes['_merge'] == 'both']
two_nodes_result = two_nodes_filtered.drop(columns='_merge')
# Разделение значений массивов на несколько колонок
two_nodes_result[['target', 'source']] = two_nodes_result['nodes_x_x'].apply(pd.Series)

many_nodes = result.merge(count_two_result, on=['owner_id', 'cluster'], how='left', indicator=True)
many_nodes_filtered = many_nodes[many_nodes['_merge'] == 'left_only']
many_nodes_result = many_nodes_filtered.drop(columns='_merge')



# many_nodes_result['nodes_x_x'] = many_nodes_result.progress_apply(my_function_many, axis=1)
two_nodes_result['path'] = two_nodes_result.progress_apply(my_function, axis=1)
two_nodes_result.to_csv('two_nodes_result.csv')

# data = list(zip(nodes_list_2, origin_nodes_list, dests_nodes_list))
# path = pd.DataFrame(data, columns=["node", "source", "target"])
two_nodes_result_explode = two_nodes_result.explode("path", ignore_index=True)
# path.dropna(subset=["node"], inplace=True)
two_nodes_result_explode["node"] = two_nodes_result_explode["node"].astype(int)
# two_nodes_result_explode.to_csv('path200_250.csv', index=False)
two_nodes_result_explode.to_sql('two_nodes_result_explode', engine, if_exists='replace', index=False)


