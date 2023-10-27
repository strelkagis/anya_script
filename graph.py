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

gdf = gpd.read_file('OSMB-d78e5ec15212d21fb09fe6310694f7763ae813e3.geojson')
gdf = gdf['geometry'].to_crs(4326)

graph = ox.graph.graph_from_polygon(gdf.iloc[0], network_type='all', simplify=True,
                                               retain_all= True, truncate_by_edge = True, clean_periphery = True)


gdf_nodes, gdf_edges = ox.graph_to_gdfs(
        graph,
        nodes=False, edges=True,
        node_geometry=True,
        fill_edge_geometry=True)

edges = gdf_edges.reset_index(inplace=False)
# nodes = gdf_nodes.reset_index(inplace=False)


# edges.to_postgis(f"_edges_graph_moskovskay", engine, schema = 'osm_graph', if_exists='replace')
edges.to_postgis(f"_nodes_graph_moskovskay", engine, schema = 'osm_graph', if_exists='replace')