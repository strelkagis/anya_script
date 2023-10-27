import geopandas as gpd
import momepy
import osmnx as ox
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

def select_pg(sql):
    return gpd.read_postgis(sql, engine)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_arcgis')

cf ='["highway"~"bridleway|corridor|elevator|footway| \
    living_street|path|pedestrian|residential| \
    secondary|secondary_link|service|steps|track|unclassified"]'

place = 'Москва, Россия'
G = ox.graph_from_place(place, network_type='walk', buffer_dist=2000,truncate_by_edge=True,simplify=False,custom_filter=cf)

gdf_nodes, gdf_edges = ox.graph_to_gdfs(G)
gdf_nodes = gdf_nodes.reset_index()
gdf_edges = gdf_edges.reset_index()

gdf_nodes.to_postgis(f"moskow_nodes_pesh", engine, if_exists = 'replace', schema = 'osm_graph')
gdf_edges.to_postgis(f"moskow_edges_pesh", engine, if_exists = 'replace', schema = 'osm_graph')
