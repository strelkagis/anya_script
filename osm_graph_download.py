import osmnx as ox
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

# функция для чтения того, что получили в запросе
def select_pg(sql):
    return pd.read_sql(sql, engine)

# функция для изменения значений в бд
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = psycopg2.connect(
        database=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )
    return connection

russia_region = gpd.read_file('OSMB-2e24310da60e6cf852ac9641bf21ef747c0a7383.geojson/OSMB-2e24310da60e6cf852ac9641bf21ef747c0a7383.geojson')
russia_region.name_en = russia_region.name_en.replace(' ', '_', regex=True)
# russia_region = russia_region.to_crs(4326)
print(russia_region.geometry.crs)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_arcgis')

for i,row in russia_region.iterrows():

        print(row.name_en)
        # print(row.geometry.crs)

        G = ox.graph_from_polygon(row['geometry'], network_type='all', simplify=True, truncate_by_edge = True, retain_all = False, clean_periphery =True)
        G = ox.project_graph(G, 3857)
        # G = ox.simplify_graph(G, tolerance=5)

        hwy_speeds = {"motorway": 110, "primary": 90, "primary_link": 90, "trunk": 90, "secondary": 90, "trunk_link": 90, \
                      "secondary_link": 90, "tertiary": 60, "tertiary_link": 60, "residential": 40, "road": 40,
                      "unclassified": 20, \
                      "living_street": 20, "rest_area": 20}


        G = ox.add_edge_speeds(G, hwy_speeds, fallback=4.5)
        G = ox.add_edge_travel_times(G)
        gdf_nodes, gdf_edges = ox.graph_to_gdfs(G)
        gdf_nodes = gdf_nodes.reset_index()
        gdf_edges = gdf_edges.reset_index()
        gdf_nodes.to_postgis(f"graph_node_{row.name_en}", engine, schema = 'fo_graph')
        gdf_edges.to_postgis(f"graph_edges_{row.name_en}", engine, schema = 'fo_graph')
        # name_list.append(row.name_ru)