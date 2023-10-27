import osmnx as ox
import geopandas as gpd
from shapely.geometry import Polygon, Point
import pandas as pd

import numpy as np

def select_pg(sql):
    return gpd.read_postgis(sql, engine)

engine = create_engine('postgresql+psycopg2://kb_geo:kYZQK90yvE8aNi54ELINc0yJ1gu6wo7h@\
db03.cluster.strlk.ru/kb_arcgis')

poligon = gpd.read_postgis('SELECT * FROM index_city.clip_geom', engine, geom_col = 'geomerty')

tags = {'natural': 'water'}
object = ox.geometries.geometries_from_place(place,tags, which_result=None, buffer_dist=300)
object = object.reset_index()
object = object.to_crs(3857)

# tags_railway_road = '["highway"~"primary|primary_link|secondary|secondary_link"]'

tags_road = '["highway"~"primary|primary_link|secondary|secondary_link"]'
road = ox.graph_from_place(place, simplify=True, retain_all=False, truncate_by_edge=False, which_result=None, buffer_dist=None, clean_periphery=True, custom_filter=tags_road)
road = ox.project_graph(road, 3857)
gdf_nodes, gdf_edges = ox.graph_to_gdfs(road)
# gdf_edges.geometry.to_file('gdf_edges_.geojson', driver='GeoJSON')

# geometry_line = object[(object.geometry.geom_type == 'LineString')]['geometry']


railway = ox.graph_from_place('Москва',
                        retain_all=True, truncate_by_edge=True, simplify=True,
                        custom_filter='["railway"~"tram|rail"]')
railway = ox.project_graph(railway, 3857)
railway_nodes, railway_edges = ox.graph_to_gdfs(railway)
# railway_edges.geometry.to_file('railway_edges_.geojson', driver='GeoJSON')

# geometry_line.to_file('river.geojson', driver='GeoJSON')
geometry_poly = object[(object.geometry.geom_type=='Polygon')]['geometry'].boundary

all_geometry = geometry_poly.union(railway_edges.geometry, align=False).union(gdf_edges.geometry, align=False)

all_geometry.to_file('all_geometry.geojson_', driver='GeoJSON')
# print(object.geometry.geom_type)