import osmnx as ox
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import numpy as np
import scipy
from shapely.geometry import Point, LineString, Polygon
import networkx as nx




# создание новых точек на линии каждые 20 метров
def cut(line):
    new_points = []
    lines =[]
    distance = 100
    line_gpd = gpd.GeoDataFrame(geometry=line, crs="EPSG:3857")
    for i in range(int(line_gpd.geometry.length // distance)):
        point = line_gpd.interpolate(i * distance)
        new_points.append(point)

    # создание новой линии из новых точек
    new_line = LineString(new_points)
    # разделение линии на части длиной в 20 метров
    lines.append(new_line.split())
    # lines_gpd = gpd.GeoSeries(geometry = lines.centroid)
    point = gpd.GeoDataFrame(geometry=lines.centroid, crs="EPSG:3857")
    return point


def create_isohrone(path, type, time):

    import osmnx as ox
    import geopandas as gpd
    from sqlalchemy import create_engine
    import psycopg2
    import pandas as pd
    import numpy as np
    import scipy
    from shapely.geometry import Point, LineString, Polygon
    import networkx as nx

    network_type = type

    if network_type == 'walk':
        travel_speed = 5
        cf = '["highway"~"bridleway|corridor|elevator|footway| \
    living_street|path|pedestrian|residential| \
    secondary|secondary_link|service|steps|track|unclassified|proposed"]'
    else:
        travel_speed = 40
        cf = None

    # прочитали файл
    poly = gpd.read_file(path)
    print(poly)
    poly = poly.to_crs(3857)
    poly['area'] = poly.geometry.area
    # poly['geometry'] = poly.geometry.buffer(1)
    # gdf.to_file('datttgtv.geojson', driver='GeoJSON')

    # загрузили граф
    if poly.shape[0] != 1:
        geom_from_graph = poly.geometry.unary_union.convex_hull.buffer(time*travel_speed*1000/60*5)
        gdf = gpd.GeoDataFrame(index=[0], crs='epsg:3857', geometry=[geom_from_graph])
        gdf = gdf.to_crs(4326)
        G = ox.graph_from_polygon(gdf['geometry'].iloc[0], network_type=network_type, simplify=True, retain_all=False,
                                  truncate_by_edge=True, clean_periphery=True, custom_filter = cf)
        G = ox.project_graph(G, 3857)
    else:
        geom_from_graph = poly.geometry.buffer(time*travel_speed*1000/60*2)
        gdf = gpd.GeoDataFrame(index=[0], crs='epsg:3857', geometry=geom_from_graph)
        gdf = gdf.to_crs(4326)
        G = ox.graph_from_polygon(gdf['geometry'].iloc[0], network_type=network_type, simplify=True, retain_all=False,
                                  truncate_by_edge=True, clean_periphery=True, custom_filter = cf)
        G = ox.project_graph(G, 3857)


    # перевели скорость в метры в секунду
    meters_per_minute = travel_speed * 1000 / 60  # km per hour to m per minute
    for _, _, _, data in G.edges(data=True, keys=True):
        data["time"] = data["length"] / meters_per_minute

    # цикл по каждому полигону
    for i, row in poly.iterrows():
        # имя полигона
        name = row.name

        if row.area > 100000:
            # если площадь больше 10000, то строим границу линии делим на части
            result = cut(row.geometry.boundary)
            print(result)


create_isohrone("RZHD_Redevelopment_territories (2).gpkg", 'walk', 30)