#! /usr/bin/env python

import xml.etree.ElementTree as xml
import shapely.wkt as wkt


def read_trajectory_from_wkt(wkt_string):
    shape = wkt.loads(wkt_string)
    if shape.type != "LineString":
        raise ValueError("WKT must be a LineString, but is: {}".format(shape.type))
    return (
        {
            "lon": coord[0],
            "lat": coord[1],
        } for coord in shape.coords
    )


def read_trajectory_from_osm(osm_file, way_id):
    root = xml.parse(osm_file).getroot()
    way = root.find(".//way[@id='{}']".format(way_id))
    trajectory = list()
    for nd in way:
        node_id = nd.attrib.get("ref")
        node = root.find(".//node[@id='{}']".format(node_id)) 
        trajectory.append({
            "lat": float(node.attrib.get("lat")),
            "lon": float(node.attrib.get("lon"))
        })
    return trajectory
