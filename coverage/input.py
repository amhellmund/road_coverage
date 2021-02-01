#! /usr/bin/env python

import argparse

import xml.etree.ElementTree as xml


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSM Way Reader")
    parser.add_argument("osm_file", metavar="OSM_FILE", help="The OSM file")
    parser.add_argument("way_id", metavar="WAY_ID", type=int, help="The way id")
    args = parser.parse_args()
    print(read_trajectory_from_osm(args.osm_file, args.way_id))
