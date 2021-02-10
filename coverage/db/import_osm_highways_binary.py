#! /usr/bin/env python

import argparse
import logging
import os
import pickle
from geopy.distance import distance
from osmium import SimpleHandler


logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


class MotorwayWayHandler(SimpleHandler):
    def __init__(self):
        SimpleHandler.__init__(self)
        self.motorway_node_ids = set()
        self.motorway_ways = dict()

    def way(self, way):
        highway = way.tags.get("highway", None)
        if highway in ["motorway", "motorway_link"]:
            node_ids = [node.ref for node in way.nodes]
            self.motorway_ways[way.id] = {
                "ref": way.tags.get("ref"),
                "name": way.tags.get("name"),
                "type": highway,
                "oneway": way.tags.get("oneway"),
                "maxspeed": way.tags.get("maxspeed"),
                "lanes": way.tags.get("lanes"),
                "tunnel": way.tags.get("tunnel"),
                "nodes": node_ids
            }
            self.motorway_node_ids.update(node_ids)


def create_ways(input_file):
    logging.info(
        "Extracting OSM ways and node ids for motorways (including on-/off-ramps)")
    handler = MotorwayWayHandler()
    handler.apply_file(input_file)
    return handler.motorway_ways, handler.motorway_node_ids


class MotorwayNodeHandler(SimpleHandler):
    def __init__(self, motorway_node_ids):
        SimpleHandler.__init__(self)
        self.node_ids = motorway_node_ids
        self.nodes = dict()

    def node(self, node):
        if node.id in self.node_ids:
            self.nodes[node.id] = {
                "lat": node.location.lat,
                "lon": node.location.lon
            }


def create_nodes(input_file, motorway_node_ids):
    logging.info(
        "Extracting OSM nodes for motorways (including on-/off-ramps)")
    handler = MotorwayNodeHandler(motorway_node_ids)
    handler.apply_file(input_file)
    return handler.nodes


def compute_way_lengths(ways, nodes):
    logging.info("Compute way and segment lengths")
    def compute_distance(o, t): return distance(
        (o["lat"], o["lon"]), (t["lat"], t["lon"])).km
    for way_id, way in ways.items():
        nodes_in_way = [nodes[id] for id in way["nodes"]]
        way["segments"] = [compute_distance(
            nodes_in_way[i], nodes_in_way[i+1]) for i in range(len(nodes_in_way) - 1)]
        way["length"] = sum(way["segments"])


def store_database_to_disk(nodes, ways, database_file):
    logging.info("Write database to disk as file {}".format(database_file))
    with open(database_file, "wb") as file_stream:
        pickle.dump({
            "nodes": nodes,
            "ways": ways
        },
            file_stream)


def main(args):
    logging.info("Processing OSM file: {}".format(args.input_file))
    ways, node_ids = create_ways(args.input_file)
    nodes = create_nodes(args.input_file, node_ids)
    compute_way_lengths(ways, nodes)
    store_database_to_disk(nodes, ways, args.database_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Motorway Database Generator")
    parser.add_argument("input_file", metavar="OSM_FILE",
                        help="The input OSM file")
    parser.add_argument("database_file", metavar="OUTPUT_FILE",
                        help="The database binary file")
    args = parser.parse_args()

    main(args)
