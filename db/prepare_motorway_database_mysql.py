#! /usr/bin/env python

import argparse
import logging
import os
import sqlite3

from geopy.distance import distance
from osmium import SimpleHandler


logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

HIGHWAY_TYPE_MAPPING = {
    "motorway": 1,
    "motorway_link": 2
}


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
    for way in ways.values():
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
    road_db = create_road_db(args.database_file)
    node_ids = create_ways(road_db, args.input_file)
    create_nodes(road_db, args.input_file, node_ids)
    return
    compute_way_lengths(ways, nodes)
    store_database_to_disk(nodes, ways, args.database_file)


def create_road_db(database_file):
    logging.info("Create SQlite database {}".format(database_file))
    if os.path.exists(database_file):
        raise ValueError(
            "The road database '{}' already exists".format(database_file))
    road_db = sqlite3.connect(database_file)
    road_db.execute("PRAGMA synchronous = NORMAL")
    road_db.execute("PRAGMA journal_mode = MEMORY")
    road_db.execute(
        "CREATE TABLE ways (id INTEGER, ref TEXT, name TEXT, type TEXT, oneway INTEGER, maxspeed INTEGER, lanes INTEGER, tunnel INTEGER)")
    road_db.execute("CREATE TABLE way_nodes (way_id INTEGER, node_id INTEGER)")
    road_db.execute(
        "CREATE TABLE way_segments (way_id INTEGER, segment INTEGER, length REAL)")
    road_db.execute("CREATE TABLE way_length (way_id INTEGER, length REAL)")
    road_db.execute(
        "CREATE TABLE nodes (id INTEGER, latitude REAL, longitude REAL)")
    road_db.commit()
    return road_db


def create_ways(road_db, input_file):
    logging.info(
        "Extracting OSM ways and node ids for motorways (including on-/off-ramps)")
    handler = MotorwayWayHandler(road_db)
    handler.apply_file(input_file)
    return handler.node_ids


class MotorwayWayHandler(SimpleHandler):
    def __init__(self, road_db):
        SimpleHandler.__init__(self)
        self.road_db = road_db
        self.node_ids = set()

    def way(self, way):
        highway = way.tags.get("highway", None)
        if highway in ["motorway", "motorway_link"]:
            node_ids = [int(node.ref) for node in way.nodes]
            self.road_db.execute("INSERT INTO ways VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                 (
                                     way.id,
                                     get_tag(way.tags, "ref", None),
                                     get_tag(way.tags, "name", None),
                                     HIGHWAY_TYPE_MAPPING[highway],
                                     is_oneway(way.tags),
                                     get_tag(way.tags, "maxspeed", None, int),
                                     get_tag(way.tags, "lanes", None, int),
                                     is_tunnel(way.tags)
                                 )
                                 )
            self.road_db.executemany("INSERT INTO way_nodes VALUES (?, ?)", [
                                     (way.id, node_id) for node_id in node_ids])
            self.road_db.commit()
            self.node_ids.update(node_ids)


def get_tag(tags, key, default, type_name=str):
    return type_name(tags[key]) if key in tags else default


def is_oneway(tags):
    return "oneway" in tags and tags["oneway"] == "yes"


def is_tunnel(tags):
    return "tunnel" in tags and tags["tunnel"] == "yes"


def create_nodes(road_db, input_file, motorway_node_ids):
    logging.info(
        "Extracting OSM nodes for motorways (including on-/off-ramps)")
    handler = MotorwayNodeHandler(road_db, motorway_node_ids)
    handler.apply_file(input_file)
    handler.finalize()


class MotorwayNodeHandler(SimpleHandler):
    def __init__(self, road_db, motorway_node_ids):
        SimpleHandler.__init__(self)
        self.road_db = road_db
        self.node_ids = motorway_node_ids
        self.cache = []

    def node(self, node):
        if node.id in self.node_ids:
            self.cache.append((node.id, node.location.lat, node.location.lon))
            if len(self.cache) > 10000:
                self.road_db.executemany(
                    "INSERT INTO nodes VALUES (?, ?, ?)", self.cache)
                self.cache = []

    def finalize(self):
        if self.cache:
            self.road_db.executemany(
                "INSERT INTO nodes VALUES (?, ?, ?)", self.cache)
        self.road_db.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Motorway Database Generator")
    parser.add_argument("database_config", metavar="CONFIG_FILE",
                        help="The database configuration")
    parser.add_argument("input_file", metavar="OSM_FILE",
                        help="The input OSM file")

    parser.add_argument("")
    args = parser.parse_args()

    main(args)
