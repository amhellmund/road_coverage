#! /usr/bin/env python

import argparse
import copy
import logging
import mysql.connector
import yaml

from contextlib import contextmanager

import os
import sqlite3
import sys

from geopy.distance import distance
from osmium import SimpleHandler


def main(args):
    config = _load_configuration(args.config_file)
    with _connect_to_database(config["mysql"]) as dbcon:
        _prepare_database(dbcon, args.clear_database)
        _import_osm_into_database(dbcon, config, args.input_file)


def _load_configuration(config_file):
    logging.info("Loading configuration from file {}".format(config_file))
    with open(config_file, "r") as file_stream:
        return yaml.safe_load(file_stream)


@contextmanager
def _connect_to_database(config):
    logging.info("Connecting to MySQL database")
    with mysql.connector.connect(
        host=config["host"],
        database=config["database"],
        user=config["user"],
        password=config["password"]
    ) as dbcon:
        yield dbcon


TABLE_CONFIGURATIONS = {
    "ways": (
        ("way_id", "BIGINT"),
        ("road_type", "TINYINT"),
        ("ref", "VARCHAR(256)"),
        ("name", "VARCHAR(256)"),
        ("lanes", "TINYINT"),
        ("maxspeed", "SMALLINT"),
        ("oneway", "BOOL"),
        ("tunnel", "BOOL"),
    ),
    "way_node_ids": (
        ("way_id", "BIGINT"),
        ("idx", "SMALLINT"),
        ("node_id", "BIGINT"),
    ),
}


def _prepare_database(dbcon, clear_database=False):
    if clear_database:
        _clear_database(dbcon)
    _setup_tables(dbcon)


def _clear_database(dbcon):
    logging.info("Clearing the database")
    with dbcon.cursor() as cursor:
        for table in TABLE_CONFIGURATIONS:
            cursor.execute("DROP TABLE IF EXISTS {table}".format(table=table))


def _setup_tables(dbcon):
    logging.info("Setting up tables")
    with dbcon.cursor() as cursor:
        for table, column_specs in TABLE_CONFIGURATIONS.items():
            column_string = ",".join(
                ["{} {}".format(name, spec) for name, spec in column_specs])
            cursor.execute("CREATE TABLE IF NOT EXISTS {table} ({columns})".format(
                table=table, columns=column_string))


def _import_osm_into_database(dbcon, config, input_file):
    logging.info("Importing OSM data into database")
    node_ids = _import_osm_highways(dbcon, config["import"], input_file)


def _import_osm_highways(dbcon, config, input_file):
    logging.info("Importing highways: {}".format(
        ", ".join(config["highway"].keys())))
    osm_handler = OsmHighwayHandler(dbcon, config["highway"])
    osm_handler.apply_file(input_file)
    osm_handler.finalize()
    return osm_handler.node_ids


class OsmHighwayHandler(SimpleHandler):
    CACHE_SIZE = 100

    def __init__(self, dbcon, highway_types):
        SimpleHandler.__init__(self)
        self.dbcon = dbcon
        self.highway_types = highway_types
        self.node_ids = set()
        self._init_cache()

    def _init_cache(self):
        self.data_cache = {
            "size": 0,
            "ways": [],
            "way_node_ids": []
        }

    def way(self, way):
        highway = way.tags.get("highway", None)
        if highway in self.highway_types:
            node_ids = self._add_way_to_cache(way, highway)
            self.node_ids.update(node_ids)

            if self.data_cache["size"] >= self.CACHE_SIZE:
                self._write_cache_to_database()

    def finalize(self):
        self._write_cache_to_database()

    def _add_way_to_cache(self, way, highway_type):
        node_ids = [copy.copy(node.ref) for node in way.nodes]
        way_id = copy.copy(way.id)
        self.data_cache["ways"].append((
            way_id,
            self.highway_types[highway_type],
            get_tag(way.tags, "ref", None),
            get_tag(way.tags, "name", None),
            get_tag(way.tags, "lanes", None, int),
            get_maxspeed(way.tags),
            is_oneway(way.tags),
            is_tunnel(way.tags),
        ))
        self.data_cache["way_node_ids"].extend((
            way_id, index, node_id
        ) for index, node_id in enumerate(node_ids))
        self.data_cache["size"] += 1
        return node_ids

    def _write_cache_to_database(self):
        self.dbcon.start_transaction()
        with self.dbcon.cursor() as cursor:
            self._write_ways_to_database(cursor)
            self._write_way_node_ids_to_database(cursor)
        self.dbcon.commit()
        self._init_cache()

    def _write_ways_to_database(self, cursor):
        column_names = [name for name, _ in TABLE_CONFIGURATIONS["ways"]]
        cursor.executemany("INSERT INTO {table} ({columns}) VALUES ({values})".format(
            table="ways",
            columns=",".join(column_names),
            values=",".join(["%s"] * len(column_names))
        ), self.data_cache["ways"])

    def _write_way_node_ids_to_database(self, cursor):
        column_names = [name for name,
                        _ in TABLE_CONFIGURATIONS["way_node_ids"]]
        cursor.executemany("INSERT INTO {table} ({columns}) VALUES ({values})".format(
            table="way_node_ids",
            columns=",".join(column_names),
            values=",".join(["%s"] * len(column_names))
        ), self.data_cache["way_node_ids"])


def get_tag(tags, key, default, type_name=str):
    return type_name(tags[key]) if key in tags and tags[key] != "none" else default


def get_maxspeed(tags):
    try:
        return int(tags["maxspeed"])
    except:
        return None


def is_oneway(tags):
    return "oneway" in tags and tags["oneway"] == "yes"


def is_tunnel(tags):
    return "tunnel" in tags and tags["tunnel"] == "yes"


#     logging.info("Processing OSM file: {}".format(args.input_file))
#     road_db = create_road_db(args.database_file)
#     node_ids = create_ways(road_db, args.input_file)
#     create_nodes(road_db, args.input_file, node_ids)
#     return
#     compute_way_lengths(ways, nodes)
#     store_database_to_disk(nodes, ways, args.database_file)

# class MotorwayNodeHandler(SimpleHandler):
#     def __init__(self, motorway_node_ids):
#         SimpleHandler.__init__(self)
#         self.node_ids = motorway_node_ids
#         self.nodes = dict()

#     def node(self, node):
#         if node.id in self.node_ids:
#             self.nodes[node.id] = {
#                 "lat": node.location.lat,
#                 "lon": node.location.lon
#             }

# def create_nodes(input_file, motorway_node_ids):
#     logging.info(
#         "Extracting OSM nodes for motorways (including on-/off-ramps)")
#     handler = MotorwayNodeHandler(motorway_node_ids)
#     handler.apply_file(input_file)
#     return handler.nodes

# def compute_way_lengths(ways, nodes):
#     logging.info("Compute way and segment lengths")
#     def compute_distance(o, t): return distance(
#         (o["lat"], o["lon"]), (t["lat"], t["lon"])).km
#     for way in ways.values():
#         nodes_in_way = [nodes[id] for id in way["nodes"]]
#         way["segments"] = [compute_distance(
#             nodes_in_way[i], nodes_in_way[i+1]) for i in range(len(nodes_in_way) - 1)]
#         way["length"] = sum(way["segments"])

# def store_database_to_disk(nodes, ways, database_file):
#     logging.info("Write database to disk as file {}".format(database_file))
#     with open(database_file, "wb") as file_stream:
#         pickle.dump({
#             "nodes": nodes,
#             "ways": ways
#         },
#             file_stream)

# def create_road_db(database_file):
#     logging.info("Create SQlite database {}".format(database_file))
#     if os.path.exists(database_file):
#         raise ValueError(
#             "The road database '{}' already exists".format(database_file))
#     road_db = sqlite3.connect(database_file)
#     road_db.execute("PRAGMA synchronous = NORMAL")
#     road_db.execute("PRAGMA journal_mode = MEMORY")
#     road_db.execute(
#         "CREATE TABLE ways (id INTEGER, ref TEXT, name TEXT, type TEXT, oneway INTEGER, maxspeed INTEGER, lanes INTEGER, tunnel INTEGER)")
#     road_db.execute("CREATE TABLE way_nodes (way_id INTEGER, node_id INTEGER)")
#     road_db.execute(
#         "CREATE TABLE way_segments (way_id INTEGER, segment INTEGER, length REAL)")
#     road_db.execute("CREATE TABLE way_length (way_id INTEGER, length REAL)")
#     road_db.execute(
#         "CREATE TABLE nodes (id INTEGER, latitude REAL, longitude REAL)")
#     road_db.commit()
#     return road_db

# def create_ways(road_db, input_file):
#     logging.info(
#         "Extracting OSM ways and node ids for motorways (including on-/off-ramps)")
#     handler = MotorwayWayHandler(road_db)
#     handler.apply_file(input_file)
#     return handler.node_ids

# class MotorwayWayHandler(SimpleHandler):
#     def __init__(self, road_db):
#         SimpleHandler.__init__(self)
#         self.road_db = road_db
#         self.node_ids = set()

#     def way(self, way):
#         highway = way.tags.get("highway", None)
#         if highway in ["motorway", "motorway_link"]:
#             node_ids = [int(node.ref) for node in way.nodes]
#             self.road_db.execute("INSERT INTO ways VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
#                                  (
#                                      way.id,
#                                      get_tag(way.tags, "ref", None),
#                                      get_tag(way.tags, "name", None),
#                                      HIGHWAY_TYPE_MAPPING[highway],
#                                      is_oneway(way.tags),
#                                      get_tag(way.tags, "maxspeed", None, int),
#                                      get_tag(way.tags, "lanes", None, int),
#                                      is_tunnel(way.tags)
#                                  )
#                                  )
#             self.road_db.executemany("INSERT INTO way_nodes VALUES (?, ?)", [
#                                      (way.id, node_id) for node_id in node_ids])
#             self.road_db.commit()
#             self.node_ids.update(node_ids)

# def get_tag(tags, key, default, type_name=str):
#     return type_name(tags[key]) if key in tags else default

# def is_oneway(tags):
#     return "oneway" in tags and tags["oneway"] == "yes"

# def is_tunnel(tags):
#     return "tunnel" in tags and tags["tunnel"] == "yes"

# def create_nodes(road_db, input_file, motorway_node_ids):
#     logging.info(
#         "Extracting OSM nodes for motorways (including on-/off-ramps)")
#     handler = MotorwayNodeHandler(road_db, motorway_node_ids)
#     handler.apply_file(input_file)
#     handler.finalize()

# class MotorwayNodeHandler(SimpleHandler):
#     def __init__(self, road_db, motorway_node_ids):
#         SimpleHandler.__init__(self)
#         self.road_db = road_db
#         self.node_ids = motorway_node_ids
#         self.cache = []
#     def node(self, node):
#         if node.id in self.node_ids:
#             self.cache.append((node.id, node.location.lat, node.location.lon))
#             if len(self.cache) > 10000:
#                 self.road_db.executemany(
#                     "INSERT INTO nodes VALUES (?, ?, ?)", self.cache)
#                 self.cache = []
#     def finalize(self):
#         if self.cache:
#             self.road_db.executemany(
#                 "INSERT INTO nodes VALUES (?, ?, ?)", self.cache)
#         self.road_db.commit()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Motorway Database Generator")
    parser.add_argument("config_file", metavar="CONFIG_FILE",
                        help="The import configuration")
    parser.add_argument("input_file", metavar="OSM_FILE",
                        help="The input OSM file")
    parser.add_argument("--clear-database", help="Clear the database before import",
                        action="store_true", default=False)
    args = parser.parse_args()

    logging.basicConfig(
        format="%(levelname)s: %(message)s", level=logging.INFO)
    main(args)
