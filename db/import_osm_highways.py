#! /usr/bin/env python

import argparse
import copy
import logging
import math
import multiprocessing
import mysql.connector
import threading
import yaml

from contextlib import contextmanager
from geopy.distance import distance
from osmium import SimpleHandler

from table_config import TABLE_CONFIGURATIONS


def main(args):
    config = _load_configuration(args.config_file)
    with _connect_to_database(config["mysql"]) as dbcon:
        _prepare_database(dbcon, args)
        _import_osm_into_database(dbcon, config, args)
        _aggregate_ways(dbcon, config, args)


def _load_configuration(config_file):
    logging.info("Loading configuration from file {}".format(config_file))
    with open(config_file, "r") as file_stream:
        return yaml.safe_load(file_stream)


@contextmanager
def _connect_to_database(config, autocommit=True):
    logging.debug("Connecting to MySQL database")
    with mysql.connector.connect(
        host=config["host"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
        autocommit=autocommit
    ) as dbcon:
        yield dbcon


def _prepare_database(dbcon, args):
    if args.clear_database:
        _clear_database(dbcon, args)
    _setup_tables(dbcon)


def _clear_database(dbcon, args):
    stages = _get_stages_from_config(args)
    logging.info("Clearing databases: [{}]".format(", ".join(stages)))
    with dbcon.cursor() as cursor:
        for table, config in TABLE_CONFIGURATIONS.items():
            if config["stage"] in stages:
                cursor.execute(
                    "DROP TABLE IF EXISTS {table}".format(table=table))


def _get_stages_from_config(args):
    stages = list()
    if not args.skip_import:
        stages.append("import")
    if not args.skip_aggregation:
        stages.append("aggregation")
    if not args.skip_preparation:
        stages.append("preparation")
    return stages


def _setup_tables(dbcon):
    logging.info("Setting up tables")
    with dbcon.cursor() as cursor:
        for table, config in TABLE_CONFIGURATIONS.items():
            column_string = ",".join(
                ["{} {}".format(name, spec) for name, spec in config["columns"]])
            cursor.execute("CREATE TABLE IF NOT EXISTS {table} ({columns})".format(
                table=table, columns=column_string))


def _import_osm_into_database(dbcon, config, args):
    if args.skip_import:
        return

    logging.info("Importing OSM data into database")
    node_ids = _import_osm_highways(dbcon, config["import"], args.input_file)
    _import_osm_nodes(dbcon, node_ids, args.input_file)


def _import_osm_highways(dbcon, config, input_file):
    logging.info("Importing highways: [{}]".format(
        ", ".join(config["highway"].keys())))
    osm_handler = OsmHighwayHandler(dbcon, config["highway"])
    osm_handler.apply_file(input_file)
    osm_handler.finalize()
    return osm_handler.node_ids


class OsmHighwayHandler(SimpleHandler):
    CACHE_SIZE = 1000

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
        self._create_indices()

    def _add_way_to_cache(self, way, highway_type):
        node_ids = [copy.copy(node.ref) for node in way.nodes]
        way_id = copy.copy(way.id)
        self.data_cache["ways"].append((
            way_id,
            self.highway_types[highway_type],
            self.get_tag(way.tags, "ref", None),
            self.get_tag(way.tags, "name", None),
            self.get_tag(way.tags, "lanes", None, int),
            self.get_maxspeed(way.tags),
            self.is_oneway(way.tags),
            self.is_tunnel(way.tags),
        ))
        self.data_cache["way_node_ids"].extend((
            way_id, index, node_id
        ) for index, node_id in enumerate(node_ids))
        self.data_cache["size"] += 1
        return node_ids

    def _write_cache_to_database(self):
        self.dbcon.start_transaction()
        with self.dbcon.cursor() as cursor:
            _write_data_to_database(cursor, "ways", self.data_cache["ways"])
            _write_data_to_database(
                cursor, "way_node_ids", self.data_cache["way_node_ids"])
        self.dbcon.commit()
        self._init_cache()

    def _create_indices(self):
        with self.dbcon.cursor() as cursor:
            cursor.execute("CREATE INDEX way_index ON ways (way_id)")
            cursor.execute(
                "CREATE INDEX way_node_ids_index ON way_node_ids (way_id)")

    @ staticmethod
    def get_tag(tags, key, default, type_name=str):
        return type_name(tags[key]) if key in tags and tags[key] != "none" else default

    @ staticmethod
    def get_maxspeed(tags):
        try:
            return int(tags["maxspeed"])
        except:
            return None

    @ staticmethod
    def is_oneway(tags):
        return "oneway" in tags and tags["oneway"] == "yes"

    @ staticmethod
    def is_tunnel(tags):
        return "tunnel" in tags and tags["tunnel"] == "yes"


def _import_osm_nodes(dbcon, node_ids, input_file):
    logging.info("Importing nodes")
    osm_handler = OsmNodeHandler(dbcon, node_ids)
    osm_handler.apply_file(input_file)
    osm_handler.finalize()


class OsmNodeHandler(SimpleHandler):
    CACHE_SIZE = 1000

    def __init__(self, dbcon, node_ids):
        SimpleHandler.__init__(self)
        self.dbcon = dbcon
        self.node_ids = node_ids
        self._init_cache()

    def _init_cache(self):
        self.data_cache = []

    def node(self, node):
        if node.id in self.node_ids:
            self._add_node_to_cache(node)
            if len(self.data_cache) > self.CACHE_SIZE:
                self._write_cache_to_database()

    def finalize(self):
        self._write_cache_to_database()
        self._create_indices()

    def _add_node_to_cache(self, node):
        self.data_cache.append(
            (copy.copy(node.id), node.location.lat, node.location.lon)
        )

    def _write_cache_to_database(self):
        self.dbcon.start_transaction()
        with self.dbcon.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO nodes (node_id, location) VALUES (%s, ST_SRID(POINT(%s, %s), 4326))", self.data_cache)
        self.dbcon.commit()
        self._init_cache()

    def _create_indices(self):
        with self.dbcon.cursor() as cursor:
            cursor.execute("CREATE INDEX nodes_index ON nodes (node_id)")


def _write_data_to_database(cursor, table, data):
    column_names = [name for name,
                    _ in TABLE_CONFIGURATIONS[table]["columns"]]
    cursor.executemany("INSERT INTO {table} ({columns}) VALUES ({values})".format(
        table=table,
        columns=",".join(column_names),
        values=",".join(["%s"] * len(column_names))
    ), data)


def _aggregate_ways(dbcon, config, args):
    if args.skip_aggregation:
        return

    logging.info("Aggregating way meta data")
    way_ids = _get_way_ids(dbcon)
    way_tasks = _split_into_chunks(way_ids, multiprocessing.cpu_count())
    workers = _launch_aggregation_worker(config, way_tasks)
    _wait_for_workers(workers)
    _create_aggregation_indices(dbcon)


def _get_way_ids(dbcon):
    with dbcon.cursor() as cursor:
        cursor.execute("SELECT way_id FROM ways")
        return tuple((row[0] for row in cursor.fetchall()))


def _split_into_chunks(data, num_chunks):
    chunk_size = math.ceil(len(data) / num_chunks)
    for i in range(0, len(data), chunk_size):
        yield data[i:i+chunk_size]


def _launch_aggregation_worker(config, tasks):
    logging.debug("Launching workers")
    workers = list()
    for task in tasks:
        worker = WayAggregationWorker(config, task)
        worker.start()
        workers.append(worker)
    return workers


def _wait_for_workers(workers):
    logging.debug("Waiting for workers")
    for worker in workers:
        worker.join()


class WayAggregationWorker(threading.Thread):
    QUERY = """
        SELECT ST_X(nodes.location) as lon, ST_Y(nodes.location) as lat
        FROM ways
        JOIN way_node_ids ON ways.way_id = way_node_ids.way_id
        JOIN nodes ON way_node_ids.node_id = nodes.node_id
        WHERE ways.way_id = {}
        ORDER BY ways.way_id, way_node_ids.idx
    """

    def __init__(self, config, way_ids):
        threading.Thread.__init__(self)
        self.config = config
        self.way_ids = way_ids
        self.way_lengths = []
        self.way_segments = []
        self.way_segment_coverage = []

    def run(self):
        with _connect_to_database(self.config["mysql"], autocommit=False) as dbcon:
            for way_id in self.way_ids:
                node_data = self._get_way_data(dbcon, way_id)
                segments = self._compute_segments(node_data)
                self._create_way_lengths(way_id, segments)
                self._create_way_segments(way_id, segments)
                self._create_way_segment_coverage(way_id, segments)
            self._write_segment_data(dbcon)

    def _get_way_data(self, dbcon, way_id):
        with dbcon.cursor() as cursor:
            cursor.execute(self.QUERY.format(way_id))
            node_data = [{"lon": row[0], "lat": row[1]}
                         for row in cursor.fetchall()]
        dbcon.commit()
        return node_data

    @ staticmethod
    def _compute_segments(node_data):
        def compute_distance(o, t): return distance(
            (o["lat"], o["lon"]), (t["lat"], t["lon"])).km
        num_segments = len(node_data) - 1
        return [compute_distance(node_data[i], node_data[i+1])
                for i in range(num_segments)]

    def _create_way_lengths(self, way_id, segments):
        self.way_lengths.append(
            (way_id, sum(segments))
        )

    def _create_way_segments(self, way_id, segments):
        way_length = sum(segments)
        accumulated_length = 0
        for idx, segment in enumerate(segments):
            self.way_segments.append(
                (way_id, idx, segment, accumulated_length / way_length)
            )
            accumulated_length += segment

    def _create_way_segment_coverage(self, way_id, segments):
        for idx, segment in enumerate(segments):
            self.way_segment_coverage.append(
                (way_id, idx, 0)
            )

    def _write_segment_data(self, dbcon):
        dbcon.start_transaction()
        with dbcon.cursor() as cursor:
            _write_data_to_database(cursor, "way_lengths", self.way_lengths)
            _write_data_to_database(cursor, "way_segments", self.way_segments)
            _write_data_to_database(
                cursor, "way_segment_coverage", self.way_segment_coverage)
        dbcon.commit()


def _create_aggregation_indices(dbcon):
    with dbcon.cursor() as cursor:
        cursor.execute(
            "CREATE INDEX way_lengths_index ON way_lengths (way_id)")
        cursor.execute(
            "CREATE INDEX way_segments_index ON way_segments (way_id, segment_id)")
        cursor.execute(
            "CREATE INDEX way_segment_coverage ON way_segment_coverage (way_id, segment_id)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Motorway Database Generator")
    parser.add_argument("config_file", metavar="CONFIG_FILE",
                        help="The import configuration")
    parser.add_argument("input_file", metavar="OSM_FILE",
                        help="The input OSM file")
    parser.add_argument("--clear-database", help="Clear the database before import",
                        action="store_true", default=False)
    parser.add_argument("--skip-import", help="Skip the import stage",
                        action="store_true", default=False)
    parser.add_argument("--skip-aggregation", help="Skip the aggregation stage",
                        action="store_true", default=False)
    parser.add_argument("--skip-preparation", help="Skip the preparation stage",
                        action="store_true", default=False)
    args = parser.parse_args()

    logging.basicConfig(
        format="%(levelname)s: %(message)s", level=logging.DEBUG)
    main(args)
