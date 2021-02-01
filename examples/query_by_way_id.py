#! /usr/bin/env python

import argparse
import sys

from road_coverage.road_db import client


def main(database_file, way_ids):
    db = client.RoadDatabase.load(database_file)

    for way_id in way_ids:
        way = db.ways[way_id]
        print(way["nodes"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query Example")
    parser.add_argument("database", metavar="DB_FILE", help="The databae file")
    parser.add_argument("way_ids", metavar="WAY_IDS", type=int, nargs="+", help="The way ids to query")
    args = parser.parse_args()
    main(args.database, args.way_ids)
