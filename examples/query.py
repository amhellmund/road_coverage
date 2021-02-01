#! /usr/bin/env python

import argparse
import sys

from road_coverage.road_db import client


def main(database_file):
    db = client.RoadDatabase.load(database_file)
    
    motorway_length = sum([way["length"] for way in db.ways.values() if way["type"] == "motorway"])
    a7_length = sum([way["length"] for way in db.ways.values() if way["type"] == "motorway" and way["ref"] == "A 7"])
    a1_length = sum([way["length"] for way in db.ways.values() if way["type"] == "motorway" and way["ref"] == "A 1"])
    a5_length = sum([way["length"] for way in db.ways.values() if way["type"] == "motorway" and way["ref"] == "A 5"])

    print(motorway_length)
    print(a7_length)
    print(a1_length)
    print(a5_length)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query Example")
    parser.add_argument("database", metavar="DB_FILE", help="The databae file")
    args = parser.parse_args()
    main(args.database)
