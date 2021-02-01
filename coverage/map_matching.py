#! /usr/bin/env python

import argparse
import os
import requests

import xml.etree.ElementTree as xml

from input import read_trajectory_from_osm


def match_trajectory(trajectory):
    if not "MAP_MATCHING_API_URL" in os.environ:
        raise ValueError("Environment variable MAP_MATCHING_API_URL not found")
    request_data = {
        "shape": trajectory[0:5],
        "costing": "auto",
        "shape_match": "map_snap",
    }
    result = requests.post(os.environ["MAP_MATCHING_API_URL"], json=request_data)
    if result.status_code != 200:
        raise ValueError("Request to map matching API faile")
    return postprocess_match(result.json())


def postprocess_match(result):
    map_match = dict()
    map_match["meta"] = extract_metadata(result["admins"])
    map_match["edges"] = extract_edges(result["edges"])
    map_match["matches"] = extract_matches(result["matched_points"])
    return map_match


def extract_metadata(data):
    return [{
        "state_code": e["state_code"],
        "state_text": e["state_text"],
        "country_code": e["country_code"],
        "country_text": e["country_text"]
    } for e in data]


def extract_edges(data):
    return [{
        "way_id": e["way_id"],
        "meta_index": e["end_node"]["admin_index"],
        "road_class": e["road_class"],
        "length": e["length"],
        "begin_heading": e["begin_heading"],
        "end_heading": e["end_heading"],
    } for e in data]


def extract_matches(data):
    return [{
        "edge_index": e["edge_index"],
        "type": e["type"],
        "edge_ratio": e["distance_along_edge"],
        "lat": e["lat"],
        "lon": e["lon"]
    } for e in data]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSM Way Map Matcher")
    parser.add_argument("osm_file", metavar="OSM_FILE", help="The OSM file")
    parser.add_argument("way_id", metavar="WAY_ID", type=int, help="The way id")
    args = parser.parse_args()

    trajectory = read_trajectory_from_osm(args.osm_file, args.way_id)
    map_match = match_trajectory(trajectory)
    print([e["way_id"] for e in map_match["edges"]])
    print(map_match)
