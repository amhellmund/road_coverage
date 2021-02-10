#! /usr/bin/env python

import argparse
import copy
import logging
import pickle

from collections import defaultdict
from itertools import groupby

from db.mysql_connection import connect_to_database, load_configuration


def map_match_result_to_osm_way_segments(dbcon, match_result):
    if _is_valid_match_result(match_result):
        osm_way_segments = _get_osm_way_segments(
            dbcon, (e["way_id"] for e in match_result["edges"]))
        travelled_segments = _get_travelled_way_segments(
            match_result, osm_way_segments)
        print("Travelled Segments: {}".format(travelled_segments))
    else:
        logging.error("Provided match_result is invalid")


def _is_valid_match_result(match_result):
    def is_less(prev, next): return (prev[0] < next[0]) or (
        prev[0] == next[0] and (prev[1] < next[1]))
    index_ratio_list = [(e["edge_index"], e["edge_ratio"])
                        for e in match_result["matches"]]
    return all([is_less(index_ratio_list[i], index_ratio_list[i+1]) for i in range(len(index_ratio_list)-1)])


def _get_osm_way_segments(dbcon, way_ids):
    with dbcon.cursor() as cursor:
        cursor.execute("SELECT way_id, length, way_length_ratio FROM way_segments WHERE way_id IN ({}) ORDER BY way_id, segment_id".format(
            ",".join([str(w) for w in way_ids])
        ))
        way_segments = defaultdict()
        for row in cursor.fetchall():
            way_id, length, ratio = row
            way_segments.setdefault(way_id, []).append({
                "length": row[1],
                "ratio": row[2]
            })
    return way_segments


def _get_travelled_way_segments(match_result, way_segments):
    edges = match_result["edges"]
    travelled_way_segments = []
    new_trace = []
    for match in match_result["matches"]:
        way_id = edges[match["edge_index"]]["way_id"]
        if way_id in way_segments:
            segment_id = _get_segment_id_by_ratio(
                match["edge_ratio"], way_segments[way_id])
            new_trace.append((way_id, segment_id))
        else:
            if new_trace:
                travelled_way_segments.append(new_trace)
                new_trace = []
    if new_trace:
        travelled_way_segments.append(new_trace)
    print("Travelled: {}".format(travelled_way_segments))
    return _fill_in_missing_segments(travelled_way_segments, edges, way_segments)


def _get_segment_id_by_ratio(edge_ratio, way_segments):
    for idx, way_segment in enumerate(way_segments):
        if way_segment["ratio"] > edge_ratio:
            return idx - 1
    return len(way_segments) - 1


def _fill_in_missing_segments(travelled_way_segments, edges, way_segments):
    completed_travelled_way_segments = []
    for travelled_segments in travelled_way_segments:
        cur_segment = None
        completed_segments = []
        for segment in travelled_segments:
            if not cur_segment:
                completed_segments.append(segment)
            else:
                iter_segment = [cur_segment[0], cur_segment[1]]
                while not (iter_segment[0] == segment[0] and iter_segment[1] == segment[1]):
                    iter_segment[1] += 1
                    if iter_segment[1] > len(way_segments[iter_segment[0]])-1:
                        iter_segment[0] = _get_next_way(
                            iter_segment[0], edges)
                        iter_segment[1] = 0
                    completed_segments.append(copy.copy(iter_segment))
            cur_segment = segment
        completed_travelled_way_segments.append(completed_segments)
    return completed_travelled_way_segments


def _get_next_way(cur_edge_id, edges):
    for i in range(len(edges)):
        if edges[i]["way_id"] == cur_edge_id:
            return edges[i+1]["way_id"]
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser("OSM Way Segments")
    parser.add_argument("config", metavar="MYSQL_CONFIG")
    parser.add_argument("map_match_file", metavar="MATCH_FILE")
    args = parser.parse_args()

    with open(args.map_match_file, "rb") as file_stream:
        match_result = pickle.load(file_stream)

    config = load_configuration(args.config)
    with connect_to_database(config["mysql"]) as dbcon:
        print(map_match_result_to_osm_way_segments(dbcon, match_result))
