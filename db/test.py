#! /usr/bin/env python

import pickle

with open("./croatia.rdb", "rb") as file_stream:
    data = pickle.load(file_stream)


print(sum([way_data["length"] for way_data in data["ways"].values()]))
