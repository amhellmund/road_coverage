import pickle


class RoadDatabase:
    @staticmethod
    def load(database_file):
        with open(database_file, "rb") as file_stream:
            db = pickle.load(file_stream)
        return RoadDatabase(db["nodes"], db["ways"])

    def __init__(self, nodes, ways):
        self.nodes = nodes
        self.ways = ways
