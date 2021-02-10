TABLE_CONFIGURATIONS = {
    "ways": {
        "stage": "import",
        "columns": (
            ("way_id", "BIGINT"),
            ("road_type", "TINYINT"),
            ("ref", "VARCHAR(256)"),
            ("name", "VARCHAR(256)"),
            ("lanes", "TINYINT"),
            ("maxspeed", "SMALLINT"),
            ("oneway", "BOOL"),
            ("tunnel", "BOOL"),
        )
    },
    "way_node_ids": {
        "stage": "import",
        "columns": (
            ("way_id", "BIGINT"),
            ("idx", "SMALLINT"),
            ("node_id", "BIGINT"),
        )
    },
    "nodes": {
        "stage": "import",
        "columns":  (
            ("node_id", "BIGINT"),
            ("location", "POINT SRID 4326"),
        )
    },
    "way_lengths": {
        "stage": "aggregation",
        "columns": (
            ("way_id", "BIGINT"),
            ("length", "FLOAT"),
        )
    },
    "way_segments": {
        "stage": "aggregation",
        "columns": (
            ("way_id", "BIGINT"),
            ("segment_id", "SMALLINT"),
            ("length", "FLOAT"),
            ("way_length_ratio", "FLOAT")
        )
    },
    "way_segment_coverage": {
        "stage": "aggregation",
        "columns": (
            ("way_id", "BIGINT"),
            ("segment_id", "SMALLINT"),
            ("coverage", "INT"),
        )
    },
    "drives": {
        "stage": "preparation",
        "columns": (
            ("drive_id", "BIGINT"),
            ("drive_name", "VARCHAR(256)"),
        )
    },
    "way_segments_drive_coverage": {
        "stage": "preparation",
        "columns": (
            ("way_id", "BIGINT"),
            ("segment_id", "SMALLINT"),
            ("drive_id", "BIGINT"),
            ("date", "DATE")
        )
    }
}
