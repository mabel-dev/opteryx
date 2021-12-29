pass_thru = lambda x: x


def json(ds):
    import simdjson

    """parse each line in the file to a dictionary"""
    json_parser = simdjson.Parser()
    return json_parser.parse(ds)
