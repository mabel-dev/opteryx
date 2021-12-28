pass_thru = lambda x: x


def json(ds):
    import simdjson

    """parse each line in the file to a dictionary"""
    json_parser = simdjson.Parser()
    return json_parser.parse(ds)


def pass_thru_block(ds):
    """each blob is read as a block"""
    if isinstance(ds, str):
        return ds
    return "\n".join([r for r in ds])  # pragma: no cover


def xml(ds):
    from ...internals import xmler

    return xmler.parse(ds)
