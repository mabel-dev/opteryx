def satellites():
    from .satellite_data import SatelliteData

    return SatelliteData().get()


def planets():
    from .planet_data import PlanetData

    return PlanetData().get()


def astronauts():
    from .astronaut_data import AstronautData

    return AstronautData().get()


def no_table():
    from .no_table_data import NoTable

    return NoTable().get()
