
def satellites():
    from .satellite_data import SatelliteData
    return SatelliteData().get()

def planets():
    from .planet_data import PlanetData
    return PlanetData().get()
