from .planet_data import PlanetData
from .satellite_data import SatelliteData

def satellites():
    return SatelliteData().get()

def planets():
    return PlanetData().get()
