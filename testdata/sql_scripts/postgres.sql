CREATE TABLE planets (
  id SERIAL PRIMARY KEY,
  name VARCHAR(20),
  mass NUMERIC(6,1),
  diameter INT,
  density NUMERIC(6,1),
  gravity NUMERIC(6,1),
  escape_velocity NUMERIC(6,1),
  rotation_period NUMERIC(6,1),
  length_of_day NUMERIC(6,1),
  distance_from_sun NUMERIC(6,1),
  perihelion NUMERIC(6,1),
  aphelion NUMERIC(6,1),
  orbital_period NUMERIC(6,1),
  orbital_velocity NUMERIC(6,1),
  orbital_inclination NUMERIC(6,1),
  orbital_eccentricity NUMERIC(5,3),
  obliquity_to_orbit NUMERIC(6,1),
  mean_temperature INT,
  surface_pressure NUMERIC(7,5),
  number_of_moons INT
);

INSERT INTO planets (name, mass, diameter, density, gravity, escape_velocity, rotation_period, length_of_day, distance_from_sun, perihelion, aphelion, orbital_period, orbital_velocity, orbital_inclination, orbital_eccentricity, obliquity_to_orbit, mean_temperature, surface_pressure, number_of_moons)
VALUES 
  ('Mercury', 0.33, 4879, 5427, 3.7, 4.3, 1407.6, 4222.6, 57.9, 46, 69.8, 88, 47.4, 7, 0.205, 0.034, 167, 0, 0),
  ('Venus', 4.87, 12104, 5243, 8.9, 10.4, -5832.5, 2802, 108.2, 107.5, 108.9, 224.7, 35, 3.4, 0.007, 177.4, 464, 92, 0),
  ('Earth', 5.97, 12756, 5514, 9.8, 11.2, 23.9, 24, 149.6, 147.1, 152.1, 365.2, 29.8, 0, 0.017, 23.4, 15, 1, 1),
  ('Mars', 0.642, 6792, 3933, 3.7, 5, 24.6, 24.7, 227.9, 206.6, 249.2, 687, 24.1, 1.9, 0.094, 25.2, -65, 0.01, 2),
  ('Jupiter', 1898, 142984, 1326, 23.1, 59.5, 9.9, 9.9, 778.6, 740.5, 816.6, 4331, 13.1, 1.3, 0.049, 3.1, -110, NULL, 79),
  ('Saturn', 568, 120536, 687, 9, 35.5, 10.7, 10.7, 1433.5, 1352.6, 1514.5, 10747, 9.7, 2.5, 0.057, 26.7, -140, NULL, 62),
  ('Uranus', 86.8, 51118, 1271, 8.7, 21.3, -17.2, 17.2, 2872.5, 2741.3, 3003.6, 30589, 6.8, 0.8, 0.046, 97.8, -195, NULL, 27),
  ('Neptune', 102, 49528, 1638, 11, 23.5, 16.1, 16.1, 4495.1, 4444.5, 4545.7, 59800, 5.4, 1.8, 0.011, 28.3, -200, NULL, 14),
  ('Pluto', 0.0146, 2370, 2095, 0.7, 1.3, -153.3, 153.3, 5906.4, 4436.8, 7375.9, 90560, 4.7, 17.2, 0.244, 122.5, -225, 0.00001, 5)