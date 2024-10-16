
CREATE TABLE planets (
  id SERIAL PRIMARY KEY,
  name VARCHAR(20),
  mass DECIMAL(8, 4),
  diameter INT,
  density DECIMAL(5, 1),
  gravity DECIMAL(5, 1),
  escapeVelocity DECIMAL(5, 1),
  rotationPeriod DECIMAL(5, 1),
  lengthOfDay DECIMAL(5, 1),
  distanceFromSun DECIMAL(5, 1),
  perihelion DECIMAL(5, 1),
  aphelion DECIMAL(5, 1),
  orbitalPeriod DECIMAL(7, 1),
  orbitalVelocity DECIMAL(5, 1),
  orbitalInclination DECIMAL(5, 1),
  orbitalEccentricity DECIMAL(5, 3),
  obliquityToOrbit DECIMAL(5, 1),
  meanTemperature INT,
  surfacePressure DECIMAL(7, 5),
  numberOfMoons INT
);

INSERT INTO planets (id, name, mass, diameter, density, gravity, escapeVelocity, rotationPeriod, lengthOfDay, distanceFromSun, perihelion, aphelion, orbitalPeriod, orbitalVelocity, orbitalInclination, orbitalEccentricity, obliquityToOrbit, meanTemperature, surfacePressure, numberOfMoons)
VALUES 
  (DEFAULT, 'Mercury', 0.33::DECIMAL, 4879, 5427::DECIMAL, 3.7::DECIMAL, 4.3::DECIMAL, 1407.6::DECIMAL, 4222.6::DECIMAL, 57.9::DECIMAL, 46::DECIMAL, 69.8::DECIMAL, 88::DECIMAL, 47.4::DECIMAL, 7::DECIMAL, 0.205::DECIMAL, 0.034::DECIMAL, 167, 0::DECIMAL, 0),
  (DEFAULT, 'Venus', 4.87::DECIMAL, 12104, 5243::DECIMAL, 8.9::DECIMAL, 10.4::DECIMAL, -5832.5::DECIMAL, 2802::DECIMAL, 108.2::DECIMAL, 107.5::DECIMAL, 108.9::DECIMAL, 224.7::DECIMAL, 35::DECIMAL, 3.4::DECIMAL, 0.007::DECIMAL, 177.4::DECIMAL, 464, 92::DECIMAL, 0),
  (DEFAULT, 'Earth', 5.97::DECIMAL, 12756, 5514::DECIMAL, 9.8::DECIMAL, 11.2::DECIMAL, 23.9::DECIMAL, 24::DECIMAL, 149.6::DECIMAL, 147.1::DECIMAL, 152.1::DECIMAL, 365.2::DECIMAL, 29.8::DECIMAL, 0::DECIMAL, 0.017::DECIMAL, 23.4::DECIMAL, 15, 1::DECIMAL, 1),
  (DEFAULT, 'Mars', 0.642::DECIMAL, 6792, 3933::DECIMAL, 3.7::DECIMAL, 5::DECIMAL, 24.6::DECIMAL, 24.7::DECIMAL, 227.9::DECIMAL, 206.6::DECIMAL, 249.2::DECIMAL, 687::DECIMAL, 24.1::DECIMAL, 1.9::DECIMAL, 0.094::DECIMAL, 25.2::DECIMAL, -65, 0.01::DECIMAL, 2),
  (DEFAULT, 'Jupiter', 1898::DECIMAL, 142984, 1326::DECIMAL, 23.1::DECIMAL, 59.5::DECIMAL, 9.9::DECIMAL, 9.9::DECIMAL, 778.6::DECIMAL, 740.5::DECIMAL, 816.6::DECIMAL, 4331::DECIMAL, 13.1::DECIMAL, 1.3::DECIMAL, 0.049::DECIMAL, 3.1::DECIMAL, -110, CAST(NULL AS INTEGER), 79),
  (DEFAULT, 'Saturn', 568::DECIMAL, 120536, 687::DECIMAL, 9::DECIMAL, 35.5::DECIMAL, 10.7::DECIMAL, 10.7::DECIMAL, 1433.5::DECIMAL, 1352.6::DECIMAL, 1514.5::DECIMAL, 10747::DECIMAL, 9.7::DECIMAL, 2.5::DECIMAL, 0.057::DECIMAL, 26.7::DECIMAL, -140, NULL, 62),
  (DEFAULT, 'Uranus', 86.8::DECIMAL, 51118, 1271::DECIMAL, 8.7::DECIMAL, 21.3::DECIMAL, -17.2::DECIMAL, 17.2::DECIMAL, 2872.5::DECIMAL, 2741.3::DECIMAL, 3003.6::DECIMAL, 30589::DECIMAL, 6.8::DECIMAL, 0.8::DECIMAL, 0.046::DECIMAL, 97.8::DECIMAL, -195, NULL, 27),
  (DEFAULT, 'Neptune', 102::DECIMAL, 49528, 1638::DECIMAL, 11::DECIMAL, 23.5::DECIMAL, 16.1::DECIMAL, 16.1::DECIMAL, 4495.1::DECIMAL, 4444.5::DECIMAL, 4545.7::DECIMAL, 59800::DECIMAL, 5.4::DECIMAL, 1.8::DECIMAL, 0.011::DECIMAL, 28.3::DECIMAL, -200, NULL, 14),
  (DEFAULT, 'Pluto', 0.0146::DECIMAL, 2370, 2095::DECIMAL, 0.7::DECIMAL, 1.3::DECIMAL, -153.3::DECIMAL, 153.3::DECIMAL, 5906.4::DECIMAL, 4436.8::DECIMAL, 7375.9::DECIMAL, 90560::DECIMAL, 4.7::DECIMAL, 17.2::DECIMAL, 0.244::DECIMAL, 122.5::DECIMAL, -225, 0.00001::DECIMAL, 5);
