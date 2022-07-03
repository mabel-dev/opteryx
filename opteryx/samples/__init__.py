# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def satellites():
    """load the satellite sample data"""
    from .satellite_data import load

    return load()


def planets():
    """load the planets sample data"""
    from .planet_data import load

    return load()


def astronauts():
    """load the astronaut sample data"""
    from .astronaut_data import load

    return load()


def no_table():
    """load the null data table"""
    from .no_table_data import load

    return load()
