from typing import Iterable
import datetime


class MinMaxIndex():

    minimum = None
    maximum = None
    domain = "unknown"

    @staticmethod
    def build(dictset: Iterable[dict]):

        for row 
        # calculate the min/max for ordinals (and strings) and the cummulative
        # sum for numerics


    def add(self, attribute):

        # if the value is missing, count it and skip everything else
        if (attribute or "") == "" and attribute != False:
            continue

        value_type = type(attribute)
        if value_type in (int, float, str, datetime.date, datetime.datetime):
            self.maximum = max(attribute, self.maximum or attribute)
            self.minimum = min(attribute, self.minimum or attribute)

        value_type_name = value_type.__name__
        if self.domain != value_type_name:
            if self.domain == "unknown":
                self.domain = value_type_name
            else:
                self.domain = "mixed"

    def search(self, search_term) -> Iterable:
        pass

    def dump(self, file):
        with open(file, "wb") as f:
            f.write(self.bytes())

    def bytes(self):
        pass
