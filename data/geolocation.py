import csv
import os
from urllib import parse as urllib_parse
import requests
import hashlib


class HEETMAPreprocessor(object):
    """Base Class for parsing data coming from HEETMA, so we can
    get it ready send it to google for geo locating

    :param year: the year you'd like to parse
    :param company: company name
    :param subcompany: subcompany name (ngrid owns colonial gas for instance)
    """

    def __init__(self, year, company, subcompany=""):
        self.year = str(year)
        self.company = company

        if len(subcompany) > 0:
            self.subcompany = "(" + subcompany + ")"
        else:
            self.subcompany = subcompany

        self.leak_filename_path = os.path.join(
            self.year, "1. HEETMA Extract", self.year + "_" +
            self.company + self.subcompany + "_leaks.csv")
        self.preprocessed_leak_filename_path = os.path.join(
            self.year, "2. Pre-Process", "preprocessed_" + self.year +
            "_" + self.company + self.subcompany + "_leaks.csv")

    def parse(self):
        with open(self.leak_filename_path, "r", newline='') as fr:
            with open(self.preprocessed_leak_filename_path, "w+",
                      newline='') as fw:

                w = csv.writer(fw, delimiter=",")
                w.writerow(["ID", "COMPOUND ADDRESS", "ADDRESS",
                            "TOWN", "INTERSECTION", "DATE RECORDED", "GRADE"])

                raw_data = csv.reader(fr, delimiter=",")

                first_row = True
                for row in raw_data:

                    if first_row:
                        first_row = False
                        continue

                    if len(row[self.headers["intersecting_street"]]) == 0:
                        intersecting_address = ""
                    else:
                        # the 1: slice is to ignore the @ symbol
                        intersecting_address = " AND " + \
                            row[self.headers["intersecting_street"]][2:] \
                            .strip()

                    address = row[self.headers["address"]].strip() + \
                        intersecting_address + " " \
                        + row[self.headers["town"]].strip() + " MA"

                    primary_key = "%s%s%s%s%s%s" % (address,
                                                    row[self.headers[
                                                        "address"]],
                                                    row[self.headers["town"]],
                                                    row[self.headers[
                                                        "intersecting_street"]
                                                        ],
                                                    row[
                                                        self.headers
                                                        ["date_reported"]],
                                                    row[self.headers
                                                        ["leak_grade"]])

                    md5 = hashlib.md5()

                    md5.update(bytes(primary_key, 'utf-8'))

                    w.writerow([str(md5.hexdigest()),
                                str(address),
                                row[self.headers["address"]],
                                row[self.headers["town"]],
                                row[self.headers["intersecting_street"]],
                                row[self.headers["date_reported"]],
                                row[self.headers["leak_grade"]]])


class NationalGridPreprocessor(HEETMAPreprocessor):
    def __init__(self, year, subcompany):
        super().__init__(year, "ngrid", subcompany)

        keys = ["address", "intersecting_street", "town",
                "date_reported",
                "leak_grade",
                "note"]

        if subcompany is "boston_gas":
            header_locations = [1, 2, 3, 4, 6, 7]
        elif subcompany is "colonial_gas":
            # town doesn't exist in colonial gas data
            header_locations = [1, 2, 0, 4, 3, 5]
        else:
            print("Please enter a valid subcompany for National Grid!")
            raise ValueError

        # this maps from the data we want to the unique header for each company
        self.headers = dict(zip(keys, header_locations))


class EversourcePreprocessor(HEETMAPreprocessor):
    def __init__(self, year):
        super().__init__(year, "eversource")
        # dictionary matching header to index in a row
        self.headers = {"town": 0, "address": 1, "intersecting_street": 2,
                        "leak_grade": 3, "date_reported": 4, "note": 5}


class Geolocator(object):
    """More or less a stub used for testing our preprocessors for now.
        This will change in the future
    """

    def __init__(self):
        # This api key is just for testing at the moment
        self.api_key = ""
        self.region = "us"

    def get_latitude_and_longitude(self, address, state):

        if self.api_key == "":
            print("Please enter an api key!")
            raise ValueError

        percent_encoded_address = urllib_parse.quote(address)

        URI = (
            'https://maps.googleapis.com/maps/api/geocode/json?'
            'address=%s&region=%s&components=administrative_area:%s&key=%s'
            % (percent_encoded_address, self.region, state, self.api_key))

        res = requests.get(URI)

        content = res.json()

        status = content['status']

        if status == 'OVER_QUERY_LIMIT':
            raise Exception('API is over query limit')
        elif (status == 'ZERO_RESULTS') or (status == 'INVALID_REQUEST'):
            print('%s is invalid. Skipping' % address)
        elif status == 'OK' and len(content.get('results', [])) > 0:
            print(res)


def main():
    es = EversourcePreprocessor(2016)
    es.parse()

    ng_bg = NationalGridPreprocessor(2016, "boston_gas")
    ng_bg.parse()

    ng_cg = NationalGridPreprocessor(2016, "colonial_gas")
    ng_cg.parse()

    # test_address = "ACUSHNET AND BROOKLAWN CT NEW BEDFORD MA"
    # gl = Geolocator()
    # gl.get_latitude_and_longitude(test_address, state)

    # gl = Geolocator()

    # gl.get_latitude_and_longitude(test_address)

if __name__ == '__main__':
    main()
