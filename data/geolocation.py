import csv
import os
from secret import GOOGLE_API_KEY
from urllib import parse as urllib_parse
import requests
import hashlib
from threading import Thread
from queue import Queue


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

    def preprocess(self):
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

    def __init__(self, preprocessed_file_location):
        self.preprocessed_file_location = preprocessed_file_location

    def start(self):
        with open(self.preprocessed_file_location, "r", newline='') as fr:
            preprocessed_data = csv.reader(fr, delimiter=",")

            # create a multi-producers/worker aware queue
            q = Queue()

            # put addresses in queue

            # row[1] = "COMPOUND ADDRESS"
            first_row = True
            for row in preprocessed_data:
                if first_row:
                    first_row = False
                    continue

                compound_address = row[1]
                # print(compound_address)
                q.put(compound_address)

            # setup consumers and start
            for i in range(4):
                worker = GeolocationWorker(q)
                worker.daemon = False
                worker.start()


class GeolocationWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        # This api key is just for testing at the moment
        self.api_key = GOOGLE_API_KEY
        self.region = "us"

    def get_location_data(self, address, state):

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
            print('%s is invalid. Skipping' % percent_encoded_address)
        elif status == 'OK' and len(content.get('results', [])) > 0:
            # extract some variables
            result = content['results'][0]

            lat = result['geometry']['location']['lat']
            lng = result['geometry']['location']['lng']
            location_type = result['geometry']['location_type']
            return (lat, lng, location_type)

    def run(self):
        data_to_consume = True
        while data_to_consume:
            # get work from queue
            compound_address = self.queue.get()

            print(self.get_location_data(compound_address, 'MA'))

            # signal you're done
            self.queue.task_done()

            # this is fine since, the queue size is
            # of a finite size when we start our threads
            if self.queue.qsize() == 0:
                data_to_consume = False


def main():
    # es = EversourcePreprocessor(2016)
    # es.preprocess()

    # ng_bg = NationalGridPreprocessor(2016, "boston_gas")
    # ng_bg.preprocess()

    # ng_cg = NationalGridPreprocessor(2016, "colonial_gas")
    # ng_cg.preprocess()

    gl = Geolocator(os.path.join(
        "2016", "2. Pre-Process", "PREPROCESS_FILENAME"))
    gl.start()

    # gl = Geolocator()

    # gl.get_latitude_and_longitude(test_address)


if __name__ == '__main__':
    main()
