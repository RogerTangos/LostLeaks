import csv
import json
import os
import datetime
from collections import namedtuple
from secret import GOOGLE_API_KEY
from urllib import parse as urllib_parse
from threading import Thread
import requests
from queue import Queue


class Geolocator(object):
    """More or less a stub used for testing our preprocessors for now.
        This will change in the future
    """

    def __init__(self, preprocessed_file_location, year, utility,
                 leaks_or_repairs):
        self.preprocessed_file_location = preprocessed_file_location
        self.year = year
        self.utility = utility
        self.leaks_or_repairs = leaks_or_repairs

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

                row_id = row[0]
                address = row[1]
                Row = namedtuple('Row', [
                    'row_id', 'address', 'preprocessed_file_location', 'year',
                    'utility', 'leaks_or_repairs'])

                row = Row(
                    row_id=row_id, address=address,
                    preprocessed_file_location=self.preprocessed_file_location,
                    year=self.year,
                    utility=self.utility,
                    leaks_or_repairs=self.leaks_or_repairs)

                q.put(row)

            # setup consumers and start
            for i in range(4):
                worker = GeolocationWorker(q)
                worker.daemon = False
                worker.start()


class GeolocationWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.api_key = GOOGLE_API_KEY
        self.region = "us"

    def get_location_data(self, row, state):

        if self.api_key == "":
            print("Please enter an api key!")
            raise ValueError

        percent_encoded_address = urllib_parse.quote(row.address)

        URI = (
            'https://maps.googleapis.com/maps/api/geocode/json?'
            'address=%s&region=%s&components=administrative_area:%s&key=%s'
            % (percent_encoded_address, self.region, state, self.api_key))

        res = requests.get(URI)

        content = res.json()


        if content.get("error_message"):
            print('ERROR MESSAGE')
            import pdb; pdb.set_trace()

        filename = ('geocoded_%s_%s_%s_%s.json'
                        % (str(row.year), row.utility, row.leaks_or_repairs, row.row_id))
        relative_path = os.path.join(str(row.year), '3. JSON', filename)

        # edit content, b/c that's what will be writen to the file
        content['timestamp'] = datetime.datetime.now().isoformat()
        content['utility'] = row.utility
        content['year'] = row.year
        content['row_id'] = row.row_id
        content['leaks_or_repairs'] = row.leaks_or_repairs
        content['filename'] = filename

        # write file
        new_file = open(relative_path, 'w')
        new_file.write(str(json.dumps(content, sort_keys=True, indent=4)))
        new_file.close()


    def run(self):
        data_to_consume = True
        while data_to_consume:
            # get work from queue
            row = self.queue.get()

            self.get_location_data(row, 'MA')

            # signal you're done
            self.queue.task_done()

            # this is fine since, the queue size is
            # of a finite size when we start our threads
            if self.queue.qsize() == 0:
                data_to_consume = False


def main():
    # test_address = "ACUSHNET AND BROOKLAWN CT NEW BEDFORD MA"
    # gl.get_latitude_and_longitude(test_address, state)

    path = os.path.join("2016", "2. Pre-Process", "preprocessed_2016_ngrid(colonial_gas)_repairs.csv")
    gl = Geolocator(preprocessed_file_location=path,
                    year=2016,
                    utility='ngrid(colonial_gas)',
                    leaks_or_repairs='repairs')
    gl.start()


if __name__ == '__main__':
    main()