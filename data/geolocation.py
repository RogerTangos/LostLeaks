import csv
from urllib import parse as urllib_parse
import requests


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
