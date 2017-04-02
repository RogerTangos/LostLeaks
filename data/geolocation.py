import csv
import os
from urllib import parse as urllib_parse
import requests
from abc import ABCMeta,abstractclassmethod

class HEETMAPreprocessor(object):
    """Base Class for parsing data coming from HEETMA, so we can
    get it ready send it to google for geo locating

    :param year: the year you'd like to parse
    :param company: company name
    :param subcompany: subcompany name (ngrid owns colonial gas for instance)
    """
    __metaclass__ = ABCMeta

    def __init__(self,year,company,subcompany=""):
        self.year = str(year)
        self.company = company
        
        if len(subcompany) > 0:
            self.subcompany = "("+subcompany+")"
        else:
            self.subcompany= subcompany


        self.leak_filename_path = os.path.join(self.year,"1. HEETMA Extract", self.year + "_"+self.company+self.subcompany+"_leaks.csv")
        self.preprocessed_leak_filename_path = os.path.join(self.year,"2. Pre-Process", "preprocessed_"+self.year + "_"+self.company+self.subcompany+"_leaks.csv")


    @abstractclassmethod
    def parse(self):
        """return data from company 
        """
        return


class EversourcePreprocessor(HEETMAPreprocessor):
    def __init__(self,year):
        super().__init__(year,"eversource")
        # dictionary matching header to index in a row
        self.headers = {"town":0,"address":1,"intersecting_street":2,"leak_grade":3,"date_reported":4,"note":5}
    
    def parse(self):
        
        with open(self.leak_filename_path,"r",newline='') as fr:
            with open(self.preprocessed_leak_filename_path,"w+",newline='') as fw:

                w = csv.writer(fw,delimiter=",")
                w.writerow(["ID","COMPOUND ADDRESS","ADDRESS","TOWN","INTERSECTION","CLASSIFIED,GRADE"])

                eversource_data = csv.reader(fr,delimiter=",")

                first_row = True
                primary_key = 1
                for row in eversource_data:
                    if first_row:
                        first_row = False
                        continue

                    if len(row[self.headers["intersecting_street"]]) == 0:
                        intersecting_address = ""
                    else:
                        # the 1: slice is to ignore the @ symbol
                        intersecting_address = " AND " + row[self.headers["intersecting_street"]][2:]


                    address = row[self.headers["address"]] + intersecting_address + " " \
                    + row[self.headers["town"]]  + " MA" 
                    
                    w.writerow([str(primary_key),str(address),row[self.headers["address"]],
                        row[self.headers["town"]],row[self.headers["intersecting_street"]],
                        row[self.headers["date_reported"]],row[self.headers["leak_grade"]]])


                    primary_key += 1

                    #print(address)


class Geolocator(object):
    
    def __init__(self):
        # This api key is just for testing at the moment
        self.api_key = ""
        self.region = "us"

    def get_latitude_and_longitude(self, address):
        
        if self.api_key == "":
            print("Please enter an api key!")
            raise ValueError

        percent_encoded_address = urllib_parse.quote(address)

        URI = (
            'https://maps.googleapis.com/maps/api/geocode/json?'
            'address=%s&region=%s&key=%s' % (percent_encoded_address,self.region, self.api_key))

        res = requests.get(URI)

        content = res.json()
        
        status = content['status']

        if status == 'OVER_QUERY_LIMIT':
            raise Exception('API is over query limit')
        elif (status == 'ZERO_RESULTS') or (status == 'INVALID_REQUEST'):
            print('%s is invalid. Skipping' % sanit)
        elif status == 'OK' and len(content.get('results', [])) > 0:
            # extract some variables
            print(result)
        

def main():  
    pass
    #es =EversourcePreprocessor(2016)

    #es.parse()

    # test_address = "ACUSHNET AND BROOKLAWN CT NEW BEDFORD MA"

    #gl = Geolocator()

    #gl.get_latitude_and_longitude(test_address)

if __name__ == '__main__':
    main()