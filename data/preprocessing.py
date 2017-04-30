import csv
import os
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

        heetma_filename_path = os.path.join(
            self.year, "1. HEETMA Extract", self.year + "_" +
            self.company + self.subcompany)

        self.leak_filename_path = os.path.join(
            heetma_filename_path + "_leaks.csv")
        self.repairs_filename_path = os.path.join(
            heetma_filename_path + "_repairs.csv")

        gas_filename_path = os.path.join(
            self.year, "2. Pre-Process", "preprocessed_" +
            self.year + "_" + self.company + self.subcompany)

        self.preprocessed_leak_filename_path = gas_filename_path + "_leaks.csv"
        self.preprocessed_repair_filename_path = gas_filename_path
        + "_repairs.csv"

    def preprocess(self):
        """preprocess the HEETMA leak and repair data"""

        self.create_processed_file(
            self.leak_headers, self.leak_filename_path,
            self.preprocessed_leak_filename_path, "DATE RECORDED")

        self.create_processed_file(self.repair_headers,
                                   self.repairs_filename_path,
                                   self.preprocessed_repair_filename_path,
                                   "DATE REPAIRED")

    def create_processed_file(self, headers, raw_filename_path,
                              preprocessed_filename_path, date_column_name):
        """ this reads the heetma file, gets the data as we like it, and writes it back
            out

        :param headers: header dictionary mapping our standardized header names
                        to there places in the heetma files
        :param raw_filename_path: heetma file location
        :param preprocessed_filename_path: where we're writing our data too
        :param date_column_name: if its a repair, use "DATE REPAIRED",
                                else "DATE RECORDED"
        """
        with open(raw_filename_path, "r", newline='') as fr:
            with open(preprocessed_filename_path, "w+",
                      newline='') as fw:

                w = csv.writer(fw, delimiter=",")
                w.writerow(["ID", "COMPOUND ADDRESS", "ADDRESS",
                            "TOWN", "INTERSECTION", date_column_name, "GRADE"])

                raw_data = csv.reader(fr, delimiter=",")

                first_row = True
                for row in raw_data:

                    if first_row:
                        first_row = False
                        continue

                    if len(row[headers["intersecting_street"]]) == 0:
                        intersecting_address = ""
                    else:
                        # the 1: slice is to ignore the @ symbol
                        intersecting_address = " AND " + \
                            row[headers["intersecting_street"]][2:] \
                            .strip()

                    address = row[headers["address"]].strip() + \
                        intersecting_address + " " \
                        + row[headers["town"]].strip() + " MA"

                    primary_key = "%s%s%s%s%s%s" % (address,
                                                    row[headers[
                                                        "address"]],
                                                    row[headers["town"]],
                                                    row[headers[
                                                        "intersecting_street"]
                                                        ],
                                                    row[
                                                        headers
                                                        ["date_reported"]],
                                                    row[headers
                                                        ["leak_grade"]])

                    md5 = hashlib.md5()

                    md5.update(bytes(primary_key, 'utf-8'))

                    w.writerow([str(md5.hexdigest()),
                                str(address),
                                row[headers["address"]],
                                row[headers["town"]],
                                row[headers["intersecting_street"]],
                                row[headers["date_reported"]],
                                row[headers["leak_grade"]]])


class NationalGridPreprocessor(HEETMAPreprocessor):
    """ preprocessor specific for the nation grid company """
    def __init__(self, year, subcompany):
        super().__init__(year, "ngrid", subcompany)

        # we'll map the headers we want depending on the
        # subcompany used
        keys = ["address", "intersecting_street", "town",
                "date_reported",
                "leak_grade",
                "note"]

        if subcompany is "boston_gas":
            leak_header_locations = [1, 2, 3, 4, 6, 7]
            # no notes for boston_gas repairs
            repair_header_locations = [1, 2, 3, 8, 7, -1]
        elif subcompany is "colonial_gas":
            # town doesn't exist in colonial gas leak data
            leak_header_locations = [1, 2, 0, 4, 3, 5]
            repair_header_locations = [2, 3, 0, 4, 5, 6]

        else:
            print("Please enter a valid subcompany for National Grid!")
            raise ValueError

        # this maps from the data we want to the unique header for each company
        self.leak_headers = dict(zip(keys, leak_header_locations))
        self.repair_headers = dict(zip(keys, repair_header_locations))


class EversourcePreprocessor(HEETMAPreprocessor):
    """ preprocessor specific for the eversource company """
    def __init__(self, year):
        super().__init__(year, "eversource")
        # dictionary matching header to index in a row
        self.leak_headers = {"town": 0, "address": 1, "intersecting_street": 2,
                             "leak_grade": 3, "date_reported": 4, "note": 5}

        self.repair_headers = {"town": 0, "address": 2,
                               "intersecting_street": 3,
                               "leak_grade": 5, "date_reported": 4, "note": 6}


def main():

    es = EversourcePreprocessor(2016)
    es.preprocess()

    ng_bg = NationalGridPreprocessor(2016, "boston_gas")
    ng_bg.preprocess()

    ng_cg = NationalGridPreprocessor(2016, "colonial_gas")
    ng_cg.preprocess()


if __name__ == '__main__':
    main()
