
import os
import json
import dateutil.parser


def read_json_files(
        year,
        include_utilities=[
        "eversource", "ngrid(colonial_gas)", "ngrid(boston_gas)"],
        include_location_types=["ROOFTOP"]):

    folder_name = os.path.join(os.getcwd(), str(year), "3. JSON")
    print(folder_name)

    files = os.listdir(folder_name)
    files = [f for f in files if '.json' in f]
    for file_name in files:
        file_path = os.path.join(folder_name, file_name)
        file = open(file_path, 'r')
        text = json.loads(file.read())

        row_id = text.get('row_id')
        utility = text.get('utility')
        formatted_address = text.get(
            'results', [{}])[0].get('formatted_address')
        lat = text.get('results', [{}])[0].get('geometry',{}).get(
            'location',{}).get('lat')
        lng = text.get('results', [{}])[0].get('geometry',{}).get(
            'location',{}).get('lng')
        location_type = text.get('results', [{}])[0].get('geometry',{}).get(
            'location_type')
        timestamp = dateutil.parser.parse(text.get('timestamp'))
        record_type = text.get('leaks_or_repairs')
        # record_date = needs to be fixed


def main():
    # read_json_files(2020)
    pass


if __name__ == '__main__':
    main()
