import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random


#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    #----------------------------------------
    def map(self, raw_data):
        json_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here
        if not raw_data['name']:
            return None

        #--place any calculations needed here
        is_organization = self.is_organization(raw_data['name'], raw_data['dob'], raw_data['ssn'])

        #--mandatory attributes
        json_data['DATA_SOURCE'] = 'TEST'
        json_data['ENTITY_TYPE'] = 'GENERIC'

        #--the record_id should be unique, remove this mapping if there is not one 
        json_data['RECORD_ID'] = raw_data['uniqueid']

        #--column mappings

        # columnName: uniqueid
        # 100.0 populated, 100.0 unique
        #      1001 (1)
        #      1002 (1)
        #      1003 (1)
        #      1004 (1)
        #      1005 (1)
        #json_data['uniqueid'] = raw_data['uniqueid']  #--already mapped as record_id

        # columnName: type
        # 100.0 populated, 22.22 unique
        #      company (5)
        #      individual (4)
        # json_data['type'] = raw_data['type']  #--use our calculation here instead
        json_data['RECORD_TYPE'] = 'ORGANIZATION' if is_organization else 'PERSON' #--set the entity type


        # columnName: name
        # 88.89 populated, 100.0 unique
        #      ABC Company (1)
        #      Bob Jones (1)
        #      General Hospital (1)
        #      Mary Smith (1)
        #      Peter  Anderson (1)
        #json_data['name'] = raw_data['name'] #--commented out in favor of ...
        if is_organization:
            json_data['NAME_ORG'] = raw_data['name'] #--name_org if an organization
        else:
            json_data['NAME_FULL'] = raw_data['name'] #--name_full if not

        # columnName: gender
        # 100.0 populated, 11.11 unique
        #      u (9)
        #json_data['gender'] = raw_data['gender'] #--commented out as no value

        # columnName: dob
        # 22.22 populated, 100.0 unique
        #      2/2/92 (1)
        #      3/3/93 (1)
        json_data['DATE_OF_BIRTH'] = raw_data['dob']

        # columnName: ssn
        # 22.22 populated, 100.0 unique
        #      333-33-3333 (1)
        #      666-66-6666 (1)
        json_data['SSN_NUMBER'] = raw_data['ssn']

        if is_organization:
            json_data['ADDR_TYPE'] = 'BUSINESS' #--added if an organization

        # columnName: addr1
        # 88.89 populated, 100.0 unique
        #      111 First (1)
        #      222 Second (1)
        #      333 Third (1)
        #      444 Fourth (1)
        #      555 Fifth (1)
        json_data['ADDR_LINE1'] = raw_data['addr1']

        # columnName: city
        # 88.89 populated, 12.5 unique
        #      Las Vegas (8)
        json_data['ADDR_CITY'] = raw_data['city']

        # columnName: state
        # 88.89 populated, 12.5 unique
        #      NV (8)
        json_data['ADDR_STATE'] = raw_data['state']

        # columnName: zip
        # 88.89 populated, 100.0 unique
        #      89111 (1)
        #      89112 (1)
        #      89113 (1)
        #      89114 (1)
        #      89115 (1)
        json_data['ADDR_POSTAL_CODE'] = raw_data['zip']

        # columnName: create_date
        # 88.89 populated, 100.0 unique
        #      1/1/01 (1)
        #      2/2/02 (1)
        #      3/3/03 (1)
        #      4/4/04 (1)
        #      5/5/05 (1)
        json_data['important_date'] = raw_data['create_date']

        # columnName: status
        # 88.89 populated, 25.0 unique
        #      Active (6)
        #      Inactive (2)
        json_data['important_status'] = raw_data['status']

        # columnName: value
        # 88.89 populated, 100.0 unique
        #      1000 (1)
        #      2000 (1)
        #      3000 (1)
        #      4000 (1)
        #      5000 (1)
        #json_data['value'] = raw_data['value']

        #--capture the stats
        self.capture_mapped_stats(json_data)

        return json_data

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

        #--organization tokens
        self.variant_data['ORGANIZATION_TOKENS'] = []
        self.variant_data['ORGANIZATION_TOKENS'].append('COMPANY')
        self.variant_data['ORGANIZATION_TOKENS'].append('HOSPITAL')
        self.variant_data['ORGANIZATION_TOKENS'].append('CLINIC')
        self.variant_data['ORGANIZATION_TOKENS'].append('CITY OF')

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #----------------------------------------
    def format_dob(self, raw_date):
        try: new_date = dateparse(raw_date)
        except: return ''

        #--correct for prior century dates
        if new_date.year > datetime.now().year:
            new_date = datetime(new_date.year - 100, new_date.month, new_date.day)

        if len(raw_date) == 4:
            output_format = '%Y'
        elif len(raw_date) in (5,6):
            output_format = '%m-%d'
        elif len(raw_date) in (7,8):
            output_format = '%Y-%m'
        else:
            output_format = '%Y-%m-%d'

        return datetime.strftime(new_date, output_format)

    #-----------------------------------
    def is_organization(self, raw_name, raw_dob, raw_ssn):
        if raw_dob or raw_ssn or not raw_name:
            return False
        prior_tokens = []
        for token in raw_name.replace('.',' ').replace(',',' ').replace('-',' ').upper().split():
            if token in self.variant_data['ORGANIZATION_TOKENS']:
                return True
            elif ' '.join(prior_tokens[-2:]) in self.variant_data['ORGANIZATION_TOKENS']:
                return True
            elif ' '.join(prior_tokens[-3:]) in self.variant_data['ORGANIZATION_TOKENS']:
                return True
            prior_tokens.append(token)
        return False

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        if 'DATA_SOURCE' in json_data:
            data_source = json_data['DATA_SOURCE']
        else:
            data_source = 'UNKNOWN_DSRC'

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, subrecord[key2])

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file = 'input/test_set1.csv'
    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n') 
        sys.exit(1)

    input_file_handle = open(args.input_file, 'r')
    output_file_handle = open(args.output_file, 'w', encoding='utf-8')
    mapper = mapper()

    input_row_count = 0
    output_row_count = 0
    for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect):
        input_row_count += 1

        json_data = mapper.map(input_row)
        if json_data:
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

        if input_row_count % 1000 == 0:
            print('%s rows processed, %s rows written' % (input_row_count, output_row_count))
        if shut_down:
            break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)


    sys.exit(0)

