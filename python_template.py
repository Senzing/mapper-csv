import csv
import json
from datetime import datetime

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.delimiter = '<supply>'
        self.encoding = '<supply>'

        #--load any reference data for supporting functions
        self.load_reference_data()

    #----------------------------------------
    def map(self, raw_data):
        new_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--mandatory attributes
        new_data['DATA_SOURCE'] = '<supply>'
        new_data['ENTITY_TYPE'] = 'GENERIC'

        #--the record_id should be unique, remove this mapping if there is not one 
        new_data['RECORD_ID'] = '<remove_or_supply>'

        #--column mappings

        return new_data

    #----------------------------------------
    def load_reference_data(self):

        #--supported date formats
        self.date_formats = []
        self.date_formats.append("%Y-%m-%d")
        self.date_formats.append("%m/%d/%Y")
        self.date_formats.append("%d/%m/%Y")
        self.date_formats.append("%d-%b-%Y")
        self.date_formats.append("%Y")
        self.date_formats.append("%Y-%M")
        self.date_formats.append("%m-%Y")
        self.date_formats.append("%m/%Y")
        self.date_formats.append("%b-%Y")
        self.date_formats.append("%b/%Y")
        self.date_formats.append("%m-%d")
        self.date_formats.append("%m/%d")
        self.date_formats.append("%b-%d")
        self.date_formats.append("%b/%d")
        self.date_formats.append("%d-%m")
        self.date_formats.append("%d/%m")
        self.date_formats.append("%d-%b")
        self.date_formats.append("%d/%b")

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #----------------------------------------
    def format_date(self, raw_date, output_format = None):
        for date_format in self.date_formats:
            try: new_date = datetime.strptime(raw_date, date_format)
            except: pass
            else: 
                if not output_format:
                    if len(raw_date) == 4:
                        output_format = '%Y'
                    elif len(raw_date) in (5,6):
                        output_format = '%m-%d'
                    elif len(raw_date) in (7,8):
                        output_format = '%Y-%m'
                    else:
                        output_format = '%Y-%m-%d'
                return datetime.strftime(new_date, output_format)
        return ''

#----------------------------------------
if __name__ == "__main__":

    print('\nInitialize mapper class')
    test_mapper = mapper()

    print('\nmap function result ...')
    test_result = test_mapper.map({"COLUMN1": "100", "COLUMN2": "NULL", "COLUMN3": "NAME"})
    print('\n' + json.dumps(test_result))

    print('\nclean_value tests ...')
    tests = []
    tests.append(['ABC    COMPANY', 'ABC COMPANY'])
    tests.append([' n/a', ''])
    tests.append([None, ''])
    for test in tests:
        result = test_mapper.clean_value(test[0])
        if result == test[1]:
            print('\t%s [%s] -> [%s]' % ('pass', test[0], test[1]))
        else:
            print('\t%s [%s] -> [%s] got [%s]' % ('FAIL!', test[0], test[1], result))

    print('\nformat_date tests ...')
    tests = []
    tests.append(['11/12/1927', '1927-11-12'])
    tests.append(['01-2027', '2027-01'])
    tests.append(['1-may-2020', '2020-05-01'])
    tests.append([None, ''])
    for test in tests:
        result = test_mapper.format_date(test[0])
        if result == test[1]:
            print('\t%s [%s] -> [%s]' % ('pass', test[0], test[1]))
        else:
            print('\t%s [%s] -> [%s] got [%s]' % ('FAIL!', test[0], test[1], result))


    print ('\ntests complete\n')
