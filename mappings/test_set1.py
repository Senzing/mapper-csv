import csv
import json
from datetime import datetime

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.delimiter = ','

        #--load any reference data for supporting functions
        self.load_reference_data()

    #----------------------------------------
    def map(self, raw_data):
        new_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here
        if not raw_data['name']:
            return None

        #--place any calculations needed here
        is_organization = self.is_organization_name(raw_data['name'])


        #--mandatory attributes
        new_data['DATA_SOURCE'] = 'TEST'
        new_data['ENTITY_TYPE'] = 'ORGANIZATION' if is_organization else 'PERSON'

        #--the record_id should be unique, remove this mapping if there is not one 
        new_data['RECORD_ID'] = raw_data['uniqueid']

        #--column mappings

        # columnName: uniqueid
        # 100.0 populated, 100.0 unique
        #      1001 (1)
        #      1002 (1)
        #      1003 (1)
        #      1004 (1)
        #      1005 (1)
        #new_data['uniqueid'] = raw_data['uniqueid']

        # columnName: type
        # 100.0 populated, 22.22 unique
        #      company (5)
        #      individual (4)
        #new_data['type'] = raw_data['type']

        # columnName: name
        # 88.89 populated, 100.0 unique
        #      ABC Company (1)
        #      Bob Jones (1)
        #      General Hospital (1)
        #      Mary Smith (1)
        #      Peter  Anderson (1)
        if is_organization:
            new_data['NAME_ORG'] = raw_data['name']
        else:
            new_data['NAME_FULL'] = raw_data['name']

        # columnName: gender
        # 100.0 populated, 11.11 unique
        #      u (9)
        #new_data['gender'] = raw_data['gender']

        # columnName: dob
        # 22.22 populated, 100.0 unique
        #      2/2/92 (1)
        #      3/3/93 (1)
        new_data['DATE_OF_BIRTH'] = raw_data['dob']

        # columnName: ssn
        # 22.22 populated, 100.0 unique
        #      333-33-3333 (1)
        #      666-66-6666 (1)
        new_data['SSN_NUMBER'] = raw_data['ssn']

        # columnName: addr1
        # 88.89 populated, 100.0 unique
        #      111 First (1)
        #      222 Second (1)
        #      333 Third (1)
        #      444 Fourth (1)
        #      555 Fifth (1)
        new_data['ADDR_LINE1'] = raw_data['addr1']

        # columnName: city
        # 88.89 populated, 12.5 unique
        #      Las Vegas (8)
        new_data['ADDR_CITY'] = raw_data['city']

        # columnName: state
        # 88.89 populated, 12.5 unique
        #      NV (8)
        new_data['ADDR_STATE'] = raw_data['state']

        # columnName: zip
        # 88.89 populated, 100.0 unique
        #      89111 (1)
        #      89112 (1)
        #      89113 (1)
        #      89114 (1)
        #      89115 (1)
        new_data['ADDR_POSTAL_CODE'] = raw_data['zip']

        # columnName: create_date
        # 88.89 populated, 100.0 unique
        #      1/1/01 (1)
        #      2/2/02 (1)
        #      3/3/03 (1)
        #      4/4/04 (1)
        #      5/5/05 (1)
        new_data['important_date'] = raw_data['create_date']

        # columnName: status
        # 88.89 populated, 25.0 unique
        #      Active (6)
        #      Inactive (2)
        new_data['important_status'] = raw_data['status']

        # columnName: value
        # 88.89 populated, 100.0 unique
        #      1000 (1)
        #      2000 (1)
        #      3000 (1)
        #      4000 (1)
        #      5000 (1)
        #new_data['value'] = raw_data['value']

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

        #--orgnization tokens
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
    def format_date(self, date_string, output_format = None):
        for date_format in self.date_formats:
            try: date_value = datetime.strptime(date_string, date_format)
            except: pass
            else: 
                if not output_format:
                    if len(date_string) == 4:
                        output_format = '%Y'
                    elif len(date_string) in (5,6):
                        output_format = '%m-%d'
                    elif len(date_string) in (7,8):
                        output_format = '%Y-%m'
                    else:
                        output_format = '%Y-%m-%d'
                return datetime.strftime(date_value, output_format)
        return ''

    #-----------------------------------
    def is_organization_name(self, name_string):
        prior_tokens = []
        for token in name_string.replace('.',' ').replace(',',' ').replace('-',' ').upper().split():
            if token in self.variant_data['ORGANIZATION_TOKENS']:
                return True
            elif ' '.join(prior_tokens[-2:]) in self.variant_data['ORGANIZATION_TOKENS']:
                return True
            elif ' '.join(prior_tokens[-3:]) in self.variant_data['ORGANIZATION_TOKENS']:
                return True
            prior_tokens.append(token)
        return False

#----------------------------------------
if __name__ == "__main__":

    print('\nInitialize mapper class')
    test_mapper = mapper()

    print('\nmap function result ...')
    test_result = test_mapper.map({"COLUMN1": "100", "COLUMN2": "NULL", "COLUMN3": "NAME"})
    print('\n' + json.dumps(test_result))

    print('\nclean value tests ...')
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

    print('\nformat date tests ...')
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
