import os
import sys
import argparse
import json
import re
from datetime import datetime
import time

#=========================
class csv_functions():

    #----------------------------------------
    def __init__(self):
        self.initialized = True
        self.statPack = {}

        variantFile = __file__.replace('.py', '.json')
        if not os.path.exists(variantFile):
            print('')
            print('File %s is missing!' % variantFile)
            print('')
            self.initialized = False
            return

        try: self.variantData = json.load(open(variantFile,'r', encoding='latin-1'))
        except json.decoder.JSONDecodeError as err:
            print('')
            print('JSON error %s in %s' % (err, variantFile))
            print('')
            self.initialized = False
            return

        #--turn lists into dictionaries for speed
        if 'GARBAGE_VALUES' not in self.variantData:
            self.variantData['GARBAGE_VALUES'] = {}
        else:
            self.variantData['GARBAGE_VALUES'] = dict(zip(self.variantData['GARBAGE_VALUES'], [''] * len(self.variantData['GARBAGE_VALUES'])))

        if 'ORGANIZATION_TOKENS' not in self.variantData:
            self.variantData['ORGANIZATION_TOKENS'] = {}
        else:
            self.variantData['ORGANIZATION_TOKENS'] = dict(zip(self.variantData['ORGANIZATION_TOKENS'], [''] * len(self.variantData['ORGANIZATION_TOKENS'])))

        if 'PERSON_TOKENS' not in self.variantData:
            self.variantData['PERSON_TOKENS'] = {}
        else:
            self.variantData['PERSON_TOKENS'] = dict(zip(self.variantData['PERSON_TOKENS'], [''] * len(self.variantData['PERSON_TOKENS'])))

        if 'SENZING_ATTRIBUTES' not in self.variantData:
            self.variantData['SENZING_ATTRIBUTES'] = []
        else:
            self.variantData['SENZING_ATTRIBUTES'] = dict(zip(self.variantData['SENZING_ATTRIBUTES'], [''] * len(self.variantData['SENZING_ATTRIBUTES'])))

        #--supported date formats
        self.dateFormats = []
        self.dateFormats.append("%Y-%m-%d")
        self.dateFormats.append("%m/%d/%Y")
        self.dateFormats.append("%d/%m/%Y")
        self.dateFormats.append("%d-%b-%Y")
        self.dateFormats.append("%Y")
        self.dateFormats.append("%Y-%M")
        self.dateFormats.append("%m-%Y")
        self.dateFormats.append("%m/%Y")
        self.dateFormats.append("%b-%Y")
        self.dateFormats.append("%b/%Y")
        self.dateFormats.append("%m-%d")
        self.dateFormats.append("%m/%d")
        self.dateFormats.append("%b-%d")
        self.dateFormats.append("%b/%d")
        self.dateFormats.append("%d-%m")
        self.dateFormats.append("%d/%m")
        self.dateFormats.append("%d-%b")
        self.dateFormats.append("%d/%b")

        #--set iso country code size 
        self.isoCountrySize = 'ISO3'
    
    #----------------------------------------
    def format_date(self, dateString, outputFormat = None):
        for dateFormat in self.dateFormats:
            try: dateValue = datetime.strptime(dateString, dateFormat)
            except: pass
            else: 
                if not outputFormat:
                    if len(dateString) == 4:
                        outputFormat = '%Y'
                    elif len(dateString) in (5,6):
                        outputFormat = '%m-%d'
                    elif len(dateString) in (7,8):
                        outputFormat = '%Y-%m'
                    else:
                        outputFormat = '%Y-%m-%d'
                return datetime.strftime(dateValue, outputFormat)
        return None

    #-----------------------------------
    def clean_value(self, valueString):
        #--remove extra spaces
        returnValue = ' '.join(str(valueString).strip().split())
        if returnValue.upper() in self.variantData['GARBAGE_VALUES']:
            returnValue = ''
        return returnValue

    #-----------------------------------
    def is_senzing_attribute(self, attrString):
        if attrString in self.variantData['SENZING_ATTRIBUTES']:
            return True
        return False

    #-----------------------------------
    def get_senzing_attribute(self, attrString):
        if attrString in self.variantData['SENZING_ATTRIBUTES']:
            return self.variantData['SENZING_ATTRIBUTES'][attrString]
        return {}

    #-----------------------------------
    def is_organization_name(self, nameString):
        if nameString:
            for token in nameString.lower().replace('.',' ').replace(',',' ').split():
                if token.upper() in self.variantData['ORGANIZATION_TOKENS']:
                    return True
        return False

    #-----------------------------------
    def is_person_name(self, nameString):
        if nameString:
            for token in nameString.lower().replace('.',' ').replace(',',' ').split():
                if token.upper() in self.variantData['PERSON_TOKENS']:
                    return True
        return False

#----------------------------------------
if __name__ == "__main__":
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()
    progressInterval = 10000

    #--test the instance
    csvFunctions = csv_functions()
    if csvFunctions.initialized:
        print('')
        print('successfully initialized!')
        print('')

    sys.exit()

