import os
import sys
import argparse
import json
import re
import random
from datetime import datetime
import time

try: import probablepeople as pp
except: pp = None
#pp = None

#=========================
class csv_functions():

    #----------------------------------------
    def __init__(self):
        self.initialized = True
        self.statPack = {}

        self.variantFile = __file__.replace('.py', '.json')
        if not os.path.exists(self.variantFile):
            print('')
            print('File %s is missing!' % self.variantFile)
            print('')
            self.initialized = False
            return

        try: self.variantJson = json.load(open(self.variantFile,'r', encoding='latin-1'))
        except json.decoder.JSONDecodeError as err:
            print('')
            print('JSON error %s in %s' % (err, self.variantFile))
            print('')
            self.initialized = False
            return

        #--ensure these lists exist
        keys = [
            "GARBAGE_VALUES", 
            "ORGANIZATION_TOKENS", 
            "PERSON_TOKENS", 
            "SENZING_ATTRIBUTES",
            "NAME_SPLIT_TOKENS",
            "NAME_ENDER_TOKENS"
        ]
        for key in keys:
            if key not in self.variantJson:
                self.variantJson[key] = []

        #--turn lists into dictionaries for speed
        self.variantData = {}
        for variantList in self.variantJson:
            if type(self.variantJson[variantList]) == list:
                self.variantData[variantList] = dict(zip([i.upper() for i in self.variantJson[variantList]], [''] * len(self.variantJson[variantList])))
            else:
                self.variantData[variantList] = self.variantJson[variantList]

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
    def clean_value(self, attrName, attrValue):
        #--remove extra spaces
        newValue = ' '.join(str(attrValue).strip().split())
        #--whole field must match a garbage value
        if newValue.upper() in self.variantData['GARBAGE_VALUES']: 
            self.updateStat('GARBAGE_IN_FIELDS', attrName, newValue.upper())
            return ''
        return newValue

    #-----------------------------------
    def parse_name(self, nameString):
        #--notes: clean name has already been done
        #--       sorry names will be forced upper case
        #--       returns a dictionary of names

        #--remove in garbage expressions in the string
        nameString = nameString.upper()
        for garbageValue in self.variantData['GARBAGE_VALUES']:
            if garbageValue in nameString:
                self.updateStat('GARBAGE_IN_NAMES', garbageValue, nameString)
                nameString = nameString.replace(garbageValue,'').strip()
        newString = nameString

        primaryNameTokens = []
        secondaryNameTokens = []
        referenceNameTokens = []

        #--remove tokens in parenthesis
        groupedStrings = re.findall('\(.*?\)',newString)
        for groupedString in groupedStrings:
            self.updateStat('GROUPED_STRINGS', '()', newString + ' | ' + groupedString)
            newString = newString.replace(groupedString,'')
            referenceNameTokens.append(groupedString)

        #--split the name
        theToken = None
        split = 0
        for token in newString.replace('.',' ').replace(',',' ').replace('-',' - ').replace('/',' / ').replace(';',' ; ').upper().split():
            if split == 1:
                secondaryNameTokens.append(token)
            elif split == 2:
                referenceNameTokens.append(token)
            elif token in self.variantData['NAME_SPLIT_TOKENS']:
                #--token is skipped
                split=1
                theToken = token
            elif token in self.variantData['NAME_ENDER_TOKENS']:
                primaryNameTokens.append(token)
                split=2
                theToken = token
            else:
                primaryNameTokens.append(token)

        primaryNameStr = ' '.join(primaryNameTokens)
        secondaryNameStr = ' '.join(secondaryNameTokens)
        referenceNameStr = ' '.join(referenceNameTokens)

        if secondaryNameStr:
            self.updateStat('NAME_SPLITERS', theToken, nameString + ' -> ' + primaryNameStr + ' | ' + secondaryNameStr)
        if referenceNameStr and split == 2:
            self.updateStat('NAME_ENDERS', theToken, nameString + ' -> ' + primaryNameStr + ' | ' + referenceNameStr)

        #--probable people parser
        if pp:
            #--pp.tag(name_str) # expected output: (OrderedDict([('PrefixMarital', 'Mr'), ('GivenName', 'George'), ('Nickname', '"Gob"'), ('Surname', 'Bluth'), ('SuffixGenerational', 'II')]), 'Person')
            #--pp.tag(corp_str) # expected output: (OrderedDict([('CorporationName', 'Sitwell Housing'), ('CorporationLegalType', 'Inc')]), 'Corporation')
            #--PrefixMarital
            #--PrefixOther
            #--GivenName
            #--FirstInitial
            #--MiddleName
            #--MiddleInitial
            #--Surname
            #--LastInitial
            #--SuffixGenerational
            #--SuffixOther
            #--Nickname
            #--And
            #--CorporationName
            #--CorporationNameOrganization
            #--CorporationLegalType
            #--CorporationNamePossessiveOf
            #--ShortForm
            #--ProxyFor
            #--AKA
            try: 
                taggedName, nameType = pp.tag(primaryNameStr)
                isOrganization = False if nameType == 'Person' else True
                self.updateStat('ProbablePeople', nameType, primaryNameStr)
            except: 
                isOrganization = self.is_organization_name(primaryNameStr)

        #--home grown parser
        else:
            isOrganization = self.is_organization_name(primaryNameStr)

        if isOrganization:
            primaryNameOrg = primaryNameStr
            secondaryNameOrg = secondaryNameStr
            primaryNameFull = ''
            secondaryNameFull = ''
        else:    
            primaryNameOrg = ''
            secondaryNameOrg = ''
            primaryNameFull = primaryNameStr
            secondaryNameFull = secondaryNameStr

        nameList = []
        nameList.append({'IS_ORGANIZATION': True})
        nameList.append({'PRIMARY_NAME_ORG': primaryNameOrg})
        nameList.append({'SECONDARY_NAME_ORG': secondaryNameOrg})
        nameList.append({'PRIMARY_NAME_FULL': primaryNameFull})
        nameList.append({'SECONDARY_NAME_FULL': secondaryNameFull})
        nameList.append({'REFERENCE_NAME': referenceNameStr})

        return nameList
    

    #-----------------------------------
    def parse_provider_column(self, _str):
        if len(str(_str).strip()) == 0:
            return ''
        #example input: PHA410230, PHA393440, 1821137118
        #example ouput: "NPI_NUMBERS": [{"NPI_NUMBER": "PHA410230"}, {"NPI_NUMBER": "PHA393440"}, {"NPI_NUMBER": "1821137118"}]
        numberList = [{"NPI_NUMBER": x.strip()} for x in _str.split(',')]
        return numberList

    #-----------------------------------
    def is_organization_name(self, nameString):
        tokenCnt = 0
        priorTokens = []
        for token in nameString.replace('.',' ').replace(',',' ').replace('-',' ').upper().split():
            if token in self.variantData['ORGANIZATION_TOKENS']:
                self.updateStat('ORGANIZATION_TOKENS', token, nameString)
                return True
            elif ' '.join(priorTokens[-2:]) in self.variantData['ORGANIZATION_TOKENS']:
                self.updateStat('ORGANIZATION_TOKENS', ' '.join(priorTokens[-2:]), nameString)
                return True
            elif ' '.join(priorTokens[-3:]) in self.variantData['ORGANIZATION_TOKENS']:
                self.updateStat('ORGANIZATION_TOKENS', ' '.join(priorTokens[-3:]), nameString)
                return True
            priorTokens.append(token)
            tokenCnt += 1
        if tokenCnt > 0:
            self.updateStat('PERSONS_BY_TOKEN_COUNT', tokenCnt, nameString)

        return False

    #-----------------------------------
    def is_person_name(self, nameString):
        priorTokens = []
        for token in nameString.replace('.',' ').replace(',',' ').replace('-',' ').upper().split():
            if token in self.variantData['PERSON_TOKENS'] or \
                ' '.join(priorTokens[-2:]) in self.variantData['PERSON_TOKENS'] or \
                ' '.join(priorTokens[-3:]) in self.variantData['PERSON_TOKENS']:
                return True
            priorTokens.append(token)
        return False

    #-----------------------------------
    def is_senzing_attribute(self, attrName):
        attrName = attrName.upper()
        if attrName in self.variantData['SENZING_ATTRIBUTES']:
            return True
        elif '_' in attrName:
            baseName = attrName[attrName.find('_') + 1:]
            if baseName in self.variantData['SENZING_ATTRIBUTES']:
                return True
            else:
                baseName = attrName[0:attrName.rfind('_')]
                if baseName in self.variantData['SENZING_ATTRIBUTES']:
                    return True
        return False

    #-----------------------------------
    def get_senzing_attribute(self, attrName):
        attrName = attrName.upper()
        if attrName in self.variantData['SENZING_ATTRIBUTES']:
            return self.variantData['SENZING_ATTRIBUTES'][attrName]
        elif '_' in attrName:
            baseName = attrName[attrName.find('_') + 1:]
            if baseName in self.variantData['SENZING_ATTRIBUTES']:
                return self.variantData['SENZING_ATTRIBUTES'][baseName]
            else:
                baseName = attrName[0:attrName.rfind('_')]
                if baseName in self.variantData['SENZING_ATTRIBUTES']:
                    return self.variantData['SENZING_ATTRIBUTES'][baseName]
        return {}

    #----------------------------------------
    def updateStat(self, cat1, cat2, example = None):
        if cat1 not in self.statPack:
            self.statPack[cat1] = {}
        if cat2 not in self.statPack[cat1]:
            self.statPack[cat1][cat2] = {}
            self.statPack[cat1][cat2]['count'] = 0

        self.statPack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.statPack[cat1][cat2]:
                self.statPack[cat1][cat2]['examples'] = []
            if example not in self.statPack[cat1][cat2]['examples']:
                if len(self.statPack[cat1][cat2]['examples']) < 100:
                    self.statPack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(25,99)
                    self.statPack[cat1][cat2]['examples'][randomSampleI] = example
        return

#----------------------------------------
if __name__ == "__main__":
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    #--test the instance
    csvFunctions = csv_functions()
    if not csvFunctions.initialized:
        sys.exit(1)

    #--see if they entered arguments
    argStr = ' '.join(sys.argv[1:])
    if len(sys.argv) == 1 or 'help' in argStr.lower() or '-h' in argStr.lower():
        print()
        print('possible commands ...')
        print('\tdisplay lists')
        print('\tadd <token> to <list>')
        #print('\tdelete token from list')
        print('\tadd file asdf.txt to list')
        print()
        sys.exit(0)

    if argStr.lower() == 'display lists':
        print()
        for key in csvFunctions.variantJson.keys():
            print (key)
        print()
        sys.exit(0)

    if sys.argv[1].lower() == 'add' and len(sys.argv) >= 5: 
        variantTokens = []
        fileName = ''
        targetList = ''
        priorToken = ''
        for token in sys.argv[2:]:
            if priorToken == 'file':
                fileName = token
            elif priorToken == 'to':
                targetList = token
            elif token.lower() not in ('file', 'to'):
                variantTokens.append(token)
            priorToken = token.lower()

        variant = ' '.join(variantTokens).upper()
        targetList = targetList.upper()
        errorText = ''

        if targetList not in csvFunctions.variantJson:
            print()
            ok = input('%s is not a current list, create it? (Y/n) ' % targetList)
            if ok.upper().startswith('Y'):
                csvFunctions.variantJson[targetList] = []
            else:
                errorText = 'aborted!'
        elif fileName and variant:
            #print ('fileName = %s' % fileName)
            #print ('variant = %s' % variant)
            errorText = 'command not recognized'
        elif fileName and not os.path.exists(fileName):
            errorText = '%s not found' % fileName

        if errorText:
            print()
            print(errorText)
            print()
            sys.exit(1)
        else:

            print()
            addCnt = 0
            skipCnt = 0
            if fileName:
                variantList = [line. rstrip('\n') for line in open(fileName)]
            else:
                variantList = [variant]
            for variant in variantList:
                variant = variant.upper()
                if variant in csvFunctions.variantJson[targetList]:
                    print('exists: %s' % variant)
                    skipCnt += 1
                else:
                    csvFunctions.variantJson[targetList].append(variant)
                    print('added: %s' % variant)
                    addCnt += 1
            print()
            if addCnt > 0:
                print()
                ok = input('ok to save? (Y/n) ')
                if ok.upper().startswith('Y'):

                    #--ensures lists are sorted
                    for variantList in csvFunctions.variantJson:
                        if type(csvFunctions.variantJson[variantList]) == list:
                            csvFunctions.variantJson[variantList] = sorted(csvFunctions.variantJson[variantList])
                    #--write to file
                    with open(csvFunctions.variantFile, 'w') as f:
                        json.dump(csvFunctions.variantJson, f, indent=4, sort_keys = True)
                    print()
                    print('saved!')
                    print()

            sys.exit(0)


    print()
    print('command not recognized')
    print()
    sys.exit(1)
