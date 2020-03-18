import os
import sys
import argparse
import configparser
import signal
import time
from datetime import datetime, timedelta
import csv
import json

try: from csv_functions import csv_functions
except: csv_functions = None

#--senzing python classes
try: 
    from G2Database import G2Database
    from G2Exception import G2Exception
    from G2Engine import G2Engine
except:
    print('')
    print('Please export PYTHONPATH=<path to senzing python directory>')
    print('')
    sys.exit(1)

#--see if a g2 config manager present - v1.12+
try: 
    from G2IniParams import G2IniParams
    from G2ConfigMgr import G2ConfigMgr
except: G2ConfigMgr = None

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    """ pause for debug purposes """
    try: response = input(question)
    except KeyboardInterrupt:
        response = None
        global shutDown
        shutDown = True
    return response

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shutDown
    shutDown = True
    return
        
#----------------------------------------
def getNextRow(inputFileReader, inputFileHeaders):
    csvData = None
    while not csvData:
        try: 
            row = next(inputFileReader)
        except: 
            break
        else:
            if row:
                csvData = dict(zip(inputFileHeaders, row))
            else:
                csvData = ','.join(row)
                print('bad row skipped [%s ...]' % csvData[0:50])
                continue
                
    return csvData
    
#----------------------------------------
def g2Search(g2Engine, jsonStr, searchFlags):
    try: 
        response = bytearray()
        if False: 
            retcode = g2Engine.searchByAttributes(jsonStr, response)
        else:
            retcode = g2Engine.searchByAttributesV2(jsonStr, searchFlags, response)
        response = response.decode() if response else ''
        #if len(response) > 500:
        #    print(json.dumps(json.loads(response), indent=4))
        #    pause()
    except G2ModuleException as err:
        print('')
        print(err)
        print('')
        response = '{"EXCEPTION!": "Yes"}'

    return response
    
#----------------------------------------
def dictKeysUpper(dict):
    return {k.upper():v for k,v in dict.items()}

#----------------------------------------
def processFile():
    """ search g2 for records in a csv file """
    print('Processing file ...')
    
    #--read the mapping file
    if not os.path.exists(mappingFileName):
        print('%s does not exist' % mappingFileName)
        return -1
    
    try: mappingDoc = json.load(open(mappingFileName, 'r'))
    except ValueError as err:
        print('mapping file error: %s in %s' % (err, mappingFileName))
        return -1
    
    except:
        print('%s contains invalid json' % mappingFileName)
        return -1

    #------------------------------------------------
    #--prepare the input file
    inputFileName = mappingDoc['input']['fileName']
    if not os.path.exists(inputFileName):
        print('%s does not exist' % inputFileName)
        return -1
        
    fileDialect = 'excel'
    if mappingDoc['input']['fileType'].lower() == 'tab':
        fileDialect = 'excel-tab'
    elif mappingDoc['input']['fileType'].lower() == 'pipe':
        csv.register_dialect('pipe', delimiter = '|', quotechar = '"')
        fileDialect = 'pipe'

    if 'encoding' in mappingDoc['input']: #--force an encoding
        inputFileHandle = open(inputFileName, 'r', encoding=mappingDoc['input']['encoding'])
    else:
        inputFileHandle = open(inputFileName, 'r')
    inputFileReader = csv.reader(inputFileHandle, dialect=fileDialect)
    
    inputFileHeaders = next(inputFileReader) #--[header.replace(' ','_') for header in next(inputFileReader)]
    if 'columns' in mappingDoc['input']: #--override column headers
        if type(mappingDoc['input']['columns']) == list:
            inputFileHeaders = []
            for column in mappingDoc['input']['columns']:
                inputFileHeaders.append(column['name'])
        else:
            inputFileHeaders = mappingDoc['input']['columns'].split(',')
    inputFileHeaders = [header.strip().replace(' ','_').upper() for header in inputFileHeaders]
        
    #------------------------------------------------
    #--prepare the output file
    mappingDoc['output']['fileType'] = mappingDoc['output']['fileType'].upper()

    #--initialize search flags
    #--
    #-- G2_ENTITY_MINIMAL_FORMAT = ( 1 << 18 )
    #-- G2_ENTITY_BRIEF_FORMAT = ( 1 << 20 )
    #-- G2_ENTITY_INCLUDE_NO_FEATURES
    #--
    #-- G2_EXPORT_INCLUDE_RESOLVED = ( 1 << 2 )
    #-- G2_EXPORT_INCLUDE_POSSIBLY_SAME = ( 1 << 3 )
    #--
    searchFlags = g2Engine.G2_ENTITY_INCLUDE_NO_RELATIONS

    #--build output headers
    recordDataRequested = False
    outputHeaders = []
    for ii in range(len(mappingDoc['output']['columns'])):
        columnName = mappingDoc['output']['columns'][ii]['name'].upper()
        mappingDoc['output']['columns'][ii]['name'] = columnName
        outputHeaders.append(columnName)
        if mappingDoc['output']['columns'][ii]['source'].upper() == 'RECORD':
            recordDataRequested = True

    #--use minimal format unless record data requested
    if recordDataRequested:
        searchFlags = searchFlags | g2Engine.G2_ENTITY_INCLUDE_NO_FEATURES 
    else:
        searchFlags = searchFlags | g2Engine.G2_ENTITY_MINIMAL_FORMAT 

    mappingDoc['output']['outputHeaders'] = outputHeaders

    if 'matchLevelFilter' not in mappingDoc['output'] or int(mappingDoc['output']['matchLevelFilter']) < 1:
        mappingDoc['output']['matchLevelFilter'] = 99
    else:
        mappingDoc['output']['matchLevelFilter'] = int(mappingDoc['output']['matchLevelFilter'])
        if mappingDoc['output']['matchLevelFilter'] == 1:
            searchFlags = searchFlags | g2Engine.G2_EXPORT_INCLUDE_RESOLVED
        elif mappingDoc['output']['matchLevelFilter'] == 2:
            searchFlags = searchFlags | g2Engine.G2_EXPORT_INCLUDE_RESOLVED | g2Engine.G2_EXPORT_INCLUDE_POSSIBLY_SAME

    if 'nameScoreFilter' not in mappingDoc['output']:
        mappingDoc['output']['nameScoreFilter'] = 0
    else:
        mappingDoc['output']['nameScoreFilter'] = int(mappingDoc['output']['nameScoreFilter'])

    if 'dataSourceFilter' not in mappingDoc['output']:
        mappingDoc['output']['dataSourceFilter'] = None
    else:
        mappingDoc['output']['dataSourceFilter'] = mappingDoc['output']['dataSourceFilter'].upper()

    if 'maxReturnCount' not in mappingDoc['output']:
        mappingDoc['output']['maxReturnCount'] = 1
    else:
        mappingDoc['output']['maxReturnCount'] = int(mappingDoc['output']['maxReturnCount'])
       
    #--open the output file
    if outputFileName:
        fileName = outputFileName
    else:
        fileName = mappingDoc['output']['fileName']
    outputFileHandle = open(fileName, 'w', encoding='utf-8', newline='')
    mappingDoc['output']['fileHandle'] = outputFileHandle
    if mappingDoc['output']['fileType'] != 'JSON':
        mappingDoc['output']['fileWriter'] = csv.writer(mappingDoc['output']['fileHandle'], dialect=csv.excel, quoting=csv.QUOTE_MINIMAL)
        mappingDoc['output']['fileWriter'].writerow(outputHeaders)

    #--upper case value replacements
    for ii in range(len(mappingDoc['input']['mappings'])):
        mappingDoc['input']['mappings'][ii]['value'] = mappingDoc['input']['mappings'][ii]['value'].upper().replace(')S', ')s')
    for ii in range(len(mappingDoc['output']['columns'])):
        mappingDoc['output']['columns'][ii]['value'] = mappingDoc['output']['columns'][ii]['value'].upper().replace(')S', ')s')

    #--initialize the stats
    scoreCounts = {}
    scoreCounts['best'] = {}
    scoreCounts['best']['total'] = 0
    scoreCounts['best']['>=100'] = 0
    scoreCounts['best']['>=95'] = 0
    scoreCounts['best']['>=90'] = 0
    scoreCounts['best']['>=85'] = 0
    scoreCounts['best']['>=80'] = 0
    scoreCounts['best']['>=75'] = 0
    scoreCounts['best']['>=70'] = 0
    scoreCounts['best']['<70'] = 0
    scoreCounts['additional'] = {}
    scoreCounts['additional']['total'] = 0
    scoreCounts['additional']['>=100'] = 0
    scoreCounts['additional']['>=95'] = 0
    scoreCounts['additional']['>=90'] = 0
    scoreCounts['additional']['>=85'] = 0
    scoreCounts['additional']['>=80'] = 0
    scoreCounts['additional']['>=75'] = 0
    scoreCounts['additional']['>=70'] = 0
    scoreCounts['additional']['<70'] = 0
    scoreCounts['name'] = {}
    scoreCounts['name']['total'] = 0
    scoreCounts['name']['=100'] = 0
    scoreCounts['name']['>=95'] = 0
    scoreCounts['name']['>=90'] = 0
    scoreCounts['name']['>=85'] = 0
    scoreCounts['name']['>=80'] = 0
    scoreCounts['name']['>=75'] = 0
    scoreCounts['name']['>=70'] = 0
    scoreCounts['name']['<70'] = 0
    rowsSkipped = 0
    rowsMatched = 0
    rowsNotMatched = 0
    resolvedMatches = 0
    possibleMatches = 0
    possiblyRelateds = 0
    nameOnlyMatches = 0

    #------------------------------------------------
    #--process the rows in the input file
    batchStartTime = time.time()
    rowCnt = 0
    csvData = getNextRow(inputFileReader, inputFileHeaders)
    while csvData:
        rowCnt += 1
        csvData['ROW_ID'] = rowCnt

        #--clean garbage values
        for key in csvData:
            try:
                if csvData[key].upper() in ('NULL', 'NONE', 'N/A', '\\N'):
                    csvData[key] = ''
            except: pass

        #--perform calculations
        mappingErrors = 0
        if 'calculations' in mappingDoc['input']:
            for calcDict in mappingDoc['input']['calculations']:
                try: csvData[calcDict['name']] = eval(calcDict['expression'])
                except Exception as err: 
                    print('  error: %s [%s]' % (calcDict['name'], err)) 
                    mappingErrors += 1

        if args.debug:
            print('-' * 50)
            print(csvData)

        #------------------------------------------------
        #--perform search
        requiredFieldsMissing = False
        anyFieldsRequirement = False
        anyFieldsFound = False
        searchValues = {}
        for columnDict in mappingDoc['input']['mappings']:
            try: columnValue = columnDict['value'] % csvData
            except: 
                print('warning: could not find %s' % (columnDict['value'],)) 
                columnValue = ''
                
            if not columnValue or columnValue == 'None':
                columnValue = ''

            if 'required' in columnDict:
                if columnDict['required'].upper() == 'YES' and not columnValue:
                    requiredFieldsMissing = True
                    break
                elif columnDict['required'].upper() == 'ANY':
                    anyFieldsRequirement = True
                    if columnValue:
                        anyFieldsFound = True
                    
            if columnValue: #--dont write empty tags
                searchValues[columnDict['name']] = columnValue

        if args.debug: 
            print(json.dumps(searchValues))
            
        searchResult = '{"SEARCH_RESPONSE": {"RESOLVED_ENTITIES": []}}'
        if anyFieldsRequirement and not anyFieldsFound:
            requiredFieldsMissing = True
            if args.debug: 
                print('required fields were missing!')

        if not requiredFieldsMissing:

            searchResult = g2Search(g2Engine, json.dumps(searchValues), searchFlags)
            if 'EXCEPTION!' in searchResult:
                processingError = 1
                break
        
        #------------------------------------------------
        #--format output
        jsonResponse = json.loads(searchResult)
        matchList = []
        for resolvedEntity in jsonResponse['RESOLVED_ENTITIES']:

            #--create a list of data sources we found them in
            dataSources = {}
            for record in resolvedEntity['ENTITY']['RESOLVED_ENTITY']['RECORDS']:
                dataSource = record['DATA_SOURCE']
                if dataSource not in dataSources:
                    dataSources[dataSource] = [record['RECORD_ID']]
                else:
                    dataSources[dataSource].append(record['RECORD_ID'])

            dataSourceList = []
            for dataSource in dataSources:
                if len(dataSources[dataSource]) == 1:
                    dataSourceList.append(dataSource + ': ' + dataSources[dataSource][0])
                else:
                    dataSourceList.append(dataSource + ': ' + str(len(dataSources[dataSource])) + ' records')

            #--determine the matching criteria
            matchLevel = int(resolvedEntity['MATCH_INFO']['MATCH_LEVEL'])
            matchKey = resolvedEntity['MATCH_INFO']['MATCH_KEY'] if resolvedEntity['MATCH_INFO']['MATCH_KEY'] else '' 
            matchKey = matchKey.replace('+RECORD_TYPE', '')

            scoreData = []
            bestScores = {}
            bestScores['NAME'] = {}
            bestScores['NAME']['score'] = 0
            bestScores['NAME']['value'] = 'n/a'
            for featureCode in resolvedEntity['MATCH_INFO']['FEATURE_SCORES']:
                if featureCode == 'NAME':
                    scoreCode = 'GNR_FN'
                else: 
                    scoreCode = 'FULL_SCORE'
                for i in range(len(resolvedEntity['MATCH_INFO']['FEATURE_SCORES'][featureCode])):
                    matchingScore= resolvedEntity['MATCH_INFO']['FEATURE_SCORES'][featureCode][i][scoreCode]
                    matchingValue = resolvedEntity['MATCH_INFO']['FEATURE_SCORES'][featureCode][i]['CANDIDATE_FEAT']
                    scoreData.append('%s|%s|%s|%s' % (featureCode, scoreCode, matchingScore, matchingValue))
                    if featureCode not in bestScores:
                        bestScores[featureCode] = {}
                        bestScores[featureCode]['score'] = 0
                        bestScores[featureCode]['value'] = 'n/a'
                    if matchingScore > bestScores[featureCode]['score']:
                        bestScores[featureCode]['score'] = matchingScore
                        bestScores[featureCode]['value'] = matchingValue

            #--matchScore = str(((5-resolvedEntity['MATCH_INFO']['MATCH_LEVEL']) * 100) + int(resolvedEntity['MATCH_INFO']['MATCH_SCORE'])) + '-' + str(1000+bestScores['NAME']['score'])[-3:]
            scoreMatrix = {}
            scoreMatrix['+NAME'] = 80
            scoreMatrix['+DOB'] = 10
            scoreMatrix['-DOB'] = -10
            scoreMatrix['+ISO_COUNTRY'] = 5
            attrScore = 0
            for key in scoreMatrix.keys():
                if key in matchKey:
                    attrScore += int(round(((scoreMatrix[key] * bestScores[key[1:]]['score']) / 100),0))

            #--create the possible match entity one-line summary
            matchedEntity = {}
            matchedEntity['ENTITY_ID'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['ENTITY_ID']
            if 'ENTITY_NAME' in resolvedEntity['ENTITY']['RESOLVED_ENTITY']:
                matchedEntity['ENTITY_NAME'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['ENTITY_NAME'] + (('\n aka: ' + bestScores['NAME']['value']) if bestScores['NAME']['value'] and bestScores['NAME']['value'] != resolvedEntity['ENTITY']['RESOLVED_ENTITY']['ENTITY_NAME'] else '')
            else:
                matchedEntity['ENTITY_NAME'] = bestScores['NAME']['value'] if 'NAME' in bestScores else ''
            matchedEntity['ENTITY_SOURCES'] = '\n'.join(dataSourceList)
            matchedEntity['MATCH_LEVEL'] = matchLevel
            matchedEntity['MATCH_KEY'] = matchKey[1:]
            matchedEntity['MATCH_SCORE'] = attrScore
            matchedEntity['NAME_SCORE'] = bestScores['NAME']['score']
            matchedEntity['SCORE_DATA'] = '\n'.join(sorted(map(str, scoreData)))

            if args.debug:
                print(json.dumps(matchedEntity, indent=4))
                print()
            matchedEntity['RECORDS'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['RECORDS']

            #--check the output filters
            filteredOut = False
            if matchLevel > mappingDoc['output']['matchLevelFilter']:
                filteredOut = True
                if args.debug:
                    print(' ** did not meet matchLevelFilter **')
            if bestScores['NAME']['score'] < mappingDoc['output']['nameScoreFilter']:
                filteredOut = True
                if args.debug:
                    print(' ** did not meet nameScoreFilter **')
            if mappingDoc['output']['dataSourceFilter'] and mappingDoc['output']['dataSourceFilter'] not in dataSources:
                filteredOut = True
                if args.debug:
                    print(' ** did not meet dataSourceFiler **')
            if not filteredOut:
                matchList.append(matchedEntity)

        #--set the no match condition
        if len(matchList) == 0:
            if requiredFieldsMissing:
                rowsSkipped += 1
            else:
                rowsNotMatched += 1
            matchedEntity = {}
            matchedEntity['ENTITY_ID'] = 0
            matchedEntity['ENTITY_NAME'] = ''
            matchedEntity['ENTITY_SOURCES'] = ''
            matchedEntity['MATCH_NUMBER'] = 0
            matchedEntity['MATCH_LEVEL'] = 0
            matchedEntity['MATCH_KEY'] = ''
            matchedEntity['MATCH_SCORE'] = ''
            matchedEntity['NAME_SCORE'] = ''
            matchedEntity['SCORE_DATA'] = ''
            matchedEntity['RECORDS'] = []
            matchList.append(matchedEntity)
            if args.debug:
                print(' ** no matches found **')
        else:
            rowsMatched += 1
            
        #----------------------------------
        #--create the output rows
        matchNumber = 0
        for matchedEntity in sorted(matchList, key=lambda x: x['MATCH_SCORE'], reverse=True):
            matchNumber += 1
            matchedEntity['MATCH_NUMBER'] = matchNumber

            if matchedEntity['MATCH_SCORE']:
                score = int(matchedEntity['MATCH_SCORE'])
                level = 'best' if matchNumber == 1 else 'additional'
                scoreCounts[level]['total'] += 1
                if score >= 100:
                    scoreCounts[level]['>=100'] += 1
                elif score >= 95:
                    scoreCounts[level]['>=95'] += 1
                elif score >= 90:
                    scoreCounts[level]['>=90'] += 1
                elif score >= 85:
                    scoreCounts[level]['>=85'] += 1
                elif score >= 80:
                    scoreCounts[level]['>=80'] += 1
                elif score >= 75:
                    scoreCounts[level]['>=75'] += 1
                elif score >= 70:
                    scoreCounts[level]['>=70'] += 1
                else:
                    scoreCounts[level]['<70'] += 1

            if matchedEntity['NAME_SCORE']:
                score = int(matchedEntity['NAME_SCORE'])
                level = 'name'
                scoreCounts[level]['total'] += 1
                if score >= 100:
                    scoreCounts[level]['=100'] += 1
                elif score >= 95:
                    scoreCounts[level]['>=95'] += 1
                elif score >= 90:
                    scoreCounts[level]['>=90'] += 1
                elif score >= 85:
                    scoreCounts[level]['>=85'] += 1
                elif score >= 80:
                    scoreCounts[level]['>=80'] += 1
                elif score >= 75:
                    scoreCounts[level]['>=75'] += 1
                elif score >= 70:
                    scoreCounts[level]['>=70'] += 1
                else:
                    scoreCounts[level]['<70'] += 1

            if matchNumber > mappingDoc['output']['maxReturnCount']:
                break

            #--get the column values
            uppercasedJsonData = False
            rowValues = []
            for columnDict in mappingDoc['output']['columns']:
                columnValue = ''
                try: 
                    if columnDict['source'].upper() == 'CSV':
                        columnValue = columnDict['value'] % csvData
                    elif columnDict['source'].upper() == 'API':
                        columnValue = columnDict['value'] % matchedEntity
                except: 
                    print('warning: could not find %s' % (columnDict['value'],)) 

                #--comes from the records
                if columnDict['source'].upper() == 'RECORD':
                    #if not uppercasedJsonData:
                    #    record['JSON_DATA'] = dictKeysUpper(record['JSON_DATA'])
                    #    uppercasedJsonData = True
                    columnValues = []
                    for record in matchedEntity['RECORDS']:
                        if columnDict['value'].upper().endswith('_DATA'):
                            for item in record[columnDict['value'].upper()]:
                                if not item.startswith('CK_'):
                                    columnValues.append(item)
                        else:
                            try: thisValue = columnDict['value'] % record['JSON_DATA']
                            except: pass
                            else:
                                if thisValue and thisValue not in columnValues:
                                    columnValues.append(thisValue)
                    if columnValues:
                        columnValue = '\n'.join(sorted(map(str, columnValues)))

                #if args.debug:
                #    print(columnDict['value'], columnValue)
                if len(columnValue) > 32000:
                    columnValue = columnValue[0:32000]
                    print('column %s truncated at 32k' % columnDict['name'])
                rowValues.append(columnValue.replace('\n', '|'))
                        
            #--write the record
            if mappingDoc['output']['fileType'] != 'JSON':
                mappingDoc['output']['fileWriter'].writerow(rowValues)
            else:
                mappingDoc['output']['fileHandle'].write(json.dumps(rowValues) + '\n')

            #--update the counters
            if matchedEntity['MATCH_LEVEL'] == 1:
                resolvedMatches += 1
            elif matchedEntity['MATCH_LEVEL'] == 2:
                possibleMatches += 1
            elif matchedEntity['MATCH_LEVEL'] == 3:
                possiblyRelateds += 1
            elif matchedEntity['MATCH_LEVEL'] == 4:
                nameOnlyMatches += 1
                    
        if rowCnt % sqlCommitSize == 0:
            now = datetime.now().strftime('%I:%M%p').lower()
            elapsedMins = round((time.time() - procStartTime) / 60, 1)
            eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
            batchStartTime = time.time()
            print(' %s rows searched at %s, %s per second ... %s rows matched, %s resolved matches, %s possible matches, %s possibly related, %s name only' % (rowCnt, now, eps, rowsMatched, resolvedMatches, possibleMatches, possiblyRelateds, nameOnlyMatches))

        if args.debug:
            #print(json.dumps(matchedEntity['RECORDS'], indent=4))
            pause()

        #--forced shutdown
        if shutDown:
            break
            
        #--next record
        csvData = getNextRow(inputFileReader, inputFileHeaders)

    now = datetime.now().strftime('%I:%M%p').lower()
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    eps = int(float(sqlCommitSize) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))
    batchStartTime = time.time()
    print(' %s rows searched at %s, %s per second ... %s rows matched, %s resolved matches, %s possible matches, %s possibly related, %s name only' % (rowCnt, now, eps, rowsMatched, resolvedMatches, possibleMatches, possiblyRelateds, nameOnlyMatches))
    print(json.dumps(scoreCounts, indent = 4))    
    #--close all inputs and outputs
    inputFileHandle.close()
    outputFileHandle.close()

    return shutDown

#----------------------------------------
if __name__ == "__main__":
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()
    sqlCommitSize = 100
    
    #--defaults
    iniFileName = os.getenv('SENZING_INI_FILE_NAME') if os.getenv('SENZING_INI_FILE_NAME', None) else appPath + os.path.sep + 'G2Module.ini'

    argParser = argparse.ArgumentParser()
    argParser.add_argument('-c', '--ini_file_name', dest='ini_file_name', default=iniFileName, help='name of the g2.ini file, defaults to %s' % iniFileName)
    argParser.add_argument('-m', '--mappingFile', dest='mappingFileName', help='the the name of a mapping file')
    argParser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    argParser.add_argument('-D', '--debug', dest='debug', action='store_true', default=False, help='print debug statements')

    #parser.add_argument('inputFileName', type=str, help='the name of an input file' )
    args = argParser.parse_args()
    iniFileName = args.ini_file_name
    mappingFileName = args.mappingFileName
    outputFileName = args.outputFileName
    g2debugFlag = args.debug

    #--get parameters from ini file
    if not os.path.exists(iniFileName):
        print('')
        print('ini file %s not found!' % iniFileName)
        print('')
        sys.exit(1)
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)

    #--use config file if in the ini file, otherwise expect to get from database with config manager lib
    #print(iniParser.get('SQL', 'G2CONFIGFILE'))
    try: configTableFile = iniParser.get('SQL', 'G2CONFIGFILE')
    except: configTableFile = None
    if not configTableFile and not G2ConfigMgr:
        print('')
        print('Config information missing from ini file and no config manager present!')
        print('')
        sys.exit(1)

    #--get the config from the file
    if configTableFile:
        try: cfgData = json.load(open(configTableFile), encoding="utf-8")
        except ValueError as e:
            print('')
            print('G2CONFIGFILE: %s has invalid json' % configTableFile)
            print(e)
            print('')
            sys.exit(1)
        except IOError as e:
            print('')
            print('G2CONFIGFILE: %s was not found' % configTableFile)
            print(e)
            print('')
            sys.exit(1)

    #--get the config from the config manager
    else:
        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(iniFileName)
        try: 
            g2ConfigMgr = G2ConfigMgr()
            g2ConfigMgr.initV2('pyG2ConfigMgr', iniParams, False)
            defaultConfigID = bytearray() 
            g2ConfigMgr.getDefaultConfigID(defaultConfigID)
            if len(defaultConfigID) == 0:
                print('')
                print('No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            defaultConfigDoc = bytearray() 
            g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
            if len(defaultConfigDoc) == 0:
                print('')
                print('No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            cfgData = json.loads(defaultConfigDoc.decode())
            g2ConfigMgr.destroy()
        except:
            #--error already printed by the api wrapper
            sys.exit(1)

    #--initialize the g2engine
    try:
        g2Engine = G2Engine()
        if configTableFile:
            g2Engine.init('csv_search_viewer', iniFileName, False)
        else:
            iniParamCreator = G2IniParams()
            iniParams = iniParamCreator.getJsonINIParams(iniFileName)
            g2Engine.initV2('csv_search', iniParams, False)
    except G2Exception as err:
        print('')
        print('Could not initialize the G2 Engine')
        print(str(err))
        print('')
        sys.exit(1)

    #--load the csv functions if available
    if csv_functions:
        csv_functions = csv_functions()
        if not csv_functions.initialized:
            sys.exit(1)

    returnCode = processFile()

    print('')
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if returnCode == 0:
        print('Process completed successfully in %s minutes!' % elapsedMins)
    else:
        print('Process aborted after %s minutes!' % elapsedMins)
    print('')

    g2Engine.destroy()
    sys.exit(returnCode)
