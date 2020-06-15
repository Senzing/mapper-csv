import os
import sys
import argparse
import configparser
import signal
import time
from datetime import datetime, timedelta
import csv
import json
import glob
from csv_functions import csv_functions

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
def getNextRow(fileInfo):
    errCnt = 0
    rowData = None
    while not rowData:

        #--quit for consecutive errors
        if errCnt >= 10:
            fileInfo['ERROR'] = 'YES'
            print()
            print('Shutdown due to too many errors')
            break
             
        try: line = next(fileInfo['reader'])
        except StopIteration:
            break
        except: 
            print(' row %s: %s' % (fileInfo['rowCnt'], sys.exc_info()[0]))
            fileInfo['skipCnt'] += 1
            errCnt += 1
            continue
        fileInfo['rowCnt'] += 1
        if line: #--skip empty lines

            #--csv reader will return a list (mult-char delimiter must be manually split)
            if type(line) == list:
                row = line
            else:
                row = [removeQuoteChar(x.strip()) for x in line.split(fileInfo['delimiter'])]

            #--turn into a dictionary if there is a header
            if 'header' in fileInfo:

                #--column mismatch
                if len(row) != len(fileInfo['header']):
                    print(' row %s has %s columns, expected %s' % (fileInfo['rowCnt'], len(row), len(fileInfo['header'])))
                    fileInfo['skipCnt'] += 1
                    errCnt += 1
                    continue

                #--is it the header row
                elif str(row[0]).upper() == fileInfo['header'][0].upper() and str(row[len(row)-1]).upper() == fileInfo['header'][len(fileInfo['header'])-1].upper():
                    fileInfo['skipCnt'] += 1
                    if fileInfo['rowCnt'] != 1:
                        print(' row %s contains the header' % fileInfo['rowCnt'])
                        errCnt += 1
                    continue

                #--return a good row
                else:
                    rowData = dict(zip(fileInfo['header'], [str(x).strip() for x in row]))

            else: #--if not just return what should be the header row
                fileInfo['skipCnt'] += 1
                rowData = [str(x).strip() for x in row]

        else:
            print(' row %s is blank' % fileInfo['rowCnt'])
            fileInfo['skipCnt'] += 1
            continue

    return fileInfo, rowData

#----------------------------------------
def removeQuoteChar(s):
    if len(s)>2 and s[0] + s[-1] in ("''", '""'):
        return s[1:-1] 
    return s 

#----------------------------------------
def getValue(rowData, expression):
    try: rtnValue = expression % rowData
    except: 
        print('warning: could not map %s' % (expression,)) 
        rtnValue = ''
    return rtnValue
    
#----------------------------------------
def processFile():
    global shutDown
    
    #--read the mapping file
    if not os.path.exists(mappingFileName):
        print()
        print('%s does not exist' % mappingFileName)
        return -1
    
    try: mappingDoc = json.load(open(mappingFileName, 'r'))
    except ValueError as err:
        print()
        print('mapping file error: %s in %s' % (err, mappingFileName))
        return -1

    #--upper case value replacements
    if 'columnHeaders' in mappingDoc['input']:
        mappingDoc['input']['columnHeaders'] = [x.upper() for x in mappingDoc['input']['columnHeaders']]
    for ii in range(len(mappingDoc['search']['attributes'])):
        mappingDoc['search']['attributes'][ii]['mapping'] = mappingDoc['search']['attributes'][ii]['mapping'].upper().replace(')S', ')s')
    for ii in range(len(mappingDoc['output']['columns'])):
        mappingDoc['output']['columns'][ii]['value'] = mappingDoc['output']['columns'][ii]['value'].upper().replace(')S', ')s')

    #--build output headers
    recordDataRequested = False
    outputHeaders = []
    for ii in range(len(mappingDoc['output']['columns'])):
        columnName = mappingDoc['output']['columns'][ii]['name'].upper()
        mappingDoc['output']['columns'][ii]['name'] = columnName
        outputHeaders.append(columnName)
        if mappingDoc['output']['columns'][ii]['source'].upper() == 'RECORD':
            recordDataRequested = True
    mappingDoc['output']['outputHeaders'] = outputHeaders

    #--use minimal format unless record data requested
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
    if recordDataRequested:
        searchFlags = searchFlags | g2Engine.G2_ENTITY_INCLUDE_NO_FEATURES 
    else:
        searchFlags = searchFlags | g2Engine.G2_ENTITY_MINIMAL_FORMAT 

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
    #for ii in range(len(mappingDoc['search']['attributes'])):
    #    mappingDoc['search']['attributes'][ii]['value'] = mappingDoc['search']['attributes'][ii]['value'].upper().replace(')S', ')s')
    #for ii in range(len(mappingDoc['output']['columns'])):
    #    mappingDoc['output']['columns'][ii]['value'] = mappingDoc['output']['columns'][ii]['value'].upper().replace(')S', ')s')

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

    mappingDoc['search']['rowsSearched'] = 0
    mappingDoc['search']['rowsSkipped'] = 0
    mappingDoc['search']['mappedList'] = []
    mappingDoc['search']['unmappedList'] = []
    mappingDoc['search']['ignoredList'] = []
    mappingDoc['search']['statistics'] = {}

    #--ensure uniqueness of attributes, especially if using labels (usage types)
    errorCnt = 0
    labelAttrList = []
    for i1 in range(len(mappingDoc['search']['attributes'])):
        if mappingDoc['search']['attributes'][i1]['attribute'] == '<ignore>':
            if 'mapping' in mappingDoc['search']['attributes'][i1]:
                mappingDoc['search']['ignoredList'].append(mappingDoc['search']['attributes'][i1]['mapping'].replace('%(','').replace(')s',''))
            continue
        elif csv_functions.is_senzing_attribute(mappingDoc['search']['attributes'][i1]['attribute']):
            mappingDoc['search']['mappedList'].append(mappingDoc['search']['attributes'][i1]['attribute'])
        else:
            mappingDoc['search']['unmappedList'].append(mappingDoc['search']['attributes'][i1]['attribute'])
        mappingDoc['search']['statistics'][mappingDoc['search']['attributes'][i1]['attribute']] = 0

        if 'label' in mappingDoc['search']['attributes'][i1]:
            mappingDoc['search']['attributes'][i1]['label_attribute'] = mappingDoc['search']['attributes'][i1]['label'].replace('_', '-') + '_'
        else:
            mappingDoc['search']['attributes'][i1]['label_attribute'] = ''
        mappingDoc['search']['attributes'][i1]['label_attribute'] += mappingDoc['search']['attributes'][i1]['attribute']
        if mappingDoc['search']['attributes'][i1]['label_attribute'] in labelAttrList:
            errorCnt += 1
            print('attribute %s (%s) is duplicated for output %s!' % (i1, mappingDoc['search']['attributes'][i1]['label_attribute'], i))
        else:
            labelAttrList.append(mappingDoc['search']['attributes'][i1]['label_attribute'])

    if errorCnt:
        return -1

    #--override mapping document with parameters
    if inputFileName or 'inputFileName' not in mappingDoc['input']:
        mappingDoc['input']['inputFileName'] = inputFileName
    #if fieldDelimiter or 'fieldDelimiter' not in mappingDoc['input']:
    #    mappingDoc['input']['fieldDelimiter'] = fieldDelimiter
    #if fileEncoding or 'fileEncoding' not in mappingDoc['input']:
    #    mappingDoc['input']['fileEncoding'] = fileEncoding
    if 'columnHeaders' not in mappingDoc['input']:
        mappingDoc['input']['columnHeaders'] = []

    #--get the input file
    if not mappingDoc['input']['inputFileName']:
        print('')
        print('no input file supplied')
        return 1
    fileList = glob.glob(mappingDoc['input']['inputFileName'])
    if len(fileList) == 0:
        print('')
        print('%s not found' % inputFileName)
        return 1

    #--for each input file
    totalRowCnt = 0
    for fileName in fileList:
        print('')
        print('Processing %s ...' % fileName)
        currentFile = {}
        currentFile['name'] = fileName
        currentFile['rowCnt'] = 0
        currentFile['skipCnt'] = 0

        #--open the file
        if 'fileEncoding' in mappingDoc['input'] and mappingDoc['input']['fileEncoding']:
            currentFile['fileEncoding'] = mappingDoc['input']['fileEncoding']
            currentFile['handle'] = open(fileName, 'r', encoding=mappingDoc['input']['fileEncoding'])
        else:
            currentFile['handle'] = open(fileName, 'r')

        #--set the dialect
        currentFile['fieldDelimiter'] = mappingDoc['input']['fieldDelimiter']
        if not mappingDoc['input']['fieldDelimiter']:
            currentFile['csvDialect'] = csv.Sniffer().sniff(currentFile['handle'].readline(), delimiters='|,\t')
            currentFile['handle'].seek(0)
            currentFile['fieldDelimiter'] = currentFile['csvDialect'].delimiter
            mappingDoc['input']['fieldDelimiter'] = currentFile['csvDialect'].delimiter
        elif mappingDoc['input']['fieldDelimiter'].lower() in ('csv', 'comma', ','):
            currentFile['csvDialect'] = 'excel'
        elif mappingDoc['input']['fieldDelimiter'].lower() in ('tab', 'tsv', '\t'):
            currentFile['csvDialect'] = 'excel-tab'
        elif mappingDoc['input']['fieldDelimiter'].lower() in ('pipe', '|'):
            csv.register_dialect('pipe', delimiter = '|', quotechar = '"')
            currentFile['csvDialect'] = 'pipe'
        elif len(mappingDoc['input']['fieldDelimiter']) == 1:
            csv.register_dialect('other', delimiter = delimiter, quotechar = '"')
            currentFile['csvDialect'] = 'other'
        elif len(mappingDoc['input']['fieldDelimiter']) > 1:
            currentFile['csvDialect'] = 'multi'
        else:
            currentFile['csvDialect'] = 'excel'

        #--set the reader (csv cannot be used for multi-char delimiters)
        if currentFile['csvDialect'] != 'multi':
            currentFile['reader'] = csv.reader(currentFile['handle'], dialect=currentFile['csvDialect'])
        else:
            currentFile['reader'] = currentFile['handle']

        #--get the current file header row and use it if not one already
        currentFile, currentHeaders = getNextRow(currentFile)
        if not mappingDoc['input']['columnHeaders']:
            mappingDoc['input']['columnHeaders'] = [x.upper() for x in currentHeaders]
        currentFile['header'] = mappingDoc['input']['columnHeaders']

        while True:
            currentFile, rowData = getNextRow(currentFile)
            if not rowData or shutDown:
                break

            totalRowCnt += 1
            rowData['ROW_ID'] = totalRowCnt

            #--clean garbage values
            for key in rowData:
                rowData[key] = csv_functions.clean_value(key, rowData[key])

            #--perform calculations
            mappingErrors = 0
            if 'calculations' in mappingDoc:
                for calcDict in mappingDoc['calculations']:
                    try: newValue = eval(list(calcDict.values())[0])
                    except Exception as err: 
                        print('  error: %s [%s]' % (list(calcDict.keys())[0], err)) 
                        mappingErrors += 1
                    else:
                        if type(newValue) == list:
                            for newItem in newValue:
                                rowData.update(newItem)
                        else:
                            rowData[list(calcDict.keys())[0]] = newValue

            if debugOn:
                print(json.dumps(rowData, indent=4))
                pause()

            if 'filter' in mappingDoc['search']:
                try: skipRow = eval(mappingDoc['search']['filter'])
                except Exception as err: 
                    skipRow = False
                    print(' filter error: %s [%s]' % (mappingDoc['search']['filter'], err))
                if skipRow:
                    mappingDoc['search']['rowsSkipped'] += 1
                    continue

            rootValues = {}
            subListValues = {}
            for attrDict in mappingDoc['search']['attributes']:
                if attrDict['attribute'] == '<ignore>':
                    continue

                attrValue = getValue(rowData, attrDict['mapping'])
                if attrValue:
                    mappingDoc['search']['statistics'][attrDict['attribute']] += 1
                    if 'subList' in attrDict:
                        if attrDict['subList'] not in subListValues:
                            subListValues[attrDict['subList']] = {}
                        subListValues[attrDict['subList']][attrDict['label_attribute']] = attrValue
                    else:
                        rootValues[attrDict['label_attribute']] = attrValue

            #--create the search json
            jsonData = {}
            for subList in subListValues:
                jsonData[subList] = [subListValues[subList]]
            jsonData.update(rootValues)
            if debugOn:
                print(json.dumps(jsonData, indent=4))
                pause()
            jsonStr = json.dumps(jsonData)

            #--empty searchResult = '{"SEARCH_RESPONSE": {"RESOLVED_ENTITIES": []}}'???
            try: 
                response = bytearray()
                retcode = g2Engine.searchByAttributesV2(jsonStr, searchFlags, response)
                response = response.decode() if response else ''
                #if len(response) > 500:
                #    print(json.dumps(json.loads(response), indent=4))
                #    pause()
            except G2ModuleException as err:
                print('')
                print(err)
                print('')
                shutDown = True
                break
            jsonResponse = json.loads(response)

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
                    for scoreRecord in resolvedEntity['MATCH_INFO']['FEATURE_SCORES'][featureCode]:
                        matchingScore= scoreRecord[scoreCode]
                        matchingValue = scoreRecord['CANDIDATE_FEAT']
                        scoreData.append('%s|%s|%s|%s' % (featureCode, scoreCode, matchingScore, matchingValue))
                        if featureCode not in bestScores:
                            bestScores[featureCode] = {}
                            bestScores[featureCode]['score'] = 0
                            bestScores[featureCode]['value'] = 'n/a'
                        if matchingScore > bestScores[featureCode]['score']:
                            bestScores[featureCode]['score'] = matchingScore
                            bestScores[featureCode]['value'] = matchingValue

                if debugOn: 
                    print(json.dumps(bestScores, indent=4))


                #--perform scoring (use stored match_score if not overridden in the mapping document)
                if 'scoring' not in mappingDoc:
                    matchScore = str(((5-resolvedEntity['MATCH_INFO']['MATCH_LEVEL']) * 100) + int(resolvedEntity['MATCH_INFO']['MATCH_SCORE'])) + '-' + str(1000+bestScores['NAME']['score'])[-3:]
                else:
                    matchScore = 0
                    for featureCode in bestScores:
                        if featureCode in mappingDoc['scoring']:
                            if debugOn: 
                                print(featureCode, mappingDoc['scoring'][featureCode])
                            if bestScores[featureCode]['score'] >= mappingDoc['scoring'][featureCode]['threshold']:
                                matchScore += int(round(bestScores[featureCode]['score'] * (mappingDoc['scoring'][featureCode]['+weight'] / 100),0))
                            elif '-weight' in mappingDoc['scoring'][featureCode]:
                                matchScore += -mappingDoc['scoring'][featureCode]['-weight'] #--actual score does not matter if below the threshold

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
                matchedEntity['MATCH_SCORE'] = matchScore
                matchedEntity['NAME_SCORE'] = bestScores['NAME']['score']
                matchedEntity['SCORE_DATA'] = '\n'.join(sorted(map(str, scoreData)))

                if debugOn:
                    print(json.dumps(matchedEntity, indent=4))
                    pause()

                matchedEntity['RECORDS'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['RECORDS']

                #--check the output filters
                filteredOut = False
                if matchLevel > mappingDoc['output']['matchLevelFilter']:
                    filteredOut = True
                    if debugOn:
                        print(' ** did not meet matchLevelFilter **')
                if bestScores['NAME']['score'] < mappingDoc['output']['nameScoreFilter']:
                    filteredOut = True
                    if debugOn:
                        print(' ** did not meet nameScoreFilter **')
                if mappingDoc['output']['dataSourceFilter'] and mappingDoc['output']['dataSourceFilter'] not in dataSources:
                    filteredOut = True
                    if debugOn:
                        print(' ** did not meet dataSourceFiler **')
                if not filteredOut:
                    matchList.append(matchedEntity)

            #--set the no match condition
            if len(matchList) == 0:
            #    if requiredFieldsMissing:
            #        rowsSkipped += 1
            #    else:
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
                if debugOn:
                    print(' ** no matches found **')
            else:
                rowsMatched += 1
                
            #----------------------------------
            #--create the output rows
            matchNumber = 0
            for matchedEntity in sorted(matchList, key=lambda x: x['MATCH_SCORE'], reverse=True):
                matchNumber += 1
                matchedEntity['MATCH_NUMBER'] = matchNumber if matchedEntity['ENTITY_ID'] != 0 else 0

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
                #uppercasedJsonData = False
                rowValues = []
                for columnDict in mappingDoc['output']['columns']:
                    columnValue = ''
                    try: 
                        if columnDict['source'].upper() == 'CSV':
                            columnValue = columnDict['value'] % rowData
                        elif columnDict['source'].upper() == 'API':
                            columnValue = columnDict['value'] % matchedEntity
                    except: 
                        print('warning: could not find %s in %s' % (columnDict['value'],columnDict['source'].upper())) 

                    #--comes from the records
                    if columnDict['source'].upper() == 'RECORD':
                        #if not uppercasedJsonData:
                        #    record['JSON_DATA'] = dictKeysUpper(record['JSON_DATA'])
                        #    uppercasedJsonData = True
                        columnValues = []
                        for record in matchedEntity['RECORDS']:
                            if columnDict['value'].upper().endswith('_DATA'):
                                for item in record[columnDict['value'].upper()]:
                                    columnValues.append(item)
                            else:
                                try: thisValue = columnDict['value'] % record['JSON_DATA']
                                except: pass
                                else:
                                    if thisValue and thisValue not in columnValues:
                                        columnValues.append(thisValue)
                        if columnValues:
                            columnValue = '\n'.join(sorted(map(str, columnValues)))

                    #if debugOn:
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
                        
            if totalRowCnt % sqlCommitSize == 0:
                now = datetime.now().strftime('%I:%M%p').lower()
                elapsedMins = round((time.time() - procStartTime) / 60, 1)
                eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
                batchStartTime = time.time()
                print(' %s rows searched at %s, %s per second ... %s rows matched, %s resolved matches, %s possible matches, %s possibly related, %s name only' % (totalRowCnt, now, eps, rowsMatched, resolvedMatches, possibleMatches, possiblyRelateds, nameOnlyMatches))

            #--break conditions
            if shutDown:
                break
            elif 'ERROR' in currentFile:
                break

        currentFile['handle'].close()
        if shutDown:
            break

    #--all files completed
    now = datetime.now().strftime('%I:%M%p').lower()
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    eps = int(float(sqlCommitSize) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))
    batchStartTime = time.time()
    print(' %s rows searched at %s, %s per second ... %s rows matched, %s resolved matches, %s possible matches, %s possibly related, %s name only' % (totalRowCnt, now, eps, rowsMatched, resolvedMatches, possibleMatches, possiblyRelateds, nameOnlyMatches))
    print(json.dumps(scoreCounts, indent = 4))    

    #--close all inputs and outputs
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
    
    senzingConfigFile = os.getenv('SENZING_CONFIG_FILE') if os.getenv('SENZING_CONFIG_FILE', None) else appPath + os.path.sep + 'G2Module.ini'
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--senzingConfigFile', dest='senzingConfigFile', default=senzingConfigFile, help='name of the g2.ini file, defaults to %s' % senzingConfigFile)
    parser.add_argument('-m', '--mappingFileName', dest='mappingFileName', help='the name of a mapping file')
    parser.add_argument('-i', '--inputFileName', dest='inputFileName', help='the name of a csv input file')
    parser.add_argument('-d', '--delimiterChar', dest='delimiterChar', help='delimiter character')
    parser.add_argument('-e', '--fileEncoding', dest='fileEncoding', help='file encoding')
    parser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='logFileName', help='optional statistics filename (json format).')
    parser.add_argument('-D', '--debugOn', dest='debugOn', action='store_true', default=False, help='run in debug mode')
    args = parser.parse_args()
    senzingConfigFile = args.senzingConfigFile
    mappingFileName = args.mappingFileName
    inputFileName = args.inputFileName
    delimiterChar = args.delimiterChar
    fileEncoding = args.fileEncoding
    outputFileName = args.outputFileName
    logFileName = args.logFileName
    debugOn = args.debugOn

    #--get parameters from ini file
    if not os.path.exists(senzingConfigFile):
        print('')
        print('Senzing config file: %s not found!' % senzingConfigFile)
        print('')
        sys.exit(1)
    iniParser = configparser.ConfigParser()
    iniParser.read(senzingConfigFile)

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
        iniParams = iniParamCreator.getJsonINIParams(senzingConfigFile)
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
            g2Engine.init('csv_search_viewer', senzingConfigFile, False)
        else:
            iniParamCreator = G2IniParams()
            iniParams = iniParamCreator.getJsonINIParams(senzingConfigFile)
            g2Engine.initV2('csv_search', iniParams, False)
    except G2Exception as err:
        print('')
        print('Could not initialize the G2 Engine')
        print(str(err))
        print('')
        sys.exit(1)

    #--load the csv functions if available
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
