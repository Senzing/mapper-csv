#! /usr/bin/env python3
import os
import sys
import argparse
import configparser
import signal
import time
from datetime import datetime, timedelta
import json
import csv
import glob
from importlib.machinery import SourceFileLoader
from csv_functions import csv_functions

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    """ pause for debug purposes """
    global shutDown
    try: response = input(question)
    except KeyboardInterrupt:
        response = None
        shutDown = True
    return response

#----------------------------------------
def signal_handler(signal, frame):
    global shutDown
    print('USER INTERUPT! Shutting down ... (please wait)')
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
    try:
        if expression in rowData:
            #print('here')
            rtnValue = rowData[expression]
        else:  
            rtnValue = expression % rowData
    except: 
        print('warning: could not map %s' % (expression,))
        rtnValue = ''
    return rtnValue

#----------------------------------------
def processFile():
    global shutDown

    #--check if mapped via python class first
    pythonMapperClass = None
    if pythonModuleFile: 
        if not os.path.exists(pythonModuleFile):
            print('%s does not exist' % mappingFileName)
            return -1
        pythonMapper = SourceFileLoader(pythonModuleFile, pythonModuleFile).load_module()
        pythonMapperClass = pythonMapper.mapper()

        mappingDoc = {}
        mappingDoc['input'] = {}
        #mappingDoc['fileEncoding'] = fileEncoding

    #--read the mapping file
    else:
        if not os.path.exists(mappingFileName):
            print('%s does not exist' % mappingFileName)
            return -1

        try: mappingDoc = json.load(open(mappingFileName, 'r'))
        except ValueError as err:
            print()
            print('mapping file error: %s in %s' % (err, mappingFileName))
            return -1

    #--command line overides
    if fileEncoding:
        mappingDoc['input']['fileEncoding'] = fileEncoding
    if fieldDelimiter:
        mappingDoc['input']['fieldDelimiter'] = fieldDelimiter

    #--initialize stats for python class mapper
    if 'outputs' not in mappingDoc:
        mappingDoc['outputs'] = []
    else:        

        #--validate all outputs
        errorCnt = 0
        for i in range(len(mappingDoc['outputs'])):
            if 'enabled' in mappingDoc['outputs'][i] and mappingDoc['outputs'][i]['enabled'].upper().startswith("N"):
                continue

            mappingDoc['outputs'][i]['rowsWritten'] = 0
            mappingDoc['outputs'][i]['rowsSkipped'] = 0
            mappingDoc['outputs'][i]['ignoredList'] = []
            mappingDoc['outputs'][i]['statistics'] = {}

            #--ensure uniqueness of attributes, especially if using labels (usage types)
            aggregate = False 
            labelAttrList = []
            for i1 in range(len(mappingDoc['outputs'][i]['attributes'])):
                if mappingDoc['outputs'][i]['attributes'][i1]['attribute'] == '<ignore>':
                    if 'mapping' in mappingDoc['outputs'][i]['attributes'][i1]:
                        mappingDoc['outputs'][i]['ignoredList'].append(mappingDoc['outputs'][i]['attributes'][i1]['mapping'].replace('%(','').replace(')s',''))
                    continue
                mappingDoc['outputs'][i]['statistics'][mappingDoc['outputs'][i]['attributes'][i1]['attribute']] = 0
        
                if 'label' in mappingDoc['outputs'][i]['attributes'][i1]:
                    mappingDoc['outputs'][i]['attributes'][i1]['label_attribute'] = mappingDoc['outputs'][i]['attributes'][i1]['label'].replace('_', '-') + '_'
                else:
                    mappingDoc['outputs'][i]['attributes'][i1]['label_attribute'] = ''
                mappingDoc['outputs'][i]['attributes'][i1]['label_attribute'] += mappingDoc['outputs'][i]['attributes'][i1]['attribute']
                if mappingDoc['outputs'][i]['attributes'][i1]['label_attribute'] in labelAttrList:
                    errorCnt += 1
                    print('attribute %s (%s) is duplicated for output %s!' % (i1, mappingDoc['outputs'][i]['attributes'][i1]['label_attribute'], i))
                else:
                    labelAttrList.append(mappingDoc['outputs'][i]['attributes'][i1]['label_attribute'])

                if 'subList' in mappingDoc['outputs'][i]['attributes'][i1]:
                    aggregate = True 

            mappingDoc['outputs'][i]['aggregate'] = aggregate
        if errorCnt:
            return -1

    #--initialize aggregated record array
    totalRowCnt = 0
    aggregatedRecords = {}

    #--open output file
    try: outputFileHandle = open(outputFileName, 'w')
    except IOError as err: 
        print('')
        print('Could not write to %s \n%s' % (outputFileName, err))
        return -1

    #--override mapping document with parameters
    if inputFileName or 'inputFileName' not in mappingDoc['input']:
        mappingDoc['input']['inputFileName'] = inputFileName
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
        if 'fieldDelimiter' in mappingDoc['input']:
            currentFile['fieldDelimiter'] = mappingDoc['input']['fieldDelimiter']
        else:
            sniffer = csv.Sniffer().sniff(currentFile['handle'].readline(), delimiters='|,\t')
            currentFile['handle'].seek(0)
            currentFile['fieldDelimiter'] = sniffer.delimiter
            mappingDoc['input']['fieldDelimiter'] = sniffer.delimiter
        if mappingDoc['input']['fieldDelimiter'].lower() in ('csv', 'comma', ','):
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
            mappingDoc['input']['columnHeaders'] = [str(x).replace(' ', '_') for x in currentHeaders]
        currentFile['header'] = mappingDoc['input']['columnHeaders']

        while True:
            currentFile, rowData = getNextRow(currentFile)
            if not rowData:
                break

            totalRowCnt += 1
            rowData['ROW_ID'] = totalRowCnt

            #--clean garbage values
            for key in rowData:
                rowData[key] = csv_functions.clean_value(key, rowData[key])

            #--python mapper statistics
            if pythonMapperClass:
                mappedData = pythonMapperClass.map(rowData)
                if not mappedData:
                    currentFile['skipCnt'] += 1
                    continue

                jsonList = []
                i = 0 
                for jsonData in mappedData if type(mappedData) == list else [mappedData]:
                    if i == len(mappingDoc['outputs']):
                        outputDict = {}
                        outputDict['rowsWritten'] = 0
                        outputDict['rowsSkipped'] = 0
                        outputDict['ignoredList'] = []
                        outputDict['statistics'] = {}
                        mappingDoc['outputs'].append(outputDict)
                    removeAttrList = []
                    for attribute in jsonData:
                        if not jsonData[attribute]:
                            removeAttrList.append(attribute)
                        else:
                            if type(jsonData[attribute]) != list: #--to help capture stats in sub-lists
                                subList = [{attribute: jsonData[attribute]}]
                            else:
                                subList = jsonData[attribute]
                            for record in subList:
                                for attribute in record:
                                    if attribute not in mappingDoc['outputs'][i]['statistics']:
                                        mappingDoc['outputs'][i]['statistics'][attribute] = 1
                                    else:
                                        mappingDoc['outputs'][i]['statistics'][attribute] += 1
                    for attribute in removeAttrList:
                        del jsonData[attribute]
                    if jsonData:
                        jsonList.append(jsonData)
                        outputDict['rowsWritten'] += 1
                    i = i + 1

            else:

                #--perform calculations
                mappingErrors = 0
                if 'calculations' in mappingDoc:
                    for calcDict in mappingDoc['calculations']:
                        newAttribute = list(calcDict.keys())[0]
                        newExpression = list(calcDict.values())[0]
                        try: newValue = eval(newExpression)
                        except Exception as err: 
                            print('  error: %s [%s]' % (newAttribute, err)) 
                            mappingErrors += 1
                        else:
                            if newAttribute == '<list>':
                                if type(newValue) == list:
                                    for newItem in newValue:
                                        rowData.update(newItem)
                                else:                                
                                    print('  error: %s [%s]' % (newAttribute, 'expression did not return a list!')) 
                                    mappingErrors += 1
                            else:
                                rowData[newAttribute] = newValue

                #--process the record for each output
                jsonList = []
                for i in range(len(mappingDoc['outputs'])):
                    if 'enabled' in mappingDoc['outputs'][i] and mappingDoc['outputs'][i]['enabled'].upper().startswith('N'):
                        continue

                    if 'filter' in mappingDoc['outputs'][i]:
                        try: skipRow = eval(mappingDoc['outputs'][i]['filter'])
                        except Exception as err: 
                            skipRow = False
                            print(' filter error: %s [%s]' % (mappingDoc['outputs'][i]['filter'], err))
                        if skipRow:
                            mappingDoc['outputs'][i]['rowsSkipped'] += 1
                            continue

                    dataSource = getValue(rowData, mappingDoc['outputs'][i]['data_source'])
                    if 'record_type' in mappingDoc['outputs'][i]:
                        recordType = getValue(rowData, mappingDoc['outputs'][i]['record_type'])
                    else:
                        recordType = dataSource

                    entityKey = None
                    recordID = None
                    uniqueKey = None
                    if 'entity_key' in mappingDoc['outputs'][i]:
                        entityKey = getValue(rowData, mappingDoc['outputs'][i]['entity_key'])
                        uniqueKey = dataSource + '|' + entityKey
                    elif 'record_id' in mappingDoc['outputs'][i]:
                        recordID = getValue(rowData, mappingDoc['outputs'][i]['record_id'])
                        uniqueKey = dataSource + '|' + recordID

                    rootValues = {}
                    subListValues = {}
                    for attrDict in mappingDoc['outputs'][i]['attributes']:
                        if attrDict['attribute'] == '<ignore>':
                            continue

                        attrValue = getValue(rowData, attrDict['mapping'])
                        if attrValue:
                            mappingDoc['outputs'][i]['statistics'][attrDict['attribute']] += 1
                            if 'subList' in attrDict:
                                if attrDict['subList'] not in subListValues:
                                    subListValues[attrDict['subList']] = {}
                                subListValues[attrDict['subList']][attrDict['label_attribute']] = attrValue
                            else:
                                rootValues[attrDict['label_attribute']] = attrValue

                    #--complete the json record
                    jsonData = {}
                    for subList in subListValues:
                        jsonData[subList] = [subListValues[subList]]
                    jsonData['DATA_SOURCE'] = dataSource
                    jsonData['RECORD_TYPE'] = recordType
                    if entityKey:
                        jsonData['ENTITY_KEY'] = entityKey
                    elif recordID:
                        jsonData['RECORD_ID'] = recordID
                    jsonData.update(rootValues)

                    #--just output if not aggregating
                    if not mappingDoc['outputs'][i]['aggregate']:
                        jsonList.append(jsonData)
                        mappingDoc['outputs'][i]['rowsWritten'] += 1
                    else:
                        if uniqueKey not in aggregatedRecords:
                            mappingDoc['outputs'][i]['rowsWritten'] += 1
                            aggregatedRecords[uniqueKey] = jsonData
                        else:
                            #--update root attributes
                            for attribute in jsonData:
                                if type(jsonData[attribute]) != list:
                                    #--append missing
                                    if attribute not in aggregatedRecords[uniqueKey]:
                                        aggregatedRecords[uniqueKey][attribute] = jsonData[attribute]
                                    else:
                                        if jsonData[attribute] != aggregatedRecords[uniqueKey][attribute]:
                                            print(' %s update ignored ... [%s] vs [%s]' % (attribute, jsonData[attribute], aggregatedRecords[uniqueKey][attribute]))
                                            #--do not update for now... just not sure how!

                            #--aggregate distinct subLists
                            for subList in subListValues:
                                subRecord = subListValues[subList]
                                if subList not in aggregatedRecords[uniqueKey]:
                                    aggregatedRecords[uniqueKey][subList] = []

                                if subRecord not in aggregatedRecords[uniqueKey][subList]:
                                    aggregatedRecords[uniqueKey][subList].append(subRecord)
                            jsonData = aggregatedRecords[uniqueKey]

            for jsonData in jsonList:
                try: outputFileHandle.write(json.dumps(jsonData) + '\n')
                except IOError as err: 
                    print('')
                    print('Could no longer write to %s \n%s' % (outputFileName, err))
                    shutDown = True
                    break
                #mappingDoc['outputs'][i]['rowsWritten'] += 1
                if debugOn:
                    print(json.dumps(jsonData, indent=4))
                    pause()

            #--break conditions
            if shutDown:
                break
            elif 'ERROR' in currentFile:
                break

            if currentFile['rowCnt'] % 10000 == 0:
                print(' %s rows processed, %s rows skipped' % (currentFile['rowCnt'], currentFile['skipCnt']))

        currentFile['handle'].close()
        if shutDown:
            break
        else:
            print(' %s rows processed, %s rows skipped, complete!' % (currentFile['rowCnt'], currentFile['skipCnt']))

    #-write aggregated records to file
    if aggregatedRecords:
        print('writing aggregated records to output file ...')
        if not shutDown:
            rowCnt = 0
            for uniqueKey in aggregatedRecords:
                try: outputFileHandle.write(json.dumps(aggregatedRecords[uniqueKey]) + '\n')
                except IOError as err: 
                    print('')
                    print('Could not longer write to %s \n%s' % (outputFileName, err))
                    print('')
                    shutDown = True
                    break
                rowCnt += 1
                if rowCnt % 10000 == 0:
                    print(' %s rows processed' % rowCnt)
            if not shutDown:
                print(' %s rows processed, complete!' % rowCnt)

    #--close all inputs and outputs
    outputFileHandle.close()

    for i in range(len(mappingDoc['outputs'])):
        print()
        print('OUTPUT #%s ...' % i)
        print('  %s rows written' % mappingDoc['outputs'][i]['rowsWritten'])
        print('  %s rows skipped' % mappingDoc['outputs'][i]['rowsSkipped'])
        print()
        print(' MAPPED ATTRIBUTES:') 
        mappingDoc['outputs'][i]['mappedList'] = []
        mappingDoc['outputs'][i]['unmappedList'] = []
        for attribute in mappingDoc['outputs'][i]['statistics']:
            if csv_functions.is_senzing_attribute(attribute):
                mappingDoc['outputs'][i]['mappedList'].append(attribute)
            else:
                mappingDoc['outputs'][i]['unmappedList'].append(attribute)


        for attribute in mappingDoc['outputs'][i]['mappedList']:
            percentPopulated = round(mappingDoc['outputs'][i]['statistics'][attribute] / mappingDoc['outputs'][i]['rowsWritten'] * 100, 2)
            print('  %s %10d %s %%' % (attribute.lower().ljust(30,'.'), mappingDoc['outputs'][i]['statistics'][attribute], percentPopulated))
        if mappingDoc['outputs'][i]['unmappedList']:
            print()
            print(' UNMAPPED ATTRIBUTES:') 
            for attribute in mappingDoc['outputs'][i]['unmappedList']:
                percentPopulated = round(mappingDoc['outputs'][i]['statistics'][attribute] / mappingDoc['outputs'][i]['rowsWritten'] * 100, 2)
                print('  %s %10d %s %%' % (attribute.lower().ljust(30,'.'), mappingDoc['outputs'][i]['statistics'][attribute], percentPopulated))
        if mappingDoc['outputs'][i]['ignoredList']:
            print()
            print(' COLUMNS IGNORED: \n  %s' % ', '.join([i.lower() for i in mappingDoc['outputs'][i]['ignoredList']]))

    if shutDown:
        return -1

    #for fileName in openFiles:
    #    openFiles[fileName].close()

    return 0

#----------------------------------------
if __name__ == '__main__':
    procStartTime = time.time()
    shutDown = False   
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mappingFileName', dest='mappingFileName', help='the name of a mapping file')
    parser.add_argument('-p', '--pythonModuleFile', dest='pythonModuleFile', help='optional name of a python module file to generate')
    parser.add_argument('-i', '--inputFileName', dest='inputFileName', help='the name of a csv input file')
    parser.add_argument('-d', '--fieldDelimiter', dest='fieldDelimiter', help='delimiter character')
    parser.add_argument('-e', '--fileEncoding', dest='fileEncoding', help='file encoding')
    parser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='logFileName', help='optional statistics filename (json format).')
    parser.add_argument('-D', '--debugOn', dest='debugOn', action='store_true', default=False, help='run in debug mode')
    args = parser.parse_args()
    mappingFileName = args.mappingFileName
    pythonModuleFile = args.pythonModuleFile
    inputFileName = args.inputFileName
    fieldDelimiter = args.fieldDelimiter
    fileEncoding = args.fileEncoding
    outputFileName = args.outputFileName
    logFileName = args.logFileName
    debugOn = args.debugOn

    #--validations
    if not mappingFileName and not pythonModuleFile:
        print('Either a -m mapping file or a -p python module must be specified')
        sys.exit(1)
    if not outputFileName:
        print('an output file must be specified with -o')
        sys.exit(1)

    csv_functions = csv_functions()
    if not csv_functions.initialized:
        sys.exit(1)

    result = processFile()

    if logFileName: 
        print('')
        with open(logFileName, 'w') as f:
            json.dump(csv_functions.statPack, f, indent=4, sort_keys = True)    
        print('Mapping stats written to %s' % logFileName)

    print('')
    if result != 0:
        print('process aborted!')
    else:
        print('process completed!')
    print('')
    
    sys.exit(result)
