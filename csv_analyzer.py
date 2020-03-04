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
    csv_data = None
    while not csv_data:

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
                    csv_data = dict(zip(fileInfo['header'], [str(x).strip() for x in row]))

            else: #--if not just return what should be the header row
                fileInfo['skipCnt'] += 1
                csv_data = [str(x).strip() for x in row]

        else:
            print(' row %s is blank' % fileInfo['rowCnt'])
            fileInfo['skipCnt'] += 1
            continue

    return fileInfo, csv_data

#----------------------------------------
def removeQuoteChar(s):
    if len(s)>2 and s[0] + s[-1] in ("''", '""'):
        return s[1:-1] 
    return s 

#----------------------------------------
def analyzeFile():
    """ analyze a csv file """
    global shutDown
    statPack = {}
    totalRowCnt = 0

    #--get parameters from a mapping file
    mappingDoc = {'input': {}}
    if mappingFileName and os.path.exists(mappingFileName): 
        try: mappingDoc = json.load(open(mappingFileName, 'r'))
        except ValueError as err:
            print('')
            print('mapping file error: %s in %s' % (err, mappingFileName))
            return 1

    #--override mapping document with parameters
    if inputFileName or 'inputFileName' not in mappingDoc['input']:
        mappingDoc['input']['inputFileName'] = inputFileName
    if fieldDelimiter or 'fieldDelimiter' not in mappingDoc['input']:
        mappingDoc['input']['fieldDelimiter'] = fieldDelimiter
    if fileEncoding or 'fileEncoding' not in mappingDoc['input']:
        mappingDoc['input']['fileEncoding'] = fileEncoding
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
        print('Analyzing %s ...' % fileName)
        currentFile = {}
        currentFile['name'] = fileName
        currentFile['rowCnt'] = 0
        currentFile['skipCnt'] = 0

        #--open the file
        if mappingDoc['input']['fileEncoding']:
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
            mappingDoc['input']['columnHeaders'] = [str(x).replace(' ', '_') for x in currentHeaders]
        currentFile['header'] = mappingDoc['input']['columnHeaders']

        #--initialize the statpack first time through
        if not statPack:
            colNum = 0
            errCnt = 0
            for columnName in mappingDoc['input']['columnHeaders']:
                colNum += 1
                if not columnName:
                    print(' column %s header is blank!' % colNum)
                    errCnt += 1
                if columnName not in statPack: 
                    statPack[columnName] = {'null': 0}
            if errCnt:
                print()
                print('Row 1 does not contain a valid column header!')
                currentFile['handle'].close
                return 1

        #--process the rows in the input file
        currentFile, rowData = getNextRow(currentFile)
        while rowData:
            totalRowCnt += 1

            #--for each column
            for columnName in currentFile['header']:

                columnValue = str(rowData[columnName]).strip()
                if not columnValue or columnValue.upper() in ('NONE', 'NULL', '\\N'):
                    statPack[columnName]['null'] += 1
                    continue
                
                if columnValue in statPack[columnName]:
                    statPack[columnName][columnValue] += 1
                else:
                    statPack[columnName][columnValue] = 1
                
            currentFile, rowData = getNextRow(currentFile)

            #--break conditions
            if shutDown:
                break
            elif 'ERROR' in currentFile:
                break

            if currentFile['rowCnt'] % 10000 == 0:
                print(' %s records processed' % currentFile['rowCnt'])

        currentFile['handle'].close()
        if shutDown:
            break
        else:
            print(' %s records processed, complete!' % currentFile['rowCnt'])
    
    #--export the analysis    
    if outputFileName:
        print()
        print('Writing results to %s ...' % outputFileName)
        try: outputFileHandle = open(outputFileName, 'w', newline='')
        except IOError as err: 
            print()
            print('Could not write to %s' % outputFileName)
            return 1
        outputFileWriter = csv.writer(outputFileHandle, dialect=csv.excel, quoting=csv.QUOTE_ALL)
            
    bestRecordID = '<remove-or-supply>'
    possibleMappings = []    
    columnHeaders = "columnName,recordCount,percentPopulated,uniqueCount,uniquePercent,topValue1,topValue2,topValue3,topValue4,topValue5"
    if outputFileName:
        outputFileWriter.writerow(columnHeaders.split(','))
    else:
        print(columnHeaders)

    for columnName in mappingDoc['input']['columnHeaders']:
        recordCount = totalRowCnt - statPack[columnName]['null']
        percentPopulated = round(recordCount / totalRowCnt * 100, 2)
        uniqueCount = len(statPack[columnName]) - 1
        uniquePercent = round(uniqueCount / recordCount * 100, 2) if recordCount else 0

        #--first 100% unique field is recordID
        if uniqueCount == totalRowCnt and not bestRecordID.startswith('%'):
            bestRecordID = '%(' + columnName + ')s'

        topValue = []
        for value in sorted(statPack[columnName].items(), key=lambda x: x[1], reverse=True):
            if value[0] != 'null':
                topValue.append('%s (%s)' % value)
                if len(topValue) == 5:
                    break
        while len(topValue) < 5:
            topValue.append('')

        #--create possible mapping
        if recordCount:
            columnMapping = {}
            columnMapping['attribute'] = '<ignore>'
            columnMapping['mapping'] = '%(' + columnName + ')s'
            columnMapping['statistics'] = {}
            columnMapping['statistics']['columnName'] = columnName
            columnMapping['statistics']['populated%'] = percentPopulated
            columnMapping['statistics']['unique%'] = uniquePercent
            columnMapping['statistics']['top5values'] = [item for item in topValue if item]
            possibleMappings.append(columnMapping)

        rowData = (columnName, recordCount, percentPopulated, uniqueCount, uniquePercent, topValue[0], topValue[1], topValue[2], topValue[3], topValue[4])
        if outputFileName:
            outputFileWriter.writerow(rowData)
        else:
            print('"%s", %s, %s, %s, %s, "%s", "%s", "%s", "%s", "%s"' % rowData)
    
    #--close the output file
    if outputFileName:
        outputFileHandle.close()
    
    #--create or update the mapping file if provided
    if mappingFileName:
        if not mappingDoc['input']['fieldDelimiter']:
            del mappingDoc['input']['fieldDelimiter']
        if not mappingDoc['input']['fileEncoding']:
            del mappingDoc['input']['fileEncoding']
        if 'outputs' not in mappingDoc:
            mappingDoc['outputs'] = []
            outputDoc = {}
            outputDoc['data_source'] = '<supply>'
            outputDoc['entity_type'] = '<supply>'
            outputDoc['record_id'] = bestRecordID
            outputDoc['attributes'] = possibleMappings
            mappingDoc['outputs'].append(outputDoc)
        try:
            with open(mappingFileName, 'w') as f:
                json.dump(mappingDoc, f, indent=4)
        except Exception as e:
            print()
            print('Could not write to %s' % mappingFileName)
            print(e)

    return shutDown

#----------------------------------------
if __name__ == "__main__":
    procStartTime = time.time()
    shutDown = False   
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inputFileName', dest='inputFileName', help='the name of a csv input file')
    parser.add_argument('-d', '--fieldDelimiter', dest='fieldDelimiter', help='delimiter character')
    parser.add_argument('-e', '--fileEncoding', dest='fileEncoding', help='file encoding')
    parser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    parser.add_argument('-m', '--mappingFileName', dest='mappingFileName', help='optional name of a mapping file to generate')
    args = parser.parse_args()
    mappingFileName = args.mappingFileName
    inputFileName = args.inputFileName
    fieldDelimiter = args.fieldDelimiter
    fileEncoding = args.fileEncoding
    outputFileName = args.outputFileName
    
    if not inputFileName:
        print('An input file name is required')
        sys.exit(1)
        
    if mappingFileName and os.path.exists(mappingFileName):
        print()
        response = input('Mapping file already exists!!, overwrite it? (Y/N) ')
        if response[0:1].upper() != 'Y':
            print('Process aborted!')
            sys.exit(1)
        else:
            mappingFileBackup = mappingFileName + '.bk'
            if os.path.exists(mappingFileBackup):
                os.remove(mappingFileBackup)
            os.rename(mappingFileName, mappingFileBackup)

    result = analyzeFile()
    
    print('')
    if result != 0:
        print('process aborted!')
    else:
        print('process completed!')
    print('')
    
    sys.exit(result)
