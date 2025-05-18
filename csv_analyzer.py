#! /usr/bin/env python3
import os
import sys
import argparse
import configparser
import signal
import time
from datetime import datetime, date, timedelta
import json
import csv
import glob
import numpy as np

try:
    import pandas as pd
except: 
    pd = False

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
            print('\nShutdown due to too many errors')
            break
             
        try: 
            line = next(fileInfo['reader'])
        except StopIteration:
            print('done!')
            break
        except: 
            print(' row %s: %s' % (fileInfo['rowCnt'], sys.exc_info()[0]))
            fileInfo['skipCnt'] += 1
            errCnt += 1
            continue
        fileInfo['rowCnt'] += 1
        if line:

            if fileInfo['fieldDelimiter'] in ('JSON', 'PARQUET'):
                csv_data = json.loads(line) if fileInfo['fieldDelimiter'] == 'JSON' else line
                return fileInfo, csv_data

            #--csv reader will return a list (mult-char delimiter must be manually split)
            if type(line) == list:
                row = line
            else:
                row = [removeQuoteChar(x.strip()) for x in line.split(fileInfo['fieldDelimiter'])]

            #--turn into a dictionary if there is a header
            if 'columnHeaders' in fileInfo:

                #--column mismatch
                if len(row) != len(fileInfo['columnHeaders']):
                    print(' row %s has %s columns, expected %s' % (fileInfo['rowCnt'], len(row), len(fileInfo['columnHeaders'])))
                    fileInfo['skipCnt'] += 1
                    errCnt += 1
                    continue

                #--is it the header row
                elif str(row[0]).upper() == fileInfo['columnHeaders'][0].upper() and str(row[len(row)-1]).upper() == fileInfo['columnHeaders'][len(fileInfo['columnHeaders'])-1].upper():
                    fileInfo['skipCnt'] += 1
                    if fileInfo['rowCnt'] != 1:
                        print(' row %s contains the header' % fileInfo['rowCnt'])
                        errCnt += 1
                    continue

                #--return a good row
                else:
                    csv_data = dict(zip(fileInfo['columnHeaders'], [str(x).strip() for x in row]))

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
    statPack = {'orders':{}, 'counts': {}, 'values': {}}
    totalRowCnt = 0

    #--get parameters from a mapping file
    mappingDoc = {'input': {}}
    if mappingFileName and os.path.exists(mappingFileName): 
        try: 
            mappingDoc = json.load(open(mappingFileName, 'r'))
        except ValueError as err:
            print(f"mapping file error: {err} in {mappingFileName}")
            return 1

    #--override mapping document with parameters
    mappingDoc['input']['inputFileName'] = inputFileName if inputFileName else mappingDoc['input'].get('inputFileName')
    mappingDoc['input']['fieldDelimiter'] = fieldDelimiter if fieldDelimiter else mappingDoc['input'].get('fieldDelimiter', '')
    mappingDoc['input']['fileEncoding'] = fileEncoding if fileEncoding else mappingDoc['input'].get('fileEncoding')
    mappingDoc['input']['columnHeaders'] = mappingDoc['input'].get('columnHeaders', [])

    #--get the input file
    if not mappingDoc['input']['inputFileName']:
        print('\nNo input file supplied')
        return 1
    fileList = glob.glob(mappingDoc['input']['inputFileName'])
    if len(fileList) == 0:
        print(f"{inputFileName} not found")
        return 1

    #--need a test record for python module
    testRecord = None

    #--for each input file
    for fileName in fileList:
        print(f"\nAnalyzing {fileName} ...")
        currentFile = {}
        currentFile['name'] = fileName
        currentFile['rowCnt'] = 0
        currentFile['skipCnt'] = 0
        currentFile['fieldDelimiter'] = mappingDoc['input']['fieldDelimiter']

        #--open the file
        if currentFile['fieldDelimiter'].upper().startswith('PARQUET'):
            currentFile['fieldDelimiter'] = 'PARQUET'
            currentFile['handle'] = pd.read_parquet(fileName, engine='auto')
            currentFile['reader'] = iter(currentFile['handle'].to_dict(orient="records"))
        else:
            if mappingDoc['input']['fileEncoding']:
                currentFile['fileEncoding'] = mappingDoc['input']['fileEncoding']
                currentFile['handle'] = open(fileName, 'r', encoding=mappingDoc['input']['fileEncoding'])
            else:
                currentFile['handle'] = open(fileName, 'r')

            if currentFile['fieldDelimiter'] == 'JSON':
                currentFile['reader'] = currentFile['handle']
            else:
                if not currentFile['fieldDelimiter']:
                    sniffer = csv.Sniffer().sniff(currentFile['handle'].readline(), delimiters='|,\t')
                    currentFile['handle'].seek(0)
                    currentFile['fieldDelimiter'] = sniffer.delimiter
                    mappingDoc['input']['fieldDelimiter'] = sniffer.delimiter
                    print(f"sniffed [{currentFile['fieldDelimiter']}]")
                else:
                    print(f"[{currentFile['fieldDelimiter']}]")
                    input('wait')

                if currentFile['fieldDelimiter'].lower() in ('csv', 'comma', ','):
                    currentFile['csvDialect'] = 'excel'
                elif currentFile['fieldDelimiter'].lower() in ('tab', 'tsv', '\t'):
                    currentFile['csvDialect'] = 'excel-tab'
                elif currentFile['fieldDelimiter'].lower() in ('pipe', '|'):
                    csv.register_dialect('pipe', delimiter = '|', quotechar = '"')
                    currentFile['csvDialect'] = 'pipe'
                elif len(currentFile['fieldDelimiter']) == 1:
                    csv.register_dialect('other', delimiter = delimiter, quotechar = '"')
                    currentFile['csvDialect'] = 'other'
                elif len(currentFile['fieldDelimiter']) > 1:
                    currentFile['csvDialect'] = 'multi'
                else:
                    currentFile['csvDialect'] = 'excel'

                mappingDoc['input']['csvDialect'] = currentFile['csvDialect']

                #--set the reader (csv cannot be used for multi-char delimiters)
                if currentFile['csvDialect'] == 'multi':
                    currentFile['reader'] = currentFile['handle']
                else:
                    currentFile['reader'] = csv.reader(currentFile['handle'], dialect=currentFile['csvDialect'])

            #--get the current file header row and use it if not one already
            currentFile, currentHeaders = getNextRow(currentFile)
            if not mappingDoc['input']['columnHeaders']:
                mappingDoc['input']['columnHeaders'] = [str(x).replace(' ', '_') for x in currentHeaders]
        currentFile['columnHeaders'] = mappingDoc['input']['columnHeaders']

        #--process the rows in the input file
        currentFile, rowData = getNextRow(currentFile)

        # create schema first to preserve field order
        orders = {"root": 1000}
        if fileName == fileList[0] and rowData:
            for columnName in rowData:
                statPack['orders'][columnName] = orders["root"]
                statPack['counts'][columnName] = 0
                statPack['values'][columnName] = {}
                if is_iterable(rowData[columnName]) and type(rowData[columnName][0]) == dict:
                    if columnName not in orders:
                        orders[columnName] = orders["root"] + 1
                    for sub_row in rowData[columnName]:
                        for attr in sub_row:
                            combinedName = f"{columnName}.{attr}"
                            statPack['orders'][combinedName] = orders[columnName]
                            statPack['counts'][combinedName] = 0
                            statPack['values'][combinedName] = {}
                            orders[columnName] += 1
                orders["root"] += 1000

        while rowData:
            totalRowCnt += 1
            if not testRecord:
                testRecord = rowData

            for columnName in rowData:

                if is_empty(columnName, rowData[columnName]):
                    continue

                if is_iterable(rowData[columnName]) and type(rowData[columnName][0]) == dict:
                    update_stat_pack(statPack, orders, "root", columnName, len(rowData[columnName]))
                    if columnName not in orders:
                        orders[columnName] = statPack['orders'][columnName] + 1
                    for sub_row in rowData[columnName]:
                        for attr in sub_row:
                            combinedName = f"{columnName}.{attr}"
                            if is_empty(combinedName, sub_row[attr]):
                                continue
                            update_stat_pack(statPack, orders, columnName, combinedName, str(sub_row[attr]))
                else:
                    update_stat_pack(statPack, orders, "root", columnName, str(rowData[columnName]))

            currentFile, rowData = getNextRow(currentFile)

            #--break conditions
            if shutDown:
                break
            elif 'ERROR' in currentFile:
                break

            if currentFile['rowCnt'] % 10000 == 0:
                print(' %s records processed' % currentFile['rowCnt'])


        if currentFile['fieldDelimiter'] != 'PARQUET':
            currentFile['handle'].close()
        if shutDown:
            break
        else:
            print(' %s records processed, complete!' % currentFile['rowCnt'])
    
    #--export the analysis    
    if outputFileName:
        try: 
            outputFileHandle = open(outputFileName, 'w', newline='', encoding='utf-8')
            outputFileWriter = csv.writer(outputFileHandle, dialect=csv.excel, quoting=csv.QUOTE_ALL)
        except IOError as err: 
            print(f"Could not write to {outputFileName}")
            return 1
            
    bestRecordID = '<remove-or-supply>'
    possibleMappings = []    
    columnHeaders = "order, columnName,recordCount,percentPopulated,uniqueCount,uniquePercent,topValue1,topValue2,topValue3,topValue4,topValue5"
    if outputFileName:
        outputFileWriter.writerow(columnHeaders.split(','))
    else:
        print(columnHeaders)

    for item in sorted(statPack['orders'].items(), key=lambda kv: kv[1]):
        columnName = item[0]
        order = statPack['orders'].get(columnName, -1)
        recordCount = statPack['counts'][columnName]
        percentPopulated = round(recordCount / totalRowCnt * 100, 2)
        uniqueCount = len(statPack['values'][columnName])
        uniquePercent = round(uniqueCount / recordCount * 100, 2) if recordCount else 0

        #--first 100% unique field is recordID
        if uniqueCount == totalRowCnt and not bestRecordID.startswith('%'):
            bestRecordID = columnName

        topValue = []
        for value in sorted(statPack['values'][columnName].items(), key=lambda x: x[1], reverse=True):
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

        rowData = (order, columnName, recordCount, percentPopulated, uniqueCount, uniquePercent, topValue[0], topValue[1], topValue[2], topValue[3], topValue[4])
        if outputFileName:
            outputFileWriter.writerow(rowData)
        else:
            print('%s, "%s", %s, %s, %s, %s, "%s", "%s", "%s", "%s", "%s"' % rowData)
    
    #--close the output file
    if outputFileName:
        outputFileHandle.close()
        print('\nStatistics written to %s' % outputFileName)
    
    #--create or update the mapping file if provided
    if mappingFileName:
        if not mappingDoc['input']['fieldDelimiter']:
            del mappingDoc['input']['fieldDelimiter']
        if not mappingDoc['input']['fileEncoding']:
            del mappingDoc['input']['fileEncoding']
        if 'calculations' not in mappingDoc:
            mappingDoc['calculations'] = []
        if 'outputs' not in mappingDoc:
            mappingDoc['outputs'] = []
            outputDoc = {}
            outputDoc['data_source'] = '<supply>'
            outputDoc['record_type'] = 'GENERIC'
            outputDoc['record_id'] = '<remove_or_supply>'
            outputDoc['attributes'] = possibleMappings
            mappingDoc['outputs'].append(outputDoc)
        try:
            with open(mappingFileName, 'w') as f:
                json.dump(mappingDoc, f, indent=4)
        except Exception as e:
            print()
            print('Could not write to %s' % mappingFileName)
            print(e)

        print('\nMapping file written to %s' % mappingFileName)

    #--create or update the mapping file if provided
    if pythonModuleFile:

        codeLines = []
        with open(templateFile, 'r') as f:
            for line in f:

                if line.strip() == "input_file = '<input_file_name>'":
                    codeLines.append(line.replace('<input_file_name>', inputFileName))

                elif line.strip() == "csv_dialect = '<dialect>'" and mappingDoc['input'].get('csvDialect'):
                    if mappingDoc['input']['csvDialect'] == 'pipe':
                        registerDialect = "csv.register_dialect('pipe', delimiter = '|')"
                    elif mappingDoc['input']['csvDialect'] == 'other':
                        registerDialect = "csv.register_dialect('other', delimiter = '" + mappingDoc['input']['fieldDelimiter'] + "')"
                    else:
                        registerDialect = None
                    if registerDialect:
                        codeLines.append((' ' * (len(line) - len(line.strip()) -1)) + registerDialect + '\n')
                    codeLines.append(line.replace('<dialect>', mappingDoc['input']['csvDialect']))

                elif line.strip() == "input_file_handle = open(args.input_file, 'r')" and mappingDoc['input']['fileEncoding']:
                    codeLines.append(line.replace(')', ", encoding='%s')" % mappingDoc['input']['fileEncoding']))

                elif line.strip() == "json_data['RECORD_ID'] = '<supply>'":
                    if not bestRecordID.startswith('<'):
                        line = line.replace("'<supply>'", "raw_data['%s']" % bestRecordID)
                    codeLines.append(line)

                elif line.strip() == '#--column mappings':
                    codeLines.append(line)
                    for columnMapping in possibleMappings:
                        codeLines.append('\n')
                        codeLines.append("        # columnName: %s\n" % columnMapping['statistics']['columnName'])
                        codeLines.append("        # %s populated, %s unique\n" % (columnMapping['statistics']['populated%'], columnMapping['statistics']['unique%']))
                        for item in columnMapping['statistics']['top5values']:
                            codeLines.append("        #      %s\n" % item)
                        codeLines.append("        if raw_data.get('%s'):\n" % (columnMapping['statistics']['columnName']))
                        codeLines.append("            json_data['%s'] = raw_data.get('%s')\n" % (columnMapping['statistics']['columnName'], columnMapping['statistics']['columnName']))

                elif line.strip().startswith('raw_data = {'):
                    if not testRecord:
                        codeLines.append(line)
                    else:
                        codeLines.append('    raw_data = {}\n')
                        for key in testRecord:
                            val = testRecord[key]
                            if type(val) == str:
                                val = '"' + val + '"'
                            codeLines.append('    raw_data["%s"] = %s\n' % (key, val))

                else:
                    codeLines.append(line)

        try:
            with open(pythonModuleFile, 'w') as f:
                for line in codeLines:
                    f.write(line)
        except Exception as e:
            print()
            print('Could not write to %s' % pythonModuleFile)
            print(e)

        print('\nPython module written to %s' % pythonModuleFile)

    return shutDown

def update_stat_pack(statPack, orders, parent, key, val):
    if key not in statPack['counts']:
        statPack['orders'][key] = orders[parent]
        statPack['counts'][key] = 1
        statPack['values'][key] = {}
        orders[parent] += (1000 if parent == 'root' else 1)
    else:
        statPack['counts'][key] += 1

    if val not in statPack['values'][key]:
        statPack['values'][key][val] = 1
    else:
        statPack['values'][key][val] += 1

def is_empty(nam, val):
    if val is None:
        return True
    if is_iterable(val):
        return len(val) == 0
    return len(str(val).strip()) == 0

def is_iterable(val):
    if isinstance(val, np.ndarray) or isinstance(val, list):
        return True
    return False

def fileBackup(thisFileName):
    for i in range(10):
        i = '' if i == 0 else i
        backupFileName = thisFileName + '.bk%s' % i
        if os.path.exists(backupFileName):
            print('\t%s already exists!' % backupFileName)
        else:                    
            os.rename(thisFileName, backupFileName)
            print('\t%s created' % backupFileName)
            return True
    return False

#----------------------------------------
if __name__ == "__main__":
    procStartTime = time.time()
    shutDown = False   
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inputFileName', dest='inputFileName', help='the name of a csv input file')
    parser.add_argument('-d', '--fieldDelimiter', dest='fieldDelimiter', help='csv delimiter character or "json" or "parquet"')
    parser.add_argument('-e', '--fileEncoding', dest='fileEncoding', help='file encoding')
    parser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    parser.add_argument('-m', '--mappingFileName', dest='mappingFileName', help='optional name of a mapping file to generate')
    parser.add_argument('-p', '--pythonModuleFile', dest='pythonModuleFile', help='optional name of a python module file to generate')
    args = parser.parse_args()
    inputFileName = args.inputFileName
    fieldDelimiter = args.fieldDelimiter.upper() if args.fieldDelimiter else ''
    fileEncoding = args.fileEncoding
    outputFileName = args.outputFileName
    mappingFileName = args.mappingFileName
    pythonModuleFile = args.pythonModuleFile
    
    if not inputFileName:
        print('\nAn input file name is required\n')
        sys.exit(1)
        
    if fieldDelimiter == 'PARQUET' and not pd:
        print('\nPandas must be installed to analyze parquet files, try: pip install pandas\n')
        sys.exit(1)

    if mappingFileName and os.path.exists(mappingFileName):
        response = input('\nMapping file already exists!!, overwrite it? (Y/N) ')
        if response.upper.startswith('Y'):
            mappingFileName = None
            print('\nMapping file will be preserved!')
        elif not fileBackup(mappingFileName):
            print('\nAborted, backup failed!\n')
            sys.exit(1)

    if pythonModuleFile and os.path.exists(pythonModuleFile):
        response = input('\nPython module already exists!, overwrite it? (Y/N) ')
        if response[0:1].upper() != 'Y':
            pythonModuleFile = None
            print('\nmapping file will be preserved!')
        elif not fileBackup(pythonModuleFile):
            print('\nAborted, backup failed!\n')
            sys.exit(1)

    #--make sure the template file exists
    if pythonModuleFile:
        templateFile = os.path.dirname(__file__) + os.path.sep + 'python_template.py'
        if not os.path.exists(templateFile):
            print(f'\nCannot find {templateFile}\n')
            sys.exit(1)

    result = analyzeFile()
    
    print('')
    if result != 0:
        print('process aborted!')
    else:
        print('process completed!')
    print('')
    
    sys.exit(result)
