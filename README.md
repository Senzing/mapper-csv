# mapper-csv

## Overview

These csv mapping tools help you map any csv file into json for loading into Senzing.  It contains the following python scripts ...
- The [csv_analyzer.py](csv_analyzer.py) script reads a csv, accumulating column statistics like percent populated, percent unique and top 5 values.  It also creates a default mapping file to be used by the csv_mapper.
- The [csv_mapper.py](csv_mapper.py) script reads a csv using a mapping file to turn it into senzing json.
- The [csv_functions.py](csv_functions.py) and associated [csv_functions.json](csv_functions.json) are a set of functions used by the csv_analyzer and csv_mapper.
- The [python_template.py](python_template.py) is the template for a customizable python module.  Sometimes the number of transforms required to map a csv file warrant going straight to python.  As a bonus, this module is portable in that it contains a mapping class that can be called from other transports.   Sometimes, you want to test your mappings in a file, but implement it via a message queue.

## Contents

1. [Prerequisites](#prerequisites)
1. [Installation](#installation)
1. [Tutorial](#typical-use)
    1. [Run the analyzer](#run-the-analyzer)
    1. [Review the statistics](#review-the-statistics)
    1. [Complete the mapping](#complete-the-mapping)
    1. [Generate the json file](#generate-the-json-file)
    1. [Loading into Senzing](#loading-into-senzing)
1. [Mapping file structure](#mapping-file-structure)
    1. [Input section](#input-section)
    1. [Output section](#output-section)
    1. [Calculations section](#calculations-section)
1. [Mapping template class](#mapping-template-class)
    1. [Input section](#input-section)
    1. [Output section](#output-section)
    1. [Calculations section](#calculations-section)
    1. [Multiple mappings and filters](#multiple-mappings-and-filters)

### Prerequisites
- python 3.6 or higher

### Installation

Place the the following files on a directory of your choice.

- [csv_analyzer.py](csv_analyzer.py)
- [csv_mapper.py](csv_mapper.py)
- [csv_functions.py](csv_functions.py)
- [csv_functions.json](csv_functions.json)
- [csv_functions.json](csv_functions.json)
- [python_template.py](python_template.py)

Include the input, mappings and output subdirectories and files for the tutorial

- [input/test_set1.csv](input/test_set1.csv)
- [mappings/test_set1.map](mappings/test_set1.map)
- [mappings/test_set1.py](mappings/test_set1.py)
- [output/test_set1.json](output/test_set1.json)

### Tutorial

Follow these steps in order.  First use the supplied file test_set1.csv.  Then try it with one of your own files!

### Run the analyzer

Execute the csv_analyzer script as follows ...
```console
python csv_analyzer.py \
  -i input/test_set1.csv \
  -o input/test_set1-analysis.csv \
  -m mappings/test_set1.map \
  -p mappings/test_set1.py
```
- The -i parameter is for the csv file you want to analyze.
- The -o parameter is for the name of the file to write the statistics to.  It is a csv file as well.
- The -m parameter is for the name of the mapping file to create.  You will later edit this file to map the csv to json using this method.
- The -p parameter is for the name of the python module file to create.  You will later edit this file to map the csv to json using this method.

*Note: The csv analyzer use the csv module sniffer to determine the file delimiter for you.   If you have problems with this, you can override the delimiter and even the file encoding.*
- The -d parameter can be used to set the csv column delimiter manually
- The -e parameter can be used to set the encoding to sdomething like latin-1 if needed

*Note: Normally you would decide if you want a simple mapping with the -m parameter or a portable python module with the -p parameter.  There is no need to do both.  Non-python programmers can do simple mappings using the -m map[ping file method.  Python programmers will likley want to use the -p python module method as they have more complete control over the process.*

Type "python csv_analyzer.py --help"  For the additional parameters you can specify.

### Review the statistics

Open the input/test_set1-analysis.csv in your favorite spreadsheet editor.  The columns are ...
- columnName - The name of the column. 
- recordCount - The number of records with a value in this column.
- percentPopulated - The percent populated (recordCount/total record count).
- uniqueCount - The number of unique values in this column.
- uniquePercent - The unique percent (uniqueCount/recordCount).
- topValue1-5 - The top 5 most used values.  Shows the value with the number of records containing it in parenthesis.

The purpose of this analysis helps you to determine what columns to map in the file.  For instance ...
- Lets say the gender column seems to contain a date of birth and the date of birth column seems to contain the address line 1. In this case you may want to have the file re-generated as everything seems to have shifted!
- Lets say you are hoping to to match on ssn plus name. But when you look at the SSN column statistics you find that it is only 10% populated.  In this case you will want to find more values to map.
- Lets say you want to use the SSN column to match and it is 100% populated. But it is only 10% unique meaning a lot of the records have the same SSN.  This may be ok, but it certainly indicates that you are looking at a list of transactions rather than a list of entities.
- Lets say you have last_name and first_name columns but the first_name column is completely blank and the top 5 last_name examples appear to have both last and first names!  In this case you would want to map last_name to the Senzing NAME_FULL attribute and not map first name at all.

### Complete the mapping or python template



The best way to describe how to complete the mapping is review the [Mapping file structure](#mapping-file-structure)

Open the mapping file mappings/test_set1.map with your favorite text editor.

Remember when you ran the analyzer above and saved the current mapping file for this csv to *mappings/test_set1.map.bk*?  Open that file as well as and copy/paste examples into the new one based on the mapping file struture described below.

### Generate the json file

Execute the csv_mapper script as follows ...
```console
python csv_mapper.py -i input/test_set1.csv -m mappings/test_set1.map -o output/test_set1.json -l output/test_set1-statistics.json

Processing input/test_set1.csv ...
 10 rows processed, 1 rows skipped, complete!               <--the header row was skipped

OUTPUT #0 ...
  8 rows written
  1 rows skipped                                            <--this was due to the empty name filter

 MAPPED ATTRIBUTES:                                         <--these are the attributes that will be used for resolution
  name_full.....................          4 50.0 %
  name_org......................          4 50.0 %
  date_of_birth.................          2 25.0 %
  ssn_number....................          2 25.0 %
  addr_type.....................          4 50.0 %
  addr_line1....................          8 100.0 %
  addr_city.....................          8 100.0 %
  addr_state....................          8 100.0 %
  addr_postal_code..............          8 100.0 %

 UNMAPPED ATTRIBUTES:                                       <--these are attributes you decided to keep that won't be used for resolution
  important_date................          8 100.0 %
  important_status..............          8 100.0 %

 COLUMNS IGNORED: 
  uniqueid, type, name, gender, value                       <--make sure you didn't intend to map these!

process completed!
```
The -i parameter is for the csv file you want to map into json.
The -o parameter is for the name of the json records to.
The -m parameter is for the name of the completed mapping file to use.

You will want to review the statistics it produces and make sure it makes sense to you ... 
- Do the mapped statistics make sense?  Especially for calculated values such as name_org and name_full.   In this case, it shows that about 1/2 the records were for organizations and half were people.
- Should any of the unmapped attributes really be mapped?  Maybe there is a typo.  Maybe prof_license should have been named prof_license_number!
- Should any of the columns ignored be included?

### Loading into Senzing

*Please be sure first add any new configurations to Senzing!  This might include new data sources, entity types, features or attributes!.  See the G2ConfigTool.py and readme on the /opt/senzing/g2/python subdirectory.*

If you use the G2Loader program to load your data, from the /opt/senzing/g2/python directory ...

```console
python3 G2Loader.py -f <path-to>/test_set1.json
```

Please note you could also use the stream loader here https://github.com/Senzing/stream-loader

*In fact, a future update of this project will send the output directly to a rabbit mq so that yet another file of the data does not have to be created.  Or you could modify this program yourself!*

### Mapping file structure

Review the [mappings/test_set1.map](mappings/test_set1.map). The majority of it was built by the csv_analyzer.

### Input section
The purpose of this section is to set the csv file delimiter and column headers.   
```console
"input": {
    "inputFileName": "input/test_set1.csv",
    "fieldDelimiter": ",",
    "columnHeaders": [
        "uniqueid",
        "name",
        "gender",
        ...
```

### Calculations section
This is where you can transform the data in your csv file. Here you can execute python code to create new columns from old columns.  
```console
"calculations": [
      {"name_org": "rowData['name'] if rowData['type'] == 'company' else ''"},
      {"name_full": "rowData['name'] if rowData['type'] != 'company' else ''"},
      {"addr_type": "'BUSINESS' if rowData['type'] == 'company' else ''"}
],
```
The syntax is {"newcolumnName": "single line of python code"}.  References to current column values use the rowData['columnName'] syntax. These calculations are executed in order which means that all the prior calculation's new column values are available to the later ones.  Notice that the first calculation creates the "is_organization" value that is called by all the ones after it.

If the code you need to execute is more than a single line, create a function for it in the csv_functions.py script.  Notice how the first calculation that creates is_organization calls the approriate csv_function.

The calculations above determine whether or not the name field should be mapped to name_org or name_full and if the address should be labled a "business" address.  It is a best practice for organizations to have their name mapped as a name_org and their physical address to be labeled business.

### Output section
The purpose of this section is to map the csv columns to valid Senzing json attributes as defined here ... https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification

Occasionally, you will have a csv file that really contains multiple entities which should be presented to the Senzing engine in separate json messages.  
```console
"outputs": [                                <-- this is a list of output messages
    {
        "filter": "not rowData['name']",   <-- this is a record level filter  
```
Simply duplicate the output structure as many times as you want.  Each must have a data source, entity type, record ID and list of attributes.

Just like calculations above, the filter is a single line python expression referencing current record column values with the rowData['columnName'] syntax.  The one above bypasses any csv record with an empty name field.

**Step 1:** Decide on a data source code, entity type and record_id for this data set. You can hard code values like "TEST" below or refer to csv columns in source input file using the pythonic notation for replacing data in strings "%(columnName)s"
```console
"outputs": [
    {
        "filter": "not rowData['name']",      <-- add this filter to remove the blank record in the test file 
        "data_source": "<supply>",            <-- use "TEST" here
        "entity_type": "GENERIC",             <-- keep the default "GENERIC" here
        "record_id": "<remove_or_supply>",    <-- use "%(uniqueid)s" as it is a unique value (see the statistics below?) 
        ...
```
**Step 2:** Then for each column, replace the attribute tag with the Senzing attribute of your choice.   Please note you can delete any attributes you don't want or add any new attributes you have computed in the calculations section described below.

*You may notice the statistics section supplied for each column by the csv_analyzer. While not required by the csv_mapper, it is included to help you decide on the appropriate mapping.  For instance if this field was not populated or did not contain M and F as values, you may want to not map it at all or convert it in the calculations sectrion described below.*
```console
"attributes": [
    {
        "attribute": "<ignore>",            <--keep ignoring this as we used it as the record_id above
        "mapping": "%(uniqueid)s",
        "statistics": {
            "columnName": "uniqueid",
            "populated%": 100.0,
            "unique%": 100.0,
            ...
        }
    },
    {
        "attribute": "<ignore>",            <--keep ignoring this as it will not be used for resolution
        "mapping": "%(type)s",
        ...
    },
    {                                       <--add this whole attribute section to map the calculated field
        "attribute": "NAME_FULL",
        "mapping": "%(name_full)s"
    },
    {                                       <--add this whole attribute section to map the calculated field
        "attribute": "NAME_ORG",            
        "mapping": "%(name_org)s"
    },
    {
        "attribute": "<ignore>",            <--keep ignoring the name as we replace it with name_org or name_full
        "mapping": "%(name)s",
        ...
    },
    {
        "attribute": "<ignore>",            <--keep ignoring the gender as it is not populated with meaningful values
        "mapping": "%(gender)s",
        "statistics": {
            "columnName": "gender",
            "populated%": 100.0,
            "unique%": 11.11,
            "top5values": [
                "u (9)"
            ]
        }
    },
    {
        "attribute": "DATE_OF_BIRTH",       <--map this to a recognized attribute in the generic entity specification
        "mapping": "%(dob)s",
        ...
    },
    {
        "attribute": "SSN_NUMBER",          <--map this to a recognized attribute in the generic entity specification
        "mapping": "%(ssn)s",
        ...
    {                                       <--add this whole attribute section to map the calculated field
        "attribute": "ADDR_TYPE",         
        "mapping": "%(addr_type)s"
    },
    {
        "attribute": "ADDR_LINE1",          <--map this to a recognized attribute in the generic entity specification
        "mapping": "%(addr1)s",
        ...
    },
    {
        "attribute": "ADDR_CITY",           <--map this to a recognized attribute in the generic entity specification
        "mapping": "%(city)s",
        ...
    {
        "attribute": "ADDR_STATE",          <--map this to a recognized attribute in the generic entity specification
        "mapping": "%(state)s",
        ...
    },
    {
        "attribute": "ADDR_POSTAL_CODE",    <--map this to a recognized attribute in the generic entity specification
        "mapping": "%(zip)s",
        ...
    }
    {
        "attribute": "important_date",      <--map this important value to a standardized name even though not used for resolution
        "mapping": "%(create_date)s",
        ...
    },
    {
        "attribute": "important_status",    <--map this important value to a standardized name even though not used for resolution
        "mapping": "%(status)s",
        ...
    },
    {
        "attribute": "<ignore>",            <--keep ignoring this as it may be too sensitive to store in Senzing
        "mapping": "%(value)s",
        ...
    }

```
