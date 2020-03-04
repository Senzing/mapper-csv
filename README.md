# mapper-csv

## Overview

These csv mapping tools help you map any csv file into json for loading into Senzing.  It contains the following python scripts ...
- The [csv_analyzer.py](csv_analyzer.py) script reads a csv, accumulating column statistics like percent populated, percent unique and top 5 values.  It also creates a default mapping file to be used by the csv_mapper.
- The [csv_mapper.py](csv_mapper.py) script reads a csv using a mapping file to turn it into senzing json.
- The [csv_functions.py](csv_functions.py) and associated [csv_functions.json](csv_functions.json) combine to create a set of functions that can be called by the csv_mapper to convert data.  It contains functions to detect if a name is an organization or a person, standardize dates. etc.  It is expected that you will add your own functions, organization name tokens, etc.

## What a mapping file looks like

Review the [mappings/test_set1.map](mappings/test_set1.map). The majority of it was built by the csv_analyzer!

## Input Section
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
## Output Section
The purpose of this section is to map the csv columns to valid Senzing json attributes as defined here ... https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification

**Step 1:** Decide on a data source code, entity type and record_id for this data set.  You can hard code values like "TEST" below or refer to csv columns in source input file using the pythonic notation for replacing data in strings "%(columnName)s" 
```console
"outputs": [
    {
        "data_source": "<supply>",  <--"TEST" supplied here
        "entity_type": "<supply>",  <--"%(record_type)s" supplied here
        "record_id": "<supply>",    <--"%(uniqueid)s" supplied here
        ...
```
**Step 2:** Then for each column, replace the attribute tag with the Senzing attribute of your choice.   Please note you can delete any attributes you don't want or add any new attributes you have computed in the calculations section described below.
```console
{
    "attribute": "<ignore>",    <--GENDER supplied here
    "mapping": "%(gender)s"
},
{
    "attribute": "<ignore>",    <--DATE_OF_BIRTH supplied here
    "mapping": "%(DOB)s"
}
```
You may notice the statistics section supplied for each column by the csv_analyzer. This is not required by the csv_mapper, but is included to help you decide on the appropriate mapping.  For instance if this field was not populated or did not contain M and F as values, you may want to not map it at all or convert it in the calculations sectrion described below.
```console
"statistics": {
    "columnName": "gender",
    "populated%": 42.86,
    "unique%": 66.67,
    "top5values": [
        "F (2)",
        "M (1)"
    ]
}
```
## Calculations Section
The primary purpose of this section is to set the file type and column headers.   
```console

```
