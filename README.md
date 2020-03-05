# mapper-csv

## Overview

These csv mapping tools help you map any csv file into json for loading into Senzing.  It contains the following python scripts ...
- The [csv_analyzer.py](csv_analyzer.py) script reads a csv, accumulating column statistics like percent populated, percent unique and top 5 values.  It also creates a default mapping file to be used by the csv_mapper.
- The [csv_mapper.py](csv_mapper.py) script reads a csv using a mapping file to turn it into senzing json.
- The [csv_functions.py](csv_functions.py) and associated [csv_functions.json](csv_functions.json) combine to create a set of functions that can be called by the csv_mapper to convert data.  It contains functions to detect if a name is an organization or a person, standardize dates. etc.  It is expected that you will add your own functions, organization name tokens, etc.


## Contents

1. [Prerequisites](#prerequisites)
1. [Installation](#installation)
1. [Tutorial](#typical-use)
    1. Run the analayzer
1. [Mapping file structure](#mapping-file-structure)
    1. [Input section](#input-section)
    1. [Output section](#output-section)
    1. [Calculations section](#calculations-section)
    1. [Multiple mappings and filters](#multiple-mappings-and-filters)

### Prerequisites
- python 3.6 or higher

### Installation

Place the the following files on a directory of your choice.
*including the input, mappings and output subdirectories and files for the tutorial*

- [csv_analyzer.py](csv_analyzer.py)
- [csv_mapper.py](csv_mapper.py)
- [csv_functions.py](csv_functions.py)
- [csv_functions.json](csv_functions.json)
    - [input/test_set1.csv](input/test_set1.csv)
    - [mappings/test_set1.map](input/test_set1.map)
    - [output/test_set1.json](input/test_set1.json)


### Tutorial
####Run the analyzer and review the column statistics
```console
python csv_analyzer.py -i input/test_set1.csv -o input/test_set1-analysis.csv -m mappings/test_set1.map

Mapping file already exists!!, overwrite it? (Y/N) y
current mapping file renamed to mappings/test_set1.map.bk

Analyzing input/test_set1.csv ...
 8 records processed, complete!

Writing results to input/test_set1-analysis.csv ...

process completed!
```


### Mapping file structure

Review the [mappings/test_set1.map](mappings/test_set1.map). The majority of it was built by the csv_analyzer.

#### Input section
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
#### Output section
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
#### Calculations section
This is where you can transform the data in your csv file. Here you can execute python code to create new columns from old columns.  
```console
"calculations": [
    {"is_organization": "csv_functions.is_organization_name(rowData['name'])"},
    {"record_type": "'ORGANIZATION' if rowData['is_organization'] else 'PERSON'"},
    {"name_org": "rowData['name'] if rowData['is_organization'] else ''"},
    {"name_full": "rowData['name'] if not rowData['is_organization'] else ''"},
    {"ref_gender": "rowData['gender'] if rowData['is_organization'] else ''"},
    {"ref_dob": "rowData['dob'] if rowData['is_organization'] else ''"},
    {"ref_ssn": "rowData['ssn'] if rowData['is_organization'] else ''"},
    {"ref_dlnum": "rowData['dlnum'] if rowData['is_organization'] else ''"},
    {"gender": "rowData['gender'] if not rowData['is_organization'] else ''"},
    {"dob": "rowData['dob'] if not rowData['is_organization'] else ''"},
    {"ssn": "rowData['ssn'] if not rowData['is_organization'] else ''"},
    {"dlnum": "rowData['dlnum'] if not rowData['is_organization'] else ''"}
],
```
The syntax is {"newcolumnName": "single line of python code"}.  References to current column values use the rowData['columnName'] syntax. These calculations are executed in order which means that all the prior calculation's new column values are available to the later ones.  Notice that the first calculation creates the "is_organization" value that is called by all the ones after it.

If the code you need to execute is more than a single line, create a function for it in the csv_functions.py script.  Notice how the first calculation that creates is_organization calls the approriate csv_function.

Also notice that this set of calculations ... 
1. detects whether the name field on the csv record is an organization or not 
2. sets the record_type to "ORGANIZATION" or "PERSON"
3. populates the the name_org or name_full column values for later mapping. 
4. renames gender, dob, ssn and dlnum to ref_* equivalents if the name really represents an organization which should not have such features.  It does this in single commands by first creating the ref* columns and then clearing the original columns.

Finally, notice that these calculated fields were mapped by adding then to the attributes section like so ...
```console
"attributes": [
    ...
    {
        "attribute": "NAME_ORG",
        "mapping": "%(name_org)s"
    },
    {
        "attribute": "NAME_FULL",
        "mapping": "%(name_full)s"
    },
    {
        "attribute": "RECORD_TYPE",
        "mapping": "%(record_type)s"
    },
    {
        "attribute": "REF_GENDER",
        "mapping": "%(ref_gender)s"
    },
    {
        "attribute": "REF_DOB",
        "mapping": "%(ref_dob)s"
    },
    {
        "attribute": "REF_SSN",
        "mapping": "%(ref_ssn)s"
    },
    ...
```
#### Multiple mappings and filters
Occasionally, you will have a csv file that really contains multiple entities which should be presented to the Senzing engine in separate json messages.  
```console
    "outputs": [                                <-- this is a list of output messages
        {
            "filter": "not rowdata['name']",   <-- this is a record level filter  
            "data_source": "TEST",
```
Simply duplicate the output structure as many times as you want.  Each must have a data source, entity type, record ID and list of attrributes.

Just like calculations above, the filter is a single line python expression referencing current record column values with the rowData['columnName'] syntax.  The one above bypasses any csv record with an empty name field.
