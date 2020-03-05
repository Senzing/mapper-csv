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
    1. [Run the analyzer](#run-the-analyzer)
    1. [Review the statistics](#review-the-statistics)
    1. [Complete the mapping](#complete-the-mapping)
    1. [Generate the json file](#generate-the-json-file)
    1. [Loading into Senzing](#loading-into-senzing)
1. [Mapping file structure](#mapping-file-structure)
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

Include the input, mappings and output subdirectories and files for the tutorial@

- [input/test_set1.csv](input/test_set1.csv)
- [mappings/test_set1.map](input/test_set1.map)
- [output/test_set1.json](input/test_set1.json)


### Tutorial

Follow these steps in order.  First use the supplied file test_set1.json.  Then try it with one of your own files!

### Run the analyzer

Execute the csv_analyzer script as follows ...
```console
python csv_analyzer.py -i input/test_set1.csv -o input/test_set1-analysis.csv -m mappings/test_set1.map

Mapping file already exists!!, overwrite it? (Y/N) y       <--this will only occur if you analyze a file twice
current mapping file renamed to mappings/test_set1.map.bk  <--we will use this file in the tutorial!

Analyzing input/test_set1.csv ...
 8 records processed, complete!

Writing results to input/test_set1-analysis.csv ...

process completed!
```
The -i parameter is for the csv file you want to analyze.

The -o parameter is for the name of the file to write the statistics to.  It is a csv file as well.

The -m parameter is for the name of the mapping file to create.  You will later edit this file to map the csv to json.

*Note: the csv analyzer attempts to determine the file delimiter for you.   If you have problems with this, you can override the delimiter and even the file encoding.*

Type "python csv_analyzer.py --help"  For the additional parameters you can specify.

### Review the results

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

### Complete the mapping
The best way to describe how to complete the mapping is review the [Mapping file structure](#mapping-file-structure)

Open the mapping file mappings/test_set1.map with your favorite text editor.

Remember when you ran the analyzer above and saved the current mapping file for this csv to *mappings/test_set1.map.bk*?  Open that file as well as and copy/paste examples into the new one based on the mapping file struture described below.

### Generate the json file

Execute the csv_mapper script as follows ...
```console
python csv_mapper.py -i input/test_set1.csv -m mappings/test_set1.map -o output/test_set1.json

Processing input/test_set1.csv ...
 8 rows processed, 1 rows skipped, complete!

OUTPUT #0 ...
  6 rows written
  1 rows skipped                                        <--this was due to the empty name filter

 MAPPED ATTRIBUTES:
  name_org......................          3 50.0 %      <--these are the calculated attributes
  name_full.....................          3 50.0 %
  record_type...................          6 100.0 %
  gender........................          2 33.33 %
  date_of_birth.................          1 16.67 %
  ssn_number....................          1 16.67 %
  tax_id_number.................          1 16.67 %
  addr_line1....................          6 100.0 %
  addr_city.....................          6 100.0 %
  addr_state....................          6 100.0 %
  addr_postal_code..............          6 100.0 %

 UNMAPPED ATTRIBUTES:
  ref_gender....................          0 0.0 %
  ref_dob.......................          1 16.67 %     <--these are the reclassed values for organizations
  ref_ssn.......................          1 16.67 %
  ref_dlnum.....................          0 0.0 %
  prof_license..................          6 100.0 %     <--did you expect this to be a mapped attribute?

 COLUMNS IGNORED: 
  uniqueid, name                                        <--make sure you didn't intend to map these!

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
### Output section
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
### Calculations section
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
### Multiple mappings and filters
Occasionally, you will have a csv file that really contains multiple entities which should be presented to the Senzing engine in separate json messages.  
```console
    "outputs": [                                <-- this is a list of output messages
        {
            "filter": "not rowdata['name']",   <-- this is a record level filter  
            "data_source": "TEST",
```
Simply duplicate the output structure as many times as you want.  Each must have a data source, entity type, record ID and list of attrributes.

Just like calculations above, the filter is a single line python expression referencing current record column values with the rowData['columnName'] syntax.  The one above bypasses any csv record with an empty name field.
