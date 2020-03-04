# mapper-csv

## Overview

The csv mapper tools help you map any csv file into json for loading into Senzing.  It contains the following python scripts ...
- The [csv_analyzer.py](csv_analyzer.py) script reads a csv, accumulating column statistics like percent populated, percent unique and top 5 values.  It even creates a default mapping file to be used by the csv_mapper.
- The [csv_mapper.py](csv_mapper.py) script reads a csv using a mapping file to turn it into senzing json.
- The [csv_functions.py](csv_functions.py) and associated [csv_functions.json](csv_functions.json) combine to create a set of functions that can be called by the csv_mapper to convert data.  It contains functions to detect if a name is an organization or a person, standardize dates. etc.  It is expected that you will add your own functions, organization name tokens, etc.

## What a mapping file looks like

Review the [mappings/test_set1.map](mappings/test_set1.map)

The majority of it was built with csv_analyzer.py!

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
## Input Section
The purpose of this section is to map the csv columns to valid Senzing json attributes as defined here ...






## Calculations Section
The primary purpose of this section is to set the file type and column headers.   
```console

```
