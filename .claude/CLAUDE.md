# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mapper-csv is a set of Python tools for mapping CSV files into JSON format for loading into Senzing entity resolution software. The tools analyze CSV files, generate column statistics, and produce either JSON mapping files or standalone Python mapper modules.

## Commands

### Install Dependencies

```bash
python -m pip install --group all .
```

### Linting

```bash
pylint $(git ls-files '*.py' ':!:docs/source/*')
```

### Run CSV Analyzer (generate Python module - recommended)

```bash
python src/csv_analyzer.py -i input/file.csv -o output/analysis.csv -p mappings/file.py
```

### Run CSV Analyzer (generate mapping file)

```bash
python src/csv_analyzer.py -i input/file.csv -o output/analysis.csv -m mappings/file.map
```

### Run CSV Mapper with mapping file

```bash
python src/csv_mapper.py -i input/file.csv -m mappings/file.map -o output/file.json -l output/stats.json
```

### Run standalone Python mapper module

```bash
python mappings/file.py -i input/file.csv -o output/file.json -l output/stats.json
```

## Architecture

### Core Scripts (src/)

- **csv_analyzer.py**: Analyzes CSV files to generate column statistics (percent populated, percent unique, top 5 values). Outputs either a JSON mapping file (`-m`) or a standalone Python mapper module (`-p`) based on `python_template.py`.

- **csv_mapper.py**: Processes CSV files using either a JSON mapping file (`-m`) or a Python mapper module (`-p`) to produce Senzing-compatible JSON output. Supports calculations, filters, and attribute aggregation.

- **csv_functions.py**: Utility class providing date formatting, value cleaning, and Senzing attribute detection. Loads configuration from `csv_functions.json` which defines garbage values and valid Senzing attributes.

- **python_template.py**: Template used by csv_analyzer to generate standalone mapper modules. Contains the `mapper` class with methods for cleaning values, computing record hashes, formatting dates, and capturing statistics.

### Mapping File Structure

JSON mapping files have three sections:

- **input**: File settings (inputFileName, fieldDelimiter, columnHeaders)
- **calculations**: Python expressions to create derived columns (e.g., `{"name_org": "rowData['name'] if rowData['type'] == 'company' else ''}`)
- **outputs**: Data source, record type, record ID, filters, and attribute mappings to Senzing attributes

### Key Concepts

- Senzing attributes (NAME_FULL, NAME_ORG, SSN_NUMBER, DATE_OF_BIRTH, ADDR_LINE1, etc.) are used for entity resolution
- Non-Senzing attributes are preserved but not used for matching
- The `<ignore>` attribute tag excludes columns from output
- Calculations use `rowData['columnName']` syntax to reference column values
