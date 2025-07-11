#! /usr/bin/env python3
import os
import sys
import argparse
import configparser
import time
from datetime import datetime, date, timedelta
import json
import csv
import glob
import numpy as np
import subprocess
import glob
import pathlib
from typing import Iterable, Iterator


try:
    import pandas as pd
except: 
    pd = False

try: 
    import prettytable
except: 
    prettytable = False


class JsonReader(Iterable):
    def __init__(self, iterable: Iterator):
        self.iterable = iterable
        self._iterator = None

    def __iter__(self) -> Iterator:
        self._iterator = iter(self.iterable)
        return self

    def __next__(self):
        return json.loads(next(self._iterator))



class Node(object):

    def __init__(self, node_id):
        self.node_id = node_id
        self.node_desc = node_id
        self.node_type = None
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)

    def render_tree(self):
        tree = f"{self.node_desc} ({self.node_type})\n"
        parents = [{"node": self, "display_children": self.children}]
        while parents:
            if len(parents[-1]["display_children"]) == 0:
                parents.pop()
                continue
            next_node = parents[-1]["display_children"][0]
            parents[-1]["display_children"].pop(0)

            prefix = ""
            for i, _ in enumerate(parents):
                last_child = len(parents[i]["display_children"]) == 0
                if i < len(parents) - 1:  # prior level
                    prefix += "    " if last_child else "\u2502   "
                else:
                    prefix += "\u2514\u2500\u2500 " if last_child else "\u251c\u2500\u2500 "

            tree += f"{prefix}{next_node.node_desc} ({next_node.node_type})\n"
            if next_node.children:
                prior_parents = [x["node"].node_id for x in parents]
                display_children = [x for x in next_node.children if x.node_id not in prior_parents]
                parents.append({"node": next_node, "display_children": display_children})

        return tree


class FileAnalyzer():

    def __init__(self, file_name, file_type):
        self.record_count = 0
        self.root_node = Node('root')
        self.root_node.node_desc = file_name
        self.root_node.node_type = file_type
        self.nodes = {"root": self.root_node}


    def iterate_obj(self, prior_key, obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                self.update_node(prior_key, key, value)
                if isinstance(value, (dict, list, np.ndarray)): 
                    self.iterate_obj(f"{prior_key}.{key}", value)

        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list, np.ndarray)): 
                    self.iterate_obj(prior_key, item)
                else:
                    self.update_node(prior_key, prior_key.split('.')[-1], item)


    def update_node(self, prior_key, key, value):
        attr_key = f"{prior_key}.{key}" if key else prior_key
        if attr_key not in self.nodes:
            self.nodes[attr_key] = Node(attr_key)
            self.nodes[attr_key].node_desc = attr_key.replace('root.', '')
            self.nodes[attr_key].node_type = 'unk'
            
            self.nodes[attr_key].record_count = 0
            self.nodes[attr_key].unique_values = {}
            self.nodes[prior_key].add_child(self.nodes[attr_key])

        if value is not None and value:
            if self.nodes[attr_key].node_type == 'unk':
                self.nodes[attr_key].node_type = str(type(value))[8:-2]


            if isinstance(value, (dict, list)): 
                value = f"{len(value)} items"
            value = str(value)


            self.nodes[attr_key].record_count += 1
            if value not in self.nodes[attr_key].unique_values:
                self.nodes[attr_key].unique_values[value] = 1
            else:
                self.nodes[attr_key].unique_values[value] += 1

    def generate(self, template):
        header = ["attribute", "type", "record_cnt", "record_pct", "unique_cnt", "unique_pct", "top_value1", "top_value2", "top_value3", "top_value4", "top_value5"]
        rows = []
        root_node = self.root_node
        parents = [{"node": root_node, "children": root_node.children.copy()}]
        while parents:
            if len(parents[-1]["children"]) == 0:
                parents.pop()
                continue
            next_node = parents[-1]["children"][0]
            parents[-1]["children"].pop(0)

            attr_code = next_node.node_desc
            attr_type = next_node.node_type
            record_cnt = next_node.record_count
            record_pct = round(record_cnt / self.record_count * 100, 2)
            unique_cnt = len(next_node.unique_values)
            unique_pct = round(unique_cnt / record_cnt * 100, 2) if record_cnt else 0

            top_values = [''] * 5
            i = 0
            for k, v in sorted(next_node.unique_values.items(), key=lambda v: v[1], reverse=True):
                top_values[i] = f"{str(k)[0:50]} ({v})"
                i += 1
                if i == 5:
                    break

            if template == 'report':
                rows.append([attr_code, attr_type, record_cnt, record_pct, unique_cnt, unique_pct] + top_values)
            elif template == 'code':

                attr_list = attr_code.split('.')
                last_attr = attr_list[-1]
                if len(attr_list) == 1:
                    indent = ''
                    prior_attr = ''
                    prior_data = 'raw_data'
                else:
                    indent = '    ' * (len(attr_list) - 1)
                    prior_attr = attr_list[-2]
                    prior_data = f'raw_data{len(attr_list) - 1}'


                rows.append("")
                rows.append(f"{indent}# attribute: {attr_code} ({attr_type})")
                rows.append(f"{indent}# {record_pct} populated, {unique_pct} unique")
                for item in (item for item in top_values if item):
                    rows.append(f"{indent}#      {item}")

                if attr_type in ('list'):
                    new_data = f'raw_data{len(attr_list)}'
                    rows.append(f'{indent}{new_data}_list = {prior_data}.get("{last_attr}") if {prior_data}.get("{last_attr}") is not None else []:')
                    rows.append(f'{indent}for {new_data} in {new_data}_list:')
                else:
                    rows.append(f'{indent}if {prior_data}.get("{last_attr}") and {prior_data}[{last_attr}"]:')
                    rows.append(f'{indent}    json_data["{last_attr}"] = {prior_data}["{last_attr}"]')

            if next_node.children:
                prior_parents = [x["node"].node_id for x in parents]
                parents.append({"node": next_node, "children": next_node.children.copy()})

        if template == 'report':
            rows = [header] + rows
        return rows


def create_python_script(code_rows, file_type, encoding):
    script_rows = []
    template_file_name = os.path.dirname(__file__) + os.path.sep + 'python_template.py'

    import_here = '# import csv or pandas here'
    mapping_start = '# place column mappings here'
    open_reader = '# open reader here'
    mapper_call = 'for json_data in mapper.map(row)'
    file_loop = 'for row in reader:'
    close_file = '# close reader here'
    with open(template_file_name,'r') as file:
        for line in file:
            line = line[0:-1] # remove linefeed
            if line.startswith(import_here):
                if file_type == 'csv':
                    script_rows.append('import csv')
                elif file_type == 'parquet':
                    script_rows.append('import pandas as pd')
            elif line.strip().startswith(mapping_start):
                indent = ' ' * line.find(mapping_start)
                for code in code_rows:
                    script_rows.append(indent + code)
            elif line.strip().startswith(open_reader):
                indent = ' ' * line.find(open_reader)

                if file_type == 'parquet':
                    script_rows.append(indent + 'file = pd.read_parquet(fileName, engine="auto")')
                    script_rows.append(indent + 'reader = iter(file.to_dict(orient="records"))')
                elif file_type.startswith('json'):
                    script_rows.append(indent + f'input_file = open(file_name, "r", encoding="{encoding}")')
                else:
                    script_rows.append(indent + f'input_file = open(file_name, "r", encoding="{encoding}")')
                    script_rows.append(indent + f'csv_dialect = csv.Sniffer().sniff(input_file.read(2048), delimiters=[",", ";", "|", "\t"])')
                    script_rows.append(indent + f'file.seek(0)')
                    script_rows.append(indent + f'reader = csv.DictReader(input_file, dialect=csv_dialect)')
            elif line.strip().startswith(mapper_call) and file_type.startswith('json'):
                script_rows.append(line.replace('(row)', '(json.loads(row))'))
            elif line.strip().startswith(file_loop) and file_type.startswith('json'):
                script_rows.append(line.replace('reader', 'input_file'))
            elif line.strip().startswith(close_file):
                if file_type != 'parquet':
                    script_rows.append(line.replace(close_file, 'input_file.close()'))
            else:
                script_rows.append(line)

    return script_rows


def report_viewer(report):
    table_object = prettytable.PrettyTable()
    table_object.horizontal_char = '\u2500'
    table_object.vertical_char = '\u2502'
    table_object.junction_char = '\u253C'
    table_object.field_names = report[0]
    table_object.add_rows(report[1:])
    for column in report[0]:
        if any(column.endswith(x) for x in ['cnt', 'pct']):
            table_object.align[column] = 'r'
        else:
            table_object.align[column] = 'l'
    report_str = table_object.get_string()
    less = subprocess.Popen(["less", '-FMXSR'], stdin=subprocess.PIPE)
    try:
        less.stdin.write(report_str.encode('utf-8'))
        less.stdin.close()
        less.wait()
        print()
    except IOError as ex:
        print(f"\n{ex}\n")        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', help='the name of the input file')
    parser.add_argument('-t', '--file_type', help='"csv", "jsonl" or "parquet"')
    parser.add_argument('-e', '--encoding', default='utf-8', help='file encoding')
    parser.add_argument('-o', '--output_file', help='the name of the csv output file')
    parser.add_argument('-p', '--python_file_name', help='optional name of the python code file')
    args = parser.parse_args()

    if not args.input_file or not glob.glob(args.input_file):
        print('\nPlease supply a valid input file specification on the command line\n')
        sys.exit(1)
    file_list = glob.glob(args.input_file)

    if not args.file_type:
        ext = pathlib.Path(file_list[0]).suffix.lower() 
        if ext in ('.parquet', '.json', '.jsonl'):
            args.file_type = ext[1:]
        else: 
            args.file_type = 'csv'
    else:
        args.file_type = args.file_type.lower()

    if args.file_type.lower() == 'parquet' and not pd:
        print('\nPandas must be installed to analyze parquet files, try: pip3 install pandas\n')
        sys.exit(1)

    proc_start_time = time.time()
    shut_down = 0   
    analyzer = FileAnalyzer(args.input_file, args.file_type)

    try: 
        file_num = 0
        for file_name in file_list:
            file_num += 1
            print(f"reading file {file_num} of {len(file_list)}: {file_name}")
            if args.file_type == 'parquet':
                file = pd.read_parquet(fileName, engine='auto')
                reader = iter(file.to_dict(orient="records"))
            elif args.file_type.startswith('json'):
                file = open(file_name, 'r', encoding=args.encoding)
                reader = JsonReader(file)
            else:
                file = open(file_name, 'r', encoding=args.encoding)
                csv_dialect = csv.Sniffer().sniff(file.read(2048), delimiters=[',', ';', '|', '\t'])
                file.seek(0)
                reader = csv.DictReader(file, dialect=csv_dialect)
            for row in reader:
                analyzer.record_count += 1
                if analyzer.record_count % 10000 == 0:
                    print(f"{analyzer.record_count:,} rows read")
                analyzer.iterate_obj('root', row)

    except KeyboardInterrupt:
        shut_down = 9

    status = 'complete' if shut_down == 0 else 'interrupted' 
    print(f"\n{analyzer.record_count:,} rows read, file {status}\n")

    report_rows = analyzer.generate('report')
    if args.output_file:
        with open(args.output_file, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(report_rows)
        print(f"statistical report saved to {args.output_file}\n")
    elif prettytable:
        report_viewer(report_rows)

    if args.python_file_name:
        code_rows = analyzer.generate("code")
        script_rows = create_python_script(code_rows, args.file_type, args.encoding)
        with open(args.python_file_name, 'w') as file:
            file.write("\n".join(script_rows) + "\n")
        print(f"python code saved to {args.python_file_name}\n")

    sys.exit(shut_down)

