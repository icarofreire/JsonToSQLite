import os
import json
import sqlite3

"""
importar dados de arquivo json para um banco de dados SQLite;

import data from json file into SQLite database;
"""

def check_json_type(data):
    if isinstance(data, dict):
        if all(isinstance(value, (str, int, float, bool, type(None))) for value in data.values()):
            return "Key-Value Pair JSON"
        else:
            return "Nested JSON Object"
    elif isinstance(data, list):
        if all(isinstance(item, dict) for item in data):
            return "JSON Array"
        elif all(isinstance(item, list) for item in data):
            return "JSON Array of Arrays"
        else:
            return "JSON Array of Values"
    else:
        return "Unknown JSON Type"

def create_table(c, table_name, columns):
    columns_definition = ', '.join([f"{col} TEXT" for col in columns])
    create_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})"
    c.execute(create_statement)

def add_colum(c, table_name, colum_name):
    create_statement = f"ALTER TABLE {table_name} ADD COLUMN {colum_name} TEXT"
    c.execute(create_statement)

def inser_into(c, table_name, data):
    if isinstance(data, dict):
        columns = data.keys()
        values = data.values()
        create_table(c, table_name, columns)
        c.execute(f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})", list(values))

def create_table_obj(c, table_name, data):
    if isinstance(data, dict):
        columns = data.keys()
        create_table(c, table_name, columns)
        rows = consulta(c, table_name, data)
        if rows == None: c.execute(f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})", list(data.values()))

def join_inner(delim, lista):
    return delim.join([x for i, x in enumerate(lista) if i > 0 and i < len(lista)-1 ])

def quote_strs(lis):
    ll = []
    for x in lis:
        if isinstance(x, str):
            quo = "'" + x + "'"
            ll.append(quo)
        else:
            ll.append(x)
    return ll

def consulta(c, table_name, campo_valor):
    try:
        where_query = " and ".join("=".join((str(k),"?")) for k,v in campo_valor.items())
        query = f"SELECT * FROM {table_name} WHERE {where_query}"
        values = quote_strs(campo_valor.values())

        c.execute(query, values)
        rows = c.fetchall()
        return rows
    except sqlite3.OperationalError as e:
        print(e)
        return None

def insert_into_db(json_type, data, file_database):
    conn = sqlite3.connect(file_database)
    c = conn.cursor()

    if json_type == "Key-Value Pair JSON":
        table_name = 'kv_pairs_dynamic'
        columns = data.keys()
        create_table(c, table_name, columns)
        c.execute(f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})", list(data.values()))

    elif json_type == "Nested JSON Object":
        dic_geral = {}
        for key, value in data.items():
            if isinstance(value, dict):
                table_name = f"{key}_nested"
                create_table_obj(c, table_name, value)

                first_key = next(iter(value))
                dic_geral[key] = value[first_key]
            else:
                dic_geral[key] = value

        table_name = 'kv_pairs_dynamic'
        columns = dic_geral.keys()
        values = dic_geral.values()
        values = ['' if x is None else x for x in values]
        # print( dic_geral )
        create_table(c, table_name, columns)
        rows = consulta(c, table_name, dict(zip(columns, values)) )
        if rows == None: c.execute(f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})", values)

    elif json_type == "JSON Array":
        table_name = 'json_array_dynamic'
        columns = data[0].keys()  # Assuming all dicts in the array have the same keys
        create_table(c, table_name, columns)
        for item in data:
            c.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})", list(item.values()))

    elif json_type == "JSON Array of Arrays":
        table_name = 'json_array_of_arrays_dynamic'
        num_columns = max(len(sublist) for sublist in data)
        columns = [f"col_{i+1}" for i in range(num_columns)]
        create_table(c, table_name, columns)
        for sublist in data:
            # Fill the rest with None if sublist is shorter
            sublist += [None] * (num_columns - len(sublist))
            c.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * num_columns)})", sublist)

    elif json_type == "JSON Array of Values":
        table_name = 'json_array_of_values_dynamic'
        create_table(c, table_name, ['value'])
        for value in data:
            c.execute(f"INSERT INTO {table_name} (value) VALUES (?)", (value,))

    conn.commit()
    conn.close()
    print("Data has been inserted into the database.")

def lines_file(fname: str):
    if os.path.isfile(fname):
        lines = open(fname).read().splitlines()
        # for i, l in enumerate(lines): lines[i] = l[:-1]
        return lines
    return None

def importar(file_database: str, file_json: str):
    json_lines = lines_file(file_json)
    json_lines = [''.join(json_lines)]

    for json_string in json_lines:
        if json_string.find('Internal Server Error') == -1:
            data = json.loads(json_string.replace('\n\r',''))
            json_type = check_json_type(data)
            print(f"JSON Type Identified: '{json_type}'")
            insert_into_db(json_type, data, file_database)

def testes(file_database: str):
    print('*** testes ***')
    # Example JSON strings to test the script
    # json_examples = [
    # #     # '{"key1": "value1", "key2": "value2"}',
    #     # '{"person": {"name": "John", "age": 30}, "teste": {"id": "1234", "etinia": {"id": "9999", "nomeEtnia": "NÃO SE APLICA"} }, "city": "New York"}',
    # #     # '[{"name": "John"}, {"name": "Jane"}]',
    # #     # '[["John", 30], ["Jane", 25]]',
    # #     # '["John", "Jane", "Doe"]'
    # ]
    # json_examples = lines_file('objts-2')
    # json_examples = lines_file('teste-uni-obj.json')
    # json_one_line = ''.join(json_examples)
    # json_lines = [json_one_line]
    # # json_examples = '{' + json_examples.strip() + '}'
    # file_database = 'paci.db'

# Defining main function
def main():
    print("hey there")
    importar('paci.db', 'teste-uni-obj.json')
    # testes('paci.db')

if __name__ == "__main__":
    main()