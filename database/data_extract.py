import pandas as pd
import os
from datetime import datetime
import json

def file_to_dataset(path):
    if ("csv" in path.split(".")[-1]):
        read = pd.read_csv
    else:
        read = pd.read_excel
    dataset = read(path)
    cols_time = []
    for column in dataset:
        if("time" in column):
            cols_time.append(column)
    dataset = dataset.dropna(axis=0, subset=cols_time)
    print("[*] Finish File to database")
    return dataset

def files_to_dataset(path):
    print("[+] File to database")
    dirlist = [os.listdir(path)[0]]
    dataset = pd.DataFrame()
    for file in dirlist:
        if ("csv" in file.split(".")[-1]):
            read = pd.read_csv
        else:
            read = pd.read_excel
        if (dataset.empty):
            
            dataset = read(path + file)
        else:
            data_temp = read(path + file)
            dataset = pd.concat([dataset, data_temp])
    cols_time = []
    for column in dataset:
        if("time" in column):
            cols_time.append(column)
    dataset = dataset.dropna(axis=0, subset=cols_time)
    print("[*] Finish File to database")
    return dataset

def data_format(dataset):
    print("[+] Data Format")
    dataset_format = pd.DataFrame(columns=["entity","entity_type", "product", "values", "date"])
    dataset_values = pd.DataFrame()
    for colum in dataset:
        col = colum.lower()
        if ("quemador" in col):
            dataset_format['entity'] = dataset[colum].transform(lambda x: "Quemador-"+str(x))
            dataset_format['entity_type'] = "Quemador"
        if ("id" in col):
            dataset_format['entity'] = dataset[colum].transform(lambda x: "Nodo-"+x[-1])
            dataset_format['entity_type'] = "Nodo"
        if("temp" in col):
            dataset_values['temp'] = dataset[colum]
        if("tvoc" in col):
            dataset_values['tvoc'] = dataset[colum]
        if("humedad" in col):
            dataset_values['hum'] = dataset[colum]
        if("eco2" in col):
            dataset_values['eco2'] = dataset[colum]
        if("time" in col):
            try:
                dataset[colum] = dataset[colum].transform(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%dT%H:%M:%S'))
            except:
                dataset[colum] = dataset[colum].transform(lambda x: datetime.fromisoformat(x).strftime('%Y-%m-%dT%H:%M:%S'))
            dataset_format['date'] = dataset[colum]
        if("prod" in col):
            dataset_format['product'] = dataset[colum] 
    dataset_format['values'] = dataset_values.to_dict('records')
    print("[*] Finish Data Format")
    return dataset_format.to_json(orient="records")
    
def union_non_structure_files(paths):
    print("[+] Union files")
    files = []
    for path in paths:
        dataset = files_to_dataset(path)
        data_format_json = data_format(dataset)
        files +=  data_format_json
    parsed = json.loads(data_format_json)
    print(json.dumps(parsed[0:1], indent=4))
    print("[*] Finish Union files")

union_non_structure_files(['./data/2022/'])

# path = './data/2022/nodo2.csv'
# dataset = file_to_dataset(path)
# dataset_format = data_format(dataset)
# json_obj = dataset_format.to_json(orient="records")
# parsed = json.loads(json_obj)
# json_finish = json.dumps(parsed, indent=4)
# data_format_json = data_format(dataset)

# Porque es mejor hacerlo desde la libreria de pandas la transformacion en vez de utilizar la function
# para la transformacion de dataset to format es porque lo realiza de manera mas optima y tardando menos 
# por lo tento lo que se necesita es la realizacion de una modificacion directa del dataset para su 
# posterior conversion