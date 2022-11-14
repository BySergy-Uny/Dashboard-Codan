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
    dataset_format = pd.DataFrame(columns=["entity", "product", "values", "date"])
    from data_format_json import json_format
    data_format = json_format
    for row in range(0, len(dataset)):
        template = data_format.copy()
        for colum in dataset:
            col = colum.lower()
            if ("quemador" in col) or ("id" in col):
                template['entity'] = str(dataset.iloc[row][colum])
            if("temp" in col):
                template['values']['temp'] = dataset.iloc[row][colum]
            if("tvoc" in col):
                template['values']['tvoc'] = dataset.iloc[row][colum]
            if("humedad" in col):
                template['values']['hum'] = dataset.iloc[row][colum]
            if("eco2" in col):
                template['values']['eco2'] = dataset.iloc[row][colum]
            if("time" in col):
                date = dataset.iloc[row][colum]
                try:
                    date_format = datetime.fromisoformat(date)
                except:
                    try:
                        date_format = datetime.fromtimestamp(date / 1000)
                    except:
                        break
                date_transform = datetime.strftime(date_format, "%Y-%m-%dT%H:%M:%S")
                template['date'] = str(date_transform)
            if("prod" in col):
                template['product'] = str(dataset.iloc[row][colum])
        dataset_format = pd.concat([dataset_format, pd.DataFrame(template)], ignore_index=True)
    print("[*] Finish Data Format")
    return dataset_format.to_json(orient="records")
    
def union_non_structure_files(paths):
    print("[+] Union files")
    files = []
    for path in paths:
        dataset = files_to_dataset(path)
        data_format_json = data_format(dataset)
        files +=  data_format_json
    print(data_format_json)
    print("[*] Finish Union files")

# union_non_structure_files(['./data/2022/'])

path = './data/2022/nodo2.csv'
dataset = file_to_dataset(path)
json_obj = data_format(dataset)
parsed = json.loads(json_obj)
json_finish = json.dumps(parsed, indent=4)
# data_format_json = data_format(dataset)

# Porque es mejor hacerlo desde la libreria de pandas la transformacion en vez de utilizar la function
# para la transformacion de dataset to format es porque lo realiza de manera mas optima y tardando menos 
# por lo tento lo que se necesita es la realizacion de una modificacion directa del dataset para su 
# posterior conversion