import json,os
import warnings
warnings.filterwarnings(action='ignore')
import datetime
import pandas as pd


def set_config(file_name):
    with open('./{}'.format(file_name),encoding='utf-8') as file:
        config = json.load(file)
    return config

# 에러 발생 원인 찾기
def log_error(error_file,Ture_title, node, ex):
    error_dir = './'
    error_message = '{} 시드의 {} 에서 에러 발생 : {} '.format(Ture_title, node, ex)
    with open(error_dir + error_file, 'a', encoding='utf-8-sig') as f:
        f.write('{}||{}||{}\n'.format(Ture_title, node, ex))
    print(error_message)

def text_to_df(file_name,column,process_name):
    with open(file_name, 'r', encoding='utf-8-sig') as f:
        data = f.readlines()
    data_split = [x.replace('\n','').split('||') for x in data]
    df=pd.DataFrame(data_split,columns=column).drop_duplicates()
    df.insert(1,'PROCESS',process_name)
    os.remove(file_name)
    return df

# 시간 확인용
def time_check(process_name):
    dir = './'
    file = 'log.txt'
    with open(dir + file, 'a', encoding='utf-8-sig') as f:
        f.write('{}  ||  {} \n'.format(datetime.datetime.now(), process_name))