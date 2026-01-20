### Config 파일 처리
import json
import urllib3
urllib3.disable_warnings()
import warnings
warnings.filterwarnings(action='ignore')

def set_config(file_name):
    file_name=file_name if file_name.startswith('C:') else './'+file_name
    with open(file_name,encoding='utf-8') as file:
        config = json.load(file)
    return config
