import os
from dotenv import load_dotenv

env_name = os.environ.get("APP_ENV")
load_dotenv(dotenv_path=f'.env.{env_name}' if env_name else '.env')

from show_wiki import *
from graph_data import *

from typing import Union
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel
from starlette.responses import JSONResponse
import uvicorn
import pandas as pd

import json
import time

origins = ["*"]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class IndicatorKeyword(BaseModel):
    keyword : str
    indicator : str # ("PAGERANK":"기술집약도","PAGEVIEWS":"수요부상성","EPV":"공급부상성")
    top_n : int
    n_cnt : int

class PreviewKeyword(BaseModel):
    keyword : str
    node_cnt : int

class SearchItem(BaseModel):
    query_type: str
    query: str
    top_n :int

@app.get('/')
def read_root():
    return {'staus': 'Success'}
    
    
@app.post('/api/model6/v1/indicator')
def model_predict(item : IndicatorKeyword) :
    print("Network Chart v1, Indicators based")
    start = time.time()

    dicted_item = dict(item)
    keyword = dicted_item['keyword']
    indicator = dicted_item['indicator']
    top_n = dicted_item['top_n']
    n_cnt = dicted_item['n_cnt']
    
    print(f'keyword: {keyword}, indicator: {indicator}, top_n: {top_n}')
    indicatorlist=["PAGERANK","PAGEVIEWS","EPV"]
    if indicator in indicatorlist:
        result = graph_indicator_data(keyword=keyword, indicator=indicator, top_n=top_n, n_cnt=n_cnt)
        result_dict = {}
    
        if result == None:
            result_dict['message'] = {'IndicatorError' : 'Not exists Indicator'}
        elif result[0] == False:
            result_dict['message'] = {'IndicatorError' : 'Not exists Indicator'}
        elif result[0] == True :
            key_name, edges, category_dict, final_df ,history_df = result[1], result[2], result[3] ,result[4], result[5]
            result_dict['chart_1'] = convert_data(keyword, key_name, edges, category_dict)
            result_dict['history'] = history_df.to_dict()
    else:
        result_dict['message'] = {'IndicatorError' : 'Not exists Indicator'}
    end = time.time()
    print(time.strftime('%Y-%m-%d %I:%M:%S %p', time.localtime(start)), '~' , time.strftime('%Y-%m-%d %I:%M:%S %p', time.localtime(end)))

    # return data
    return JSONResponse(result_dict)

@app.post('/api/model6/v1/preview')
def model_predict(item : PreviewKeyword) :
    print("Network Chart v1, Preview Network")
    start = time.time()

    dicted_item = dict(item)
    keyword = dicted_item['keyword']
    node_cnt = dicted_item['node_cnt']
    
    print(f'keyword: {keyword}, max_node: {node_cnt}')
    
    result = graph_preview_data(keyword=keyword, node_cnt=node_cnt)
    result_dict = {}

    if result == None:
        result_dict['chart_4'] = {'Preview Error' : 'Not exists Preview'}
    elif result[0] == False:
        result_dict['chart_3'] = {'Preview Error' : 'Not exists Preview'}
    elif result[0] == True :
        key_name, edges, category_dict, final_df = result[1], result[2], result[3] ,result[4]
        result_dict['chart_1'] = convert_data(keyword, key_name, edges, category_dict)
        result_dict['chart_2'] = final_df.to_dict()

    end = time.time()
    print(time.strftime('%Y-%m-%d %I:%M:%S %p', time.localtime(start)), '~' , time.strftime('%Y-%m-%d %I:%M:%S %p', time.localtime(end)))

    # return data
    return JSONResponse(result_dict)


@app.post('/api/model6/v1/itemsearch')
def model_itemsearch(item : SearchItem) :
    print("Search item : embedding based")
    start = time.time()

    dicted_item = dict(item)
    query_type = dicted_item['query_type']
    query = dicted_item['query']
    n_cnt = dicted_item['top_n']
    
    print(f'query_type: {query_type}, query: {query}, n_cnt: {n_cnt}')
    
    result = item_list_data(query_type=query_type, query=query,  n_cnt=n_cnt)
    result_dict = {}

    if result == None:
        result_dict['message'] = {'Search Item error' : 'Not exists Item list'}
    elif result[0] == False:
        result_dict['message'] = {'Search Item error' : 'Not exists Item list'}
    elif result[0] == True :
        print(result[1].columns)
        result_dict['result'] = result[1].to_dict()
    end = time.time()
    print(time.strftime('%Y-%m-%d %I:%M:%S %p', time.localtime(start)), '~' , time.strftime('%Y-%m-%d %I:%M:%S %p', time.localtime(end)))

    # return data
    return JSONResponse(result_dict)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        reload=True
    )