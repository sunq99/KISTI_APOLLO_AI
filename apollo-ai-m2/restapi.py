#-*- coding:utf-8 -*-

import json
import requests

class RestApi(object):

    url = None
    headers = None

    # ==============================================================================#
    # Constructor                                                                   #
    # ==============================================================================#
    def __init__(self, url):
        self.url = url
        self.headers = {'Content-type': 'application/json'}

    # ==============================================================================#
    # setUrl                                                                        #
    # ==============================================================================#
    def setUrl(self, url):
        self.url = url

    # ==============================================================================#
    # getUrl                                                                        #
    # ==============================================================================#
    def getUrl(self):
        return self.url

    # ==============================================================================#
    # setHeaders                                                                    #
    # ==============================================================================#
    def setHeaders(self, headers):
        self.headers = headers

    # ==============================================================================#
    # getHeaders                                                                    #
    # ==============================================================================#
    def getHeaders(self):
        return self.headers

    # ==============================================================================#
    # query                                                                         #
    # ==============================================================================#
    def query(self, sentence, flag):
        data = {"sentence": sentence, "flag": flag}
        data_json = json.dumps(data)
        response = requests.post(self.url, data=data_json.encode(), headers=self.headers)
        return response.json()

    # ==============================================================================#
    # predict                                                                       #
    # ==============================================================================#
    def predict(self, data):
        print(data)
        data_json = json.dumps(data)
        response = requests.post(self.url, data=data_json.encode(), headers=self.headers)
        return response.json()

