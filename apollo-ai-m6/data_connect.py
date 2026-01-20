import pymssql,pymysql
import re
import pandas as pd
import numpy as np
from datetime import datetime

class Data_connect: 
    def __init__(self,data_type,config,table_config=None,charset="utf8mb4"):
        self.data_config=config["data"]
        self.table_config=table_config
        self.data_main=data_type[0]
        self.data_sub=data_type[1]
        if self.data_main=="DB":
            self.server = self.data_config[self.data_sub]["server"]
            self.user = self.data_config[self.data_sub]["user"]
            self.password = self.data_config[self.data_sub]["password"]
            self.database= self.data_config[self.data_sub]["db"]
            self.port=self.data_config[self.data_sub]["port"]
            if self.data_sub.startswith("mysql") or self.data_sub.startswith("mariadb"):
                self.db = pymysql.connect(host=self.server, user=self.user, password=self.password, database=self.database, port=self.port,charset=charset,cursorclass=pymysql.cursors.DictCursor)
            elif self.data_sub.startswith("mssql"):     
                self.db = pymssql.connect(server=self.server,port=self.port,user=self.user,password=self.password,database=self.database,charset=charset,as_dict=True)
            self.cur = self.db.cursor()
                 
    def read_data(self,index=0): # 데이터 가져오기
        if self.data_main=="DB":
            query= self.table_config["query"][index] if len(self.table_config["query"][index])!=0 else "SELECT {} from {}" if len(self.table_config["source_column"][index])!=0 else "SELECT * from {}"
            if len(self.table_config["source_column"][index])==0:
                self.cur.execute(query.format(self.table_config["source_table"][index]))
            else: self.cur.execute(query.format(self.table_config["source_column"][index],self.table_config["source_table"][index]))
            read_data = self.cur.fetchall()
            column_names = [desc[0] for desc in self.cur.description]
            read_data = pd.DataFrame(read_data, columns=column_names)
            
        elif self.data_main=="FILE":
            base_dir=self.table_config["base_dir"] if type(self.table_config["base_dir"]) is str else self.table_config["base_dir"][0]
            if self.data_sub=="csv":
                read_data=pd.read_csv(base_dir+self.table_config["source_table"][index]+'.csv',encoding='utf-8' ,sep=",", quotechar='"')
            elif self.data_sub=="excel":
                read_data=pd.read_excel(base_dir+self.table_config["source_table"][index]+'.xlsx')
            if len(self.table_config["source_column"][index])!=0:
                read_data=read_data[self.table_config["source_column"][index]]
        else : 
            print("파일 타입을 다시 확인하세요")
            return 0
        return read_data
    
    def read_query_data(self,col_query,where_query,index=0): # 데이터 가져오기
        query= "SELECT "+col_query+" from {} "+where_query
        self.cur.execute(query.format(self.table_config["source_table"][index]))
        read_data = self.cur.fetchall()
        column_names = [desc[0] for desc in self.cur.description]
        read_data = pd.DataFrame(read_data, columns=column_names)
        return read_data

        
    def write_data(self,data,index=0,file_index=False,db_write_type="many",sheet_name=datetime.today().strftime('%Y%m%d'),excel_sheet="None"): # 데이터 내보내기 
        if self.data_main=="DB":
            data=list(data.itertuples(index=False))
            if db_write_type=="many":
                if self.table_config["target_table"][index]=='ERROR_LOG' :
                    sql ="INSERT IGNORE INTO {}({}) VALUES ({}){}".format(self.table_config["target_table"][index],self.table_config["target_column"][index],','.join(['%s']*(self.table_config["target_column"][index].count(',')+1)),self.table_config["update_condition"][index])
                else:
                    sql ="INSERT INTO {}({}) VALUES ({}){}".format(self.table_config["target_table"][index],self.table_config["target_column"][index],','.join(['%s']*(self.table_config["target_column"][index].count(',')+1)),self.table_config["update_condition"][index])
                self.cur.executemany(sql,data)
                self.cur.connection.commit()
            else:
                for one_data in data:
                    placeholders = "\'"+ "\',\'".join(map(str,one_data.values()))+"\'"
                    columns =', '.join(one_data.keys())
                    sql = "REPLACE INTO %s ( %s ) VALUES ( %s )" % (one_data, columns, placeholders)
                    self.cur.execute(sql)
                    self.cur.connection.commit()
        if self.data_main=="FILE":
            base_dir=self.table_config["base_dir"] if type(self.table_config["base_dir"]) is str else self.table_config["base_dir"][1]
            if len(self.table_config["target_column"][index])==0:
                data=data
            else : data[self.table_config["target_column"][index]]
            if self.data_sub=="csv":
                data.to_csv(base_dir+self.table_config["target_table"][index]+'.csv',encoding='utf-8' ,sep=",", index=file_index)
            elif self.data_sub=="excel":
                if excel_sheet=="None":
                    data.to_excel(base_dir+self.table_config["target_table"][index]+'.xlsx',sheet_name=sheet_name, index=file_index)
                else:
                    data.to_excel(excel_sheet,sheet_name=sheet_name, index=False)
            
    def delete_record(self,index=0): # 테이블 비우기
        if self.database=="":
             self.cur.execute("TRUNCATE TABLE %s" % (self.table_config["target_table"][index]))
        else:   
            self.cur.execute("TRUNCATE TABLE %s.%s" % (self.database, self.table_config["target_table"][index]))
        self.cur.connection.commit()     
   
        
    def lencheck_record(self,index=0,table_type="target"): # 테이블 row 수 확인
        table_type="target_table" if table_type=="target" else "source_table"
        self.cur.execute("SELECT count(0) from %s" % self.table_config[table_type][index])
        return self.cur.fetchall()[0][0]

    def remove_imo(self,text): #이모지
        ignore = re.compile('[\n\r\t\xa0\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U0001F90D-\U0001F9FF\u202f]')
        return ignore.sub(' ', text).strip()
    
    def close(self): #db.close
        if self.data_main=="DB":
            self.db.close()    