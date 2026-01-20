### Data Connect
import pymssql,pymysql
import re
import pandas as pd

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
            else: 
                self.cur.execute(query.format(self.table_config["source_column"][index],self.table_config["source_table"][index]))
            read_data=pd.DataFrame(self.cur.fetchall())
        else : 
            print("파일 타입을 다시 확인하세요")
            return 0
        return read_data
    
    def read_query_data(self,col_query,where_query,index=0): # 데이터 가져오기
        query= "SELECT "+col_query+" from {} "+where_query
        self.cur.execute(query.format(self.table_config["source_table"][index]))
        read_data=pd.DataFrame(self.cur.fetchall())
        return read_data
    
    def write_data(self,data,index=0,db_write_type="many"): # 데이터 내보내기 
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
            
    def delete_record(self,index=0): # 테이블 비우기
        if self.database=="" or "." in self.table_config["target_table"][index]:
             self.cur.execute("TRUNCATE TABLE %s" % (self.table_config["target_table"][index]))
        else:   
            self.cur.execute("TRUNCATE TABLE %s.%s" % (self.database, self.table_config["target_table"][index]))
        self.cur.connection.commit()     
   
    def lencheck_record(self,index=0,table_type="target"): # 테이블 row 수 확인
        table_type="target_table" if table_type=="target" else "source_table"
        self.cur.execute("SELECT count(0) from %s" % self.table_config[table_type][index])
        return self.cur.fetchall()[0]['count(0)']

    def remove_imo(self,text): #이모지
        ignore = re.compile('[\n\r\t\xa0\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U0001F90D-\U0001F9FF\u202f]')
        return ignore.sub(' ', text).strip()
    
    def close(self): #db.close
        if self.data_main=="DB":
            self.db.close()    