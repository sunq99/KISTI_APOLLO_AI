### 데이터 처리
import module.data_connect as dc
import module.plus_func as pf
import config.path as path

conf=pf.set_config(path.conf_path)

def check_data(table_group, index, table_type):
    try:
        conn_data= dc.Data_connect(["DB","mysql"],conf,table_config=conf['table'][table_group])
        data_size=conn_data.lencheck_record(index,table_type)
    except Exception as e:
        print(e)
    finally:
        conn_data.close()
    return data_size

def read_data(table_group, index):
    try:
        conn_data= dc.Data_connect(["DB","mysql"],conf,table_config=conf['table'][table_group])
        result=conn_data.read_data(index)
    except Exception as e:
        result=e
    finally:
        conn_data.close()
    return result

def write_data(table_group, index, data):
    try:
        conn_data= dc.Data_connect(["DB","mysql"],conf,table_config=conf['table'][table_group])
        conn_data.write_data(data,index)
        result='done'
    except Exception as e:
        result=e
    finally:
        conn_data.close()
    return result

def delete_data(table_group, index):
    try:
        conn_data= dc.Data_connect(["DB","mysql"],conf,table_config=conf['table'][table_group])
        conn_data.delete_record(index)
        result='done'
    except Exception as e:
        result=e
    finally:
        conn_data.close()
    return result