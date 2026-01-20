### WIKI EDIT DUMP 처리(1_1.WIKI DUMP DOWNLOAD)
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import bz2
import xml.etree.ElementTree as ET
import re
import module.data_connect as data_connect
import module.plus_func as pf
import time
import datetime
import pandas as pd
import module.clean as clean
import module.logger as logger
import requests
from bs4 import BeautifulSoup

### WIKI ITEM 다운로드 리스트 생성(UI 연결)
def make_file_list(TEST_YN):
    url_repfix = "https://dumps.wikimedia.org/enwiki/latest/"
    response = requests.get(url_repfix)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    urls_with_multistream = [a['href'] for a in soup.find_all('a', href=True, string=re.compile('multistream')) if not a['href'].endswith('.xml')]
    # 테스트 체크 시 일부만 실행
    if TEST_YN:
        urls_with_multistream=urls_with_multistream[:2]
        test_urls_with_multistream=[a.replace('-index','').replace('.txt','.xml') for a in urls_with_multistream]
        urls_with_multistream+=test_urls_with_multistream
    return urls_with_multistream

#### WIKI ITEM DUMP 다운로드(UI 연결)
def download_item_file(log_container,log_name,progress_container,PATH,file_list):
    # URL of the page
    url_repfix = "https://dumps.wikimedia.org/enwiki/latest/"
    total_files = len(file_list)
    for ind, file in enumerate(file_list):
        if file.endswith('txt.bz2') or file.endswith('xml.bz2') : # too big...........
            continue
        else:
            # 다운로드 시작 로그
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : ⬇️ [{ind+1}/{total_files}] 다운로드 시작: {file}", reverse=True)
            url = url_repfix + file
            print(f'** Ind : {ind}, URL : {url}')
            file_dir= f'{PATH}/INDEX' if 'index' in file else f'{PATH}/XML'
            response = requests.get(url, stream=True)
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                bytes_downloaded = 0
                with open(file_dir+"/"+file, 'wb') as f:
                    for data in response.iter_content(chunk_size=1024):
                        bytes_downloaded += len(data)
                        ratio = bytes_downloaded / total_size
                        logger.progress_writer(progress_container,ratio,f"{file} 다운로드 중... ({ratio*100:.1f}%)")
                        f.write(data)
                logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : ✅ 완료: {file} ({total_size / 1024 / 1024:.1f} MB)", reverse=True)
            else:
                logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : ❌ 실패: {file}", reverse=True)
    return True

### WIKI ITEM 파일 처리(INDEX 정보 처리)
def get_index(path,index_file):
    ind_fd=bz2.open(path+"/"+index_file)
    ind_data=ind_fd.read()
    ind_list_tmp=ind_data.split(b"\n")
    ind_list_tmp = list(filter(None, ind_list_tmp))
    ind_list=list(dict.fromkeys([int(_.split(b":")[0]) for _ in ind_list_tmp]))
    return ind_list

### WIKI ITEM 파일 처리(텍스트 처리)
def clean_text(text):
    text= re.sub(r'( |)\<\!\-\-.{5,10000}?\-\-\>','',text)
    text= re.sub(r'( |)\|\-(\n|\r|\r\n)','|-',text) #줄바꿈 제거
    text= re.sub(r'(\n|\r|\r\n)( |)\|\-','|-',text) #줄바꿈 제거
    text= re.sub(r'(\n|\r|\r\n)( |)\!','!',text) #줄바꿈 제거
    text= re.sub(r'[\n|\r|\r\n]{1,3}( |)\|','|',text) #줄바꿈 제거
    text= re.sub(r'( |)\|[\n|\r|\r\n]{1,3}','|',text) #줄바꿈 제거
    text= re.sub(r'(\n|\r|\r\n)\}\}','}}',text) #줄바꿈 제거
    text= re.sub(r'[\n|\r|\r\n]{1,3}\*.{5,1000}(?=\}\})','*',text) #줄바꿈 제거
    text= re.sub(r'(<ref.{0,100}>.{5,1000}?</ref>|<ref((?!/>).){0,100}/>)','',text)
    text= re.sub(r'(?<!(\.|\}))\{\{.{5,10000}?\}\}(\n|\r|\r\n)','',text)
    text= re.sub(r'\{\{.{5,10000}?\}\}(?!(\)|\,|\|))','',text)
    text= re.sub(r'\{\|.{5,10000}\|\}(\n|\r|\r\n)','',text)
    text= re.sub(r'\[\[(File|Image)\:.{5,10000}?\]\](?!\,)','',text)
    text = text.replace("'''","")
    return text.strip()

### WIKI ITEM 파일 처리(텍스트 처리)
def fillter_item(dataframe):
    dataframe=dataframe.loc[~dataframe["title"].str.startswith("File:")]
    dataframe=dataframe.loc[~dataframe["title"].str.startswith("Category:")]
    dataframe=dataframe.loc[~dataframe["title"].str.startswith("Template:")]
    dataframe=dataframe.loc[~((dataframe["title"].str.startswith("Wikipedia:"))|(dataframe["title"].str.startswith("MediaWiki:")))]
    return dataframe

### WIKI ITEM 파일 처리(텍스트 처리)
def get_splitrow(org_df,split_df,name):
    split_df=split_df.apply(lambda x: pd.Series(x))
    split_df=split_df.stack().reset_index(level=1, drop=True).to_frame(name)
    result=org_df.merge(split_df, left_index=True, right_index=True, how='left')
    result=result.where(pd.notnull(result),None)
    return result

### WIKI ITEM 파일 처리(분류 및 적재)
def get_item(conf,file_name,dump_name,cnt):
    tree=ET.parse(file_name)             
    xroot= tree.getroot()
    df_cols=["title","id","text_temp","redirect","category","seealso","text"]
    remove_seealso=set(['div col','col div end','div col end','Div col','DIV','disambiguation'])
    category_pattern = re.compile(r"(?<=\[\[Category:).{1,50}?(?=\]\])")
    seealso_pattern = re.compile(r"(?<=\[\[).{1,100}?(?=\]\])")
    page_list=xroot.findall('page')
    if len(page_list)==0:
        return 0
    row=[]
    for node in page_list:
        title=node.find("title").text
        id=node.find("id").text
        redirect=node.find("redirect")
        redirect= redirect.get("title") if redirect != None else None
        for revision_node in node.find("revision"):
            if revision_node.tag=='text':
                text_temp=revision_node.text
                if text_temp is not None:
                    category_pattern = re.compile(r"(?<=\[\[Category:)[^\|]{1,50}?(?=\]\])|(?<=\[\[Category:)[^\]]{1,50}?(?=\|)")
                    category="|".join(category_pattern.findall(text_temp))
                    seealso_index=list(set([text_temp.find("== See also =="),text_temp.find("==See also==")]))
                    reference_index=list(set([text_temp.find("== References =="),text_temp.find("==References=="),text_temp.find("== Explanatory notes =="),text_temp.find("==Explanatory notes=="),text_temp.find("== External links =="),text_temp.find("==External links=="),text_temp.find("== Notes =="),text_temp.find("==Notes==")]))
                    if len(reference_index)>1:
                        reference_index.remove(-1)
                    seealso=None if len(seealso_index)==1 and seealso_index[0]==-1 else text_temp[max(seealso_index):] if len(reference_index)==1 and reference_index[0]==-1 else text_temp[max(seealso_index):min(reference_index)]
                    seealso=None if seealso==None else seealso_pattern.findall(seealso)
                    if seealso!=None:
                        seealso=list(set([None if ((_.lower().startswith('portal') or  _.lower().startswith('intitle') or _.lower().startswith('surname') )and _.find('|'))  or _.lower().endswith('|given name') else _.split("|")[0] for _ in seealso])-remove_seealso)
                        seealso=list(filter(None, seealso))
                        seealso="|".join(seealso)
                        seealso=None if len(seealso)==0 else seealso
                    index=seealso_index+reference_index
                    index=[item for item in index if item != -1]
                    text=text_temp[:min(index)] if len(index)>0 else text_temp
                else : category=seealso=text=None

        row.append({"title":title, 'id':id, 'text_temp':text_temp,'redirect':redirect, 'category':category, 'seealso': seealso, 'text':text})
            
    out_df= pd.DataFrame(row,columns=df_cols)
    out_df = fillter_item(out_df)
    out_df["dump_name"]=dump_name
    if len(out_df)>0:
        #redirect 
        out_df_redirect=out_df.loc[(out_df["redirect"].notnull())]
        out_df=out_df.loc[(out_df["redirect"].isnull())]
        #section
        out_df['text']=out_df["text"].apply(clean.clean)
        split_section_1=get_splitrow(out_df["id"].to_frame(),out_df.text.str.split("SP_Sec_L1"),"section")
        split_section_1['section1']=split_section_1["section"].apply(lambda x: re.split(r"(?<!\=)\=\=\n",x)[0] if re.search(r"(?<!\=)\=\=\n",x) != None else None)
        split_section_1['section']=split_section_1["section"].apply(lambda x: re.split(r"(?<!\=)\=\=\n",x)[1] if re.search(r"(?<!\=)\=\=\n",x) != None else x)
        split_section_1=split_section_1.reset_index()
        
        split_section_2=get_splitrow(split_section_1[["id","section1"]],split_section_1.section.str.split("SP_Sec_L2"),"section")
        split_section_2['section2']=split_section_2["section"].apply(lambda x: re.split(r"(?<!\=)\=\=\=\n",x)[0] if re.search(r"(?<!\=)\=\=\=\n",x) != None else None)
        split_section_2['section']=split_section_2["section"].apply(lambda x: re.split(r"(?<!\=)\=\=\=\n",x)[1] if re.search(r"(?<!\=)\=\=\=\n",x) != None else x)
        split_section_2['section']=split_section_2["section"].apply(lambda x: re.sub(r'\s+', ' ', x.replace("\n","")))
        
        # 각 ID 그룹별로 첫 번째 행의 인덱스를 찾기
        first_rows = split_section_2.groupby('id').head(1).index.to_list()
        null_indices = split_section_2[split_section_2['section1'].isnull()].index.to_list()  
        intersection = set(null_indices).intersection(set(first_rows))
        
        # 해당 인덱스의 Section을 'Summary_AI'로 설정
        split_section_2.loc[list(intersection), 'section1'] = 'Summary_AI'
        
        #category
        out_df_category=out_df[["id","category"]]
        out_df_category=out_df_category.loc[~(out_df_category["category"].isnull())]
        split_category=out_df_category.category.str.split("|")

        #seealso
        out_df_seealso=out_df[["id","seealso"]]
        out_df_seealso=out_df_seealso.loc[~(out_df_seealso["seealso"].isnull())]
        split_seealso=out_df_seealso.seealso.str.split("|")

        ##DB 반영
        try:
            dump_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=conf['table']['WIKIPEDIA'])
            if cnt==1:
                #print("테이블 값을 모두 초기화 합니다.")
                dump_connect.delete_record(index=0)
                dump_connect.delete_record(index=1)
                dump_connect.delete_record(index=2)
                dump_connect.delete_record(index=3)
                dump_connect.delete_record(index=4)
            
            if len(out_df)>0:
                dump_connect.write_data(out_df[["id","title","dump_name"]],index=0)
            if len(out_df_redirect)>0:
                dump_connect.write_data(out_df_redirect[["id","title","redirect","dump_name"]],index=2)
            if len(split_category)>0:
                dump_connect.write_data(get_splitrow(out_df_category["id"].to_frame(),split_category,"category"),index=1)
            if len(split_seealso)>0:
                seealso_df=get_splitrow(out_df_seealso["id"].to_frame(),split_seealso,"seealso")
                seealso_df=seealso_df.loc[~(seealso_df["seealso"].str.upper().str.startswith("WP:SEEALSO"))]
                dump_connect.write_data(seealso_df,index=3)
            if len(split_section_2)>0:
                dump_connect.write_data(split_section_2[["id","section1","section2","section"]],index=4)    
        except Exception as e:
            print("error 발생: ", e)
            print("{} - error 발생".format(dump_name))
        finally:
            dump_connect.close()

#### WIKI ITEM DUMP 처리(UI 연결)
def enwiki_parse(conf_name,log_container,log_name,progress_container,check_test):
    conf=pf.set_config(conf_name)
    start_time=time.time()
    xml_path=conf["table"]["WIKIPEDIA"]["xml_path"]
    ind_path=conf["table"]["WIKIPEDIA"]["index_path"]
    xml_save_file=xml_path+conf["table"]["WIKIPEDIA"]["save_file"]
    ind_list_tmp = os.listdir(ind_path)
    file_list_ind = [file for file in ind_list_tmp if file.endswith(".bz2")]
    cnt=conf["table"]["WIKIPEDIA"]["cnt"]
    for file in file_list_ind:
        ind_list=get_index(ind_path,file)
        ind_list_len=len(ind_list) if check_test==False else 2
        dump_name=file.replace("-index","").replace(".txt",".xml")
        xml_file=xml_path+"/"+dump_name
        xml_fd = open(xml_file,'rb')
        xml_text=""
        logger.log_writer(log_container,log_name,f'{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Processing : {dump_name}',reverse=True)
        for ind in range(0,ind_list_len):
            xml_fd.seek(ind_list[ind]) #1번인덱스
            unzipper = bz2.BZ2Decompressor()
            if ind==ind_list_len-1:
                block = xml_fd.read()
            else :
                block = xml_fd.read(ind_list[ind+1]) #2번 인덱스
            xml_text = xml_text+unzipper.decompress(block).decode()
            if (ind//20>0 and ind%20==0) or ind==ind_list_len-1:
                xml_text ="<root>\n"+xml_text+"</root>"
                with open(xml_save_file, "w", encoding="utf-8") as file:
                    file.write(xml_text)
                get_item(conf,xml_save_file,dump_name,cnt)
                xml_text=""
                cnt+=1
                ratio = ind/(ind_list_len-1)
                logger.progress_writer(progress_container,ratio,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {dump_name} 파일 작업 중...({ratio*100:.1f}%)")
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : ✅ 완료: {dump_name} -  {ind}/{ind_list_len-1}",reverse=True)
    print("소요시간 : {}".format(time.time()-start_time))
