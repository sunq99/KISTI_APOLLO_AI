### WIKI EDIT DUMP 처리(2_2.WIKI EDIT DOWNLOAD)
import pandas as pd
import csv,os,glob
from dask import dataframe as dd
import pathlib
import tempfile
import bz2
import warnings
import requests
from bs4 import BeautifulSoup
import datetime
import module.logger as logger

import module.proc_data as prd

# 모든 DeprecationWarning 무시
warnings.filterwarnings("ignore", category=DeprecationWarning)

monthlist=['01','02','03','04','05','06','07','08','09','10','11','12']

### edit 파일에 대한 유무 확인(추가 수집 필요여부 확인)
def check_edit_file(PATH):
    start_year=2001
    max_year=datetime.datetime.today().year
    pre_yearmonth_temp=str(datetime.datetime.today().month-1)
    pre_yearmonth=f'{max_year}-{'0'+pre_yearmonth_temp if len(pre_yearmonth_temp)==1 else pre_yearmonth_temp}'
    exist_file=os.listdir(PATH+'/output/yearly')
    result=[]
    for year_temp in range(start_year,max_year+1):
        year_file=str(year_temp)+'_edits.pickle'
        if year_file in exist_file:
            pass
        else:
            result.append(str(year_temp))
    yearmonth=[]
    for a in result:
        for b in monthlist:
            a_b=f'{a}-{b}'
            if len(glob.glob(f'{PATH}/org_data/????-??.enwiki.{a_b}.tsv.bz2'))==0 and len(glob.glob(f'{PATH}/output/????/{a_b}-?.parquet'))==0:
                yearmonth.append(f'{a}-{b}')
            if a_b==pre_yearmonth:
                break
    if len(yearmonth)>0:
        message=','.join(yearmonth)+'연월의 EDIT DUMP에 대한 수집이 필요합니다'
    elif len(result)>0:
        message='이미 수집된 연월이 있으나 연도 집계가 되지 않았습니다.'
    else:
        message='수집할 연월이 없습니다.'
    return message,yearmonth

### WIKI EDIT DUMP 다운로드
def download_file(log_container,log_name, url, folder_path):
    logger.log_writer(log_container,log_name,f"Downloading: {url}",reverse=True)
    try:
        response = requests.get(url)
        response.raise_for_status()  # HTTPError를 발생시킴
        file_name = url.split("/")[-1]
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logger.log_writer(log_container,log_name,f"Downloaded: {file_name}",reverse=True)
    except requests.RequestException as e:
        logger.log_writer(log_container,log_name,f"Failed to download {url}: {e}",reverse=True)

### WIKI EDIT DUMP 다운로드(UI 연결)
def download_files_from_year(log_container, log_name, base_url, target_folder, yearmonth):
    for target_month in yearmonth:
        try:
            response = requests.get(base_url)
            response.raise_for_status()  # HTTPError를 발생시킴
            soup = BeautifulSoup(response.text, 'html.parser')
            # Create target folder if it doesn't exist
            os.makedirs(target_folder, exist_ok=True)
            # Find all links in the webpage
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and href.endswith('.bz2'):
                    if '-' in target_month:
                        file_year = href.split('/')[-1].split('.')[2]
                    else:
                        file_year = int(href.split('/')[-1].split('.')[2].split('-')[0])  # Extract year from the filename
                    if file_year == target_month:  # Only download files from the specified year
                        full_url = os.path.join(base_url, href)
                        download_file(log_container, log_name, full_url, target_folder)
        except requests.RequestException as e:
            logger.log_writer(log_container,log_name,f"Error accessing {base_url}: {e}",reverse=True)
            return 0
    return 1

### WIKI EDIT DUMP 처리(필드 파일 불러오기)
def read_field(base_path,file_name):
    fields_file = pathlib.Path(base_path+"\\"+file_name)
    CSV_FIELDS = []
    CSV_FIELDS_META = {}
    with fields_file.open("r") as infile:
        reader = csv.reader(infile, delimiter="\t")
        # skip header
        next(reader)
        for line in reader:
            fclass = line[0]
            fname =line[1]
            dtype = line[2]
            comment = line[3]

            CSV_FIELDS.append(fname)

            if dtype == "int":
                dtype = "Int64"
            elif dtype == "bigint":
                dtype = "Int64"
            elif dtype == "array<string>":
                dtype = "object"

            if "timestamp" in fname:
                dtype = "object"

            CSV_FIELDS_META[fname] = {"class": fclass, "dtype": dtype, "comment": comment}
    timestamp_fields = [
        (id, field) for id, field in enumerate(CSV_FIELDS, start=1) if "timestamp" in field
    ]
    return CSV_FIELDS,CSV_FIELDS_META,timestamp_fields

### WIKI EDIT DUMP 처리(압축 풀기)
def decompress_bz2_file(file_path, tmpdir):
    decompressed_file_path = pathlib.Path(tmpdir, file_path.stem)
    with bz2.BZ2File(file_path, "rb") as file, decompressed_file_path.open(
        "wb"
    ) as new_file:
            # Copy the decompressed data to the new file
        for data in iter(lambda: file.read(100 * 1024), b""):
            new_file.write(data)
    print(f"  - decompressed {file_path} to {decompressed_file_path} {datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} ")
    return decompressed_file_path

def process_file(csv_file,result_fold,CSV_FIELDS,CSV_FIELDS_META,timestamp_fields):
    #임시 폴더 생성
    tmpdir = tempfile.TemporaryDirectory(prefix="mwhd-dask.")
    tmpdir_path = pathlib.Path(tmpdir.name)
    # 압축 해제
    if csv_file.suffix.endswith(".bz2"):
        csv_file= decompress_bz2_file(csv_file, tmpdir_path)
    file_name=str(csv_file).split('\\')[-1].split('.')[-2]
    fold_name=file_name.split('-')[0]
    org_data = dd.read_csv(
        csv_file,
        include_path_column=True,
        delimiter="\t",
        encoding="utf-8",
        quotechar='"',
        quoting=csv.QUOTE_NONE,
        header=None,
        names=CSV_FIELDS,
        dtype={field: CSV_FIELDS_META[field]["dtype"] for field in CSV_FIELDS},
    ) #date_format={field: "%Y-%m-%d %H:%M:%S.%f" for field in CSV_FIELDS if "timestamp" in field},
    for _, field in timestamp_fields:
        org_data[field] = dd.to_datetime(
            org_data[field], errors="coerce", format="%Y-%m-%d %H:%M:%S.%f"
        )
    for field in org_data.columns.tolist():
        if org_data.dtypes[field] == "boolean":
            org_data[field] = org_data[field].astype("boolean")
            org_data[field] = org_data[field].fillna(False)

    org_data["event_date"] = org_data["event_timestamp"].dt.date
    revision_data= org_data[(org_data["event_entity"]=="revision") & (org_data["event_type"]=="create")].copy()
    revision_data=revision_data[['event_date','page_id']].copy()

    __new_revisions = (
        revision_data
        .groupby("page_id")
        ["event_date"].count()
    ).reset_index()

    name_function = lambda x: f"{file_name}-{x}.parquet"
    __new_revisions.to_parquet(f'{result_fold}/{fold_name}/', name_function=name_function)
    
    tmpdir.cleanup()

### WIKI EDIT DUMP 처리(UI 연결)
def process_files_in_folder(log_container, log_name, folder_path):
    base_path=pathlib.Path(folder_path+'/org_data')
    csv_files = sorted([f for f in base_path.glob("*tsv.bz2")])
    CSV_FIELDS,CSV_FIELDS_META,timestamp_fields=read_field(folder_path,"data_fields.csv")
    all_count=len(csv_files)
    cnt=0
    result_path=pathlib.Path(folder_path+'/output')
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {all_count} files start",reverse=True)
    # 폴더 내의 모든 파일을 처리
    for csv_file in csv_files:
        process_file(csv_file,result_path,CSV_FIELDS,CSV_FIELDS_META,timestamp_fields)
        try:
            os.remove(csv_file)
        except Exception as e:
            logger.log_writer(log_container,log_name,f"{csv_file} 처리 중 오류 발생: {e}",reverse=True)
        cnt+=1
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {cnt} / {all_count} done",reverse=True)

### WIKI EDIT DUMP 연도별 처리(조건 확인)(UI 연결)
def check_yearly(edit_path):
    minyear=2001
    currnet_year=datetime.datetime.today().year
    yearlist=[year for year in range(minyear,currnet_year+1)]
    need_year=[]
    for y in yearlist:
        if len(glob.glob(f'{edit_path}/output/yearly/{y}_edits.pickle'))==0:
            need_year.append(y)
    for ny in need_year:
        if len(glob.glob(f'{edit_path}/output/{ny}/{ny}-??-?.parquet'))<12:
            need_year.remove(ny)
    return need_year
        
### WIKI EDIT DUMP 연도별 처리(UI 연결)
def yearly_edit(log_container, log_name, edit_path, year):
    try:
        in_path=f'{edit_path}/output'
        out_path=f'{in_path}/yearly'
        folder_path=os.path.join(in_path, str(year))
        parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
        yearly_edit=pd.DataFrame()
        for parquet_file in parquet_files:
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {parquet_file} file start",reverse=True)
            file_path = os.path.join(folder_path, parquet_file)
            df = pd.read_parquet(file_path)
            yearly_edit=pd.concat([yearly_edit,df[['page_id','event_date']]])
            yearly_edit=yearly_edit.groupby('page_id').sum().reset_index()
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {parquet_file} file done",reverse=True)
        yearly_edit.columns=['ID','EDITS']
        yearly_edit['YEAR']=str(year)
        yearly_edit.to_pickle(os.path.join(out_path, f'{year}_edits.pickle'))
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {year} done - {len(yearly_edit)} 건",reverse=True)
    except Exception as e:
        logger.log_writer(log_container,log_name,f"{year} 처리 중 오류 발생: {e}",reverse=True)

### WIKI EDIT DUMP 적재(UI 연결)
def make_editdb(log_container, log_name, edit_path):
    item_tb=prd.read_data('DUMP_EDIT', 0)
    if isinstance(item_tb, pd.DataFrame):
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 타입 확인 완료",reverse=True)
        item_tb=item_tb.drop_duplicates(subset='ID', keep='first')
        start_year=2015
        end_year=datetime.datetime.today().year-1
        try:
            prd.delete_data('DUMP_EDIT', 0)
            for y in range(start_year,end_year+1):
                yearly_edit=pd.read_pickle(os.path.join(f'{edit_path}/output/yearly',f'{y}_edits.pickle'))
                merged_df = pd.merge(yearly_edit, item_tb, on='ID', how='inner')
                logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {y}년 데이터 - {len(merged_df)} 건 적재 시작",reverse=True)
                merged_df=merged_df[['ID','TITLE','EDITS','YEAR']]
                message=prd.write_data('DUMP_EDIT',0,merged_df)
                logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {y}년 데이터 적재 완료",reverse=True)
        except Exception as e:
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 처리 중 오류 발생: {e}",reverse=True)