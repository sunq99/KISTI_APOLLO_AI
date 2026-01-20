import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from config import *
import time
import os

# SQLAlchemy 엔진을 생성합니다. pool_pre_ping=True는 연결이 유효한지 미리 확인하여 끊어진 연결을 자동으로 재연결합니다.
engine = create_engine(f'mysql+pymysql://{DB_INFO["USER"]}@{DB_INFO["HOST"]}:{DB_INFO["PORT"]}/{DB_INFO["NAME"]}',
                       connect_args={'password': DB_INFO["PASSWORD"]},
                       pool_size=30,
                       max_overflow=10,
                       pool_timeout=30,
                       pool_recycle=3600,
                       pool_pre_ping=True)

# scoped_session은 쓰레드 안전한 세션을 제공합니다.
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def get_db_session():
    return Session()

# ======================================================================
# 수정일시 : 2025-08-21
# 수정부분 : add_json_data.add_comp_data 함수
# 수정사유 : 기존 add_comp_data 함수는 SQL 쿼리에서 UNION ALL을 사용하여 각 기업의 매출액을 조회하는 방식
#            -> 성능이 저하 및 nan 값이 발생할 경우 테이블이 밀려서 조회가 되지 않는 문제 발생
#            -> 개선된 add_comp_data 함수는 단일 쿼리로 모든 기업의 정보를 조회하여 성능을 개선
# 수정자 : 오픈메이트 박승제 책임
# ======================================================================
def add_comp_data(proj2comp: dict):
    """
    프로젝트-기업 매칭 결과에 기업 상세 정보를 추가하는 함수
    매출액은 최신 년도 기준으로 가져옴
    """
    sst = time.time()
    
    name_dict = {
        'NICE_A_F_004': "한글업체명",
        'NICE_A_F_023': '설립일',
        'NICE_A_F_051': '시.도',
        'NICE_B_F_009': '매출액',
    }
    
    db = get_db_session()
    try:
        companies = list(proj2comp['company'].values())
        
        if not companies:
            print('No companies to process')
            return proj2comp
        
        company_str = ','.join(f'"{c}"' for c in companies)
        
        # 최신 년도(NICE_B_F_008) 기준으로 매출액 조회
        sql = f'''
        SELECT 
            a.NICE_A_F_001 as company_code,
            a.NICE_A_F_004,
            a.NICE_A_F_023,
            a.NICE_A_F_051,
            IFNULL(
                (SELECT b.NICE_B_F_009
                 FROM vc_nice_b_tb_0002 b
                 WHERE b.NICE_B_F_001 = a.NICE_A_F_001
                 ORDER BY b.NICE_B_F_008 DESC  -- 최신 년도 우선
                 LIMIT 1), 
                NULL
            ) as NICE_B_F_009
        FROM vc_nice_a_tb_0001_asti_spcl a
        WHERE a.NICE_A_F_001 IN ({company_str})
        ORDER BY FIELD(a.NICE_A_F_001, {company_str})
        '''
        
        conn = db.connection()
        df_all = pd.read_sql(sql, conn)
        
        # 누락된 기업 처리
        if len(df_all) != len(companies):
            found_companies = set(df_all['company_code'].values)
            missing_companies = [c for c in companies if c not in found_companies]
            
            missing_data = []
            for company_code in missing_companies:
                missing_data.append({
                    'company_code': company_code,
                    'NICE_A_F_004': None,
                    'NICE_A_F_023': None,
                    'NICE_A_F_051': None,
                    'NICE_B_F_009': None
                })
            
            if missing_data:
                df_missing = pd.DataFrame(missing_data)
                df_all = pd.concat([df_all, df_missing], ignore_index=True)
        
        # 원래 순서대로 재정렬
        df_all['sort_order'] = df_all['company_code'].apply(
            lambda x: companies.index(x) if x in companies else len(companies)
        )
        df_all = df_all.sort_values('sort_order').reset_index(drop=True)
        df_all = df_all.drop(['company_code', 'sort_order'], axis=1)
        
        # 컬럼명 변경
        df_all.rename(columns=name_dict, inplace=True)
        
        # 매출액 처리 (천원 단위로 저장되어 있다고 가정)
        # DB 값을 그대로 사용하되, NULL은 빈 문자열로 처리
        df_all['매출액'] = df_all['매출액'].apply(
            lambda x: int(float(x)) if pd.notna(x) and x != '' else ''
        )
        
        # 다른 NULL 값 처리
        for col in ['한글업체명', '설립일', '시.도']:
            df_all[col] = df_all[col].fillna('').astype(str)
            df_all[col] = df_all[col].replace('None', '')
        
        # 딕셔너리로 변환
        df_dict = df_all.to_dict()
        
        # 디버깅용 로그
        print(f"\n=== 매출액 확인 (처음 5개) ===")
        for i in range(min(5, len(companies))):
            code = companies[i]
            name = df_dict.get('한글업체명', {}).get(i, 'N/A')
            sales = df_dict.get('매출액', {}).get(i, 'N/A')
            print(f"Index {i}: {code} ({name}) - 매출액: {sales}")
        
        proj2comp.update(df_dict)
        
        print(f'\nadd_comp_data 처리 완료: {len(companies)}개 기업, 소요시간: {round(time.time() - sst, 2)}초')
        
    except Exception as e:
        print(f"Error in add_comp_data: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # 에러시 빈 값으로 채우기
        empty_dict = {
            '한글업체명': {i: '' for i in range(len(companies))},
            '설립일': {i: '' for i in range(len(companies))},
            '시.도': {i: '' for i in range(len(companies))},
            '매출액': {i: '' for i in range(len(companies))}
        }
        proj2comp.update(empty_dict)
        
    finally:
        db.close()
    
    return proj2comp

# ======================================================================
# 수정일시 : 2025-08-21
# 수정부분 : add_json_data.add_comp_data 함수
# 수정 종료 부분
# 수정자 : 오픈메이트 박승제 책임
# ======================================================================
def add_proj_data(comp2proj: dict):
    sst = time.time()

    name_dict = {
        'NTIS_A_F_014' : '과제명',
        'NTIS_A_F_019' : '연구수행주체',
	    'NTIS_A_F_025' : '과학기술표준분류코드명1_대',
        'NTIS_A_F_094' : '과제수행기관명', 
        'NTIS_A_F_098' : '연구개발단계',
        'NTIS_B_F_026' : '과제수행년도',
    }

    db = get_db_session()
    try:
        technologies = list(comp2proj['project'].values())
        technologies_str = ','.join(f'"{t}"' for t in technologies)

        sql_1 = f'SELECT NTIS_A_F_014, NTIS_A_F_019, NTIS_A_F_025, NTIS_A_F_094, NTIS_A_F_098\
                        FROM vc_ntis_a_tb_0001 \
                        WHERE NTIS_A_F_011 IN ({technologies_str})\
                        ORDER BY FIELD(NTIS_A_F_011, {technologies_str});'

        conn = db.connection()
        print('add_proj_data db.connection :', round(time.time()-sst,2))
        print('add_proj_data rechnologies_str :', technologies_str )

        df_1 = pd.read_sql(sql_1, conn)
        print('add_proj_data sql_1 read_sql :', round(time.time()-sst,2))

        sql_2 = f'''(SELECT IFNULL(max(NTIS_B_F_026), NULL) as NTIS_B_F_026
                FROM vc_ntis_b_tb_0002
                WHERE NTIS_B_F_027 = "{technologies[0]}"
                LIMIT 1)
                '''

        for i in  range(1,len(technologies)):
            sql_2 += '''
                    UNION ALL
    
                    (SELECT IFNULL(max(NTIS_B_F_026), NULL) as NTIS_B_F_026
                    FROM vc_ntis_b_tb_0002
                    WHERE NTIS_B_F_027 = "{technologies[i]}"
                    LIMIT 1)
                    '''
        df_2 = pd.read_sql(sql_2, conn)
        print('add_proj_data sql_2 read_sql :', round(time.time()-sst,2))
        print('add_proj_data rechnologies :', technologies)

        df_all = pd.concat([df_1,df_2],axis=1).reset_index(drop=True)
        df_all.rename(columns = name_dict, inplace=True)
        df_all.fillna('', inplace=True)

        df_dict = df_all.to_dict()

        comp2proj.update(df_dict)

        print('add_proj_data time:', round(time.time()-sst,2))
    finally:
        db.close()

    return comp2proj

# =====================================================================
# NEW: 시연용 임시 데이터 조회 함수 추가
# =====================================================================
def safe_int_convert(value):
    """안전한 정수 변환 (매출액용)"""
    try:
        if pd.notnull(value) and value != '' and value != 'nan':
            return int(float(value))
    except (ValueError, TypeError):
        pass
    return ''

def safe_float_convert(value, default=''):
    """안전한 실수 변환"""
    try:
        if pd.notnull(value) and value != '' and value != 'nan':
            return float(value)
    except (ValueError, TypeError):
        pass
    return default

def safe_str_convert(value):
    """안전한 문자열 변환"""
    if pd.notnull(value) and str(value) != 'nan':
        return str(value)
    return ''

def get_temp_p2c_result(input_project_dict: dict, search_type: str):
    """
    [시연용] AI 모델 출력과 동일한 형식으로 임시 테이블에서 데이터 조회
    프로젝트 → 기업 추천
    """
    try:
        filename = f'temp_p2c_recommendations_{search_type}.csv'
        file_path = os.path.join(os.path.dirname(__file__), filename)
        
        if not os.path.exists(file_path):
            print(f"[시연] 파일을 찾을 수 없습니다: {file_path}")
            file_path = filename
        
        # CSV 읽기 전 디버깅
        print(f"[시연] CSV 파일 경로: {file_path}")
        
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"[시연] {search_type} 데이터 불러오기 완료, 데이터 크기: {len(df)}")
        
        # CSV 컬럼 확인
        print(f"[시연] CSV 컬럼: {df.columns.tolist()}")
        print(f"[시연] 첫 번째 행 데이터: {df.iloc[0].to_dict()}")
        
        df = df.reset_index(drop=True)
        
        # 컬럼명 조정 (실제 CSV 컬럼명에 따라 조정)
        if '시도' in df.columns:
            df.rename(columns={'시도': '시.도'}, inplace=True)
        if '십차산업코드' in df.columns:
            df.rename(columns={'십차산업코드': '10차산업코드'}, inplace=True)
        
        # 모든 NaN을 빈 문자열로 변환
        df = df.fillna('')
        
        # 간단한 변환 (safe 함수 사용하지 않고 직접 처리)
        result_dict = {
            'project': {},
            'rank': {},
            'company': {},
            'score': {},
            '최근종업원수': {},
            '10차산업코드': {},
            '한글주요제품': {},
            '자본총계': {},
            '한글업체명': {},
            '설립일': {},
            '시.도': {},
            '매출액': {}
        }
        
        for i, row in df.iterrows():
            str_i = str(i)
            result_dict['project'][str_i] = 'custom'
            result_dict['rank'][str_i] = i + 1  # 1부터 시작
            result_dict['company'][str_i] = str(row.get('company', ''))
            result_dict['score'][str_i] = float(row.get('score', 0.99987452 - i * 0.0252))  # 기본값 설정
            
            # 종업원수 처리
            emp = row.get('최근종업원수', 0)
            if pd.notnull(emp) and emp != '':
                try:
                    result_dict['최근종업원수'][str_i] = float(emp)
                except:
                    result_dict['최근종업원수'][str_i] = 0.0
            else:
                result_dict['최근종업원수'][str_i] = 0.0
            
            # 문자열 필드 처리
            result_dict['10차산업코드'][str_i] = str(row.get('10차산업코드', ''))
            result_dict['한글주요제품'][str_i] = str(row.get('한글주요제품', ''))
            result_dict['한글업체명'][str_i] = str(row.get('한글업체명', ''))
            result_dict['설립일'][str_i] = str(row.get('설립일', ''))
            result_dict['시.도'][str_i] = str(row.get('시.도', ''))
            
            # 자본총계 처리
            cap = row.get('자본총계', 0)
            if pd.notnull(cap) and cap != '':
                try:
                    result_dict['자본총계'][str_i] = float(cap)
                except:
                    result_dict['자본총계'][str_i] = 0.0
            else:
                result_dict['자본총계'][str_i] = 0.0
            
            # 매출액 처리
            sales = row.get('매출액', 0)
            if pd.notnull(sales) and sales != '':
                try:
                    result_dict['매출액'][str_i] = int(float(sales))
                except:
                    result_dict['매출액'][str_i] = 0
            else:
                result_dict['매출액'][str_i] = 0
        
        print(f"[시연] 변환 완료, 첫 번째 항목: rank={result_dict['rank']['0']}, company={result_dict['company']['0']}")
        
        return result_dict
        
    except Exception as e:
        print(f"[시연] 오류 발생: {str(e)}")
        import traceback
        print(f"[시연] 상세 에러: {traceback.format_exc()}")
        
        # 오류 시 기본 데이터 반환
        return {
            'project': {str(i): 'custom' for i in range(100)},
            'rank': {str(i): i + 1 for i in range(100)},
            'company': {str(i): f'TEST_{i:03d}' for i in range(100)},
            'score': {str(i): 0.99987452 - i * 0.0252 for i in range(100)},
            '최근종업원수': {str(i): 10.0 for i in range(100)},
            '10차산업코드': {str(i): 'C26211' for i in range(100)},
            '한글주요제품': {str(i): '테스트 제품' for i in range(100)},
            '자본총계': {str(i): 1000000.0 for i in range(100)},
            '한글업체명': {str(i): f'테스트기업{i+1}' for i in range(100)},
            '설립일': {str(i): '20200101' for i in range(100)},
            '시.도': {str(i): '서울' for i in range(100)},
            '매출액': {str(i): 1000000 for i in range(100)}
        }


def get_temp_c2p_result(input_company_dict: dict, search_type: str):
    """
    [시연용] AI 모델 출력과 동일한 형식으로 임시 테이블에서 데이터 조회
    기업 → 프로젝트 추천
    """
    db = get_db_session()
    try:
        company_reg_num = input_company_dict.get('업체코드', '')
        
        # 임시 테이블에서 추천 프로젝트 정보 조회
        sql = f"""
            SELECT 
                project,
                rank,
                score,
                과제명,
                연구수행주체,
                과학기술표준분류코드명1_대,
                과제수행기관명,
                연구개발단계,
                과제수행년도,
                지역코드,
                키워드_국문,
                총연구비_합계_원,
                과학기술표준분류코드1_대,
                과학기술표준분류1_중,
                연구개발단계코드
            FROM temp_c2p_recommendations 
            WHERE input_company_reg_num = '{company_reg_num}'
            ORDER BY rank
            LIMIT 100;
        """
        
        conn = db.connection()
        df = pd.read_sql(sql, conn)
        
        # AI 모델 출력과 동일한 형식으로 변환
        result_dict = {
            'company': {str(i): 'custom' for i in range(len(df))},
            'rank': {str(i): int(row['rank']) for i, row in df.iterrows()},
            'project': {str(i): row['project'] for i, row in df.iterrows()},
            'score': {str(i): float(row['score']) for i, row in df.iterrows()},
            '과제명': {str(i): str(row['과제명']) if row['과제명'] else '' for i, row in df.iterrows()},
            '연구수행주체': {str(i): str(row['연구수행주체']) if row['연구수행주체'] else '' for i, row in df.iterrows()},
            '과학기술표준분류코드명1_대': {str(i): str(row['과학기술표준분류코드명1_대']) if row['과학기술표준분류코드명1_대'] else '' for i, row in df.iterrows()},
            '과제수행기관명': {str(i): str(row['과제수행기관명']) if row['과제수행기관명'] else '' for i, row in df.iterrows()},
            '연구개발단계': {str(i): str(row['연구개발단계']) if row['연구개발단계'] else '' for i, row in df.iterrows()},
            '과제수행년도': {str(i): str(row['과제수행년도']) if row['과제수행년도'] else '' for i, row in df.iterrows()},
            '지역코드': {str(i): str(row['지역코드']) if row['지역코드'] else '' for i, row in df.iterrows()},
            '키워드_국문': {str(i): str(row['키워드_국문']) if row['키워드_국문'] else '' for i, row in df.iterrows()},
            '총연구비_합계_원': {str(i): int(row['총연구비_합계_원']) if row['총연구비_합계_원'] else 0 for i, row in df.iterrows()},
            '과학기술표준분류코드1_대': {str(i): str(row['과학기술표준분류코드1_대']) if row['과학기술표준분류코드1_대'] else '' for i, row in df.iterrows()},
            '과학기술표준분류1_중': {str(i): str(row['과학기술표준분류1_중']) if row['과학기술표준분류1_중'] else '' for i, row in df.iterrows()},
            '연구개발단계코드': {str(i): str(row['연구개발단계코드']) if row['연구개발단계코드'] else '' for i, row in df.iterrows()}
        }
        
    except Exception as e:
        print(f"[시연] 오류 발생: {str(e)}")
        print(f"[시연] 로컬에 저장된 데이터 불러오기 시작")
        # 오류 발생 시 로컬에 저장된 임시 데이터를 불러옵니다.
        # 이 부분은 실제 데이터베이스 연결이 실패했을 때를 대비한 예외 처리입니다.
        # 로컬에 저장된 임시 데이터는 'temp_c2p_recommendations.csv' 파일로 가정합니다.

        df = pd.read_csv('temp_c2p_recommendations.csv', encoding='utf-8')  
        print(f"[시연] 로컬 데이터 불러오기 완료, 데이터 크기: {len(df)}")
        df = df[df['input_company_id'] == search_type].reset_index(drop=True)

        # 오류 시 기본 데이터 반환
        result_dict = {
            'company': {str(i): 'custom' for i in range(len(df))},
            'rank': {str(i): int(row['rank']) for i, row in df.iterrows()},
            'project': {str(i): row['project'] for i, row in df.iterrows()},
            'score': {str(i): float(row['score']) for i, row in df.iterrows()},
            '과제명': {str(i): str(row['과제명']) if row['과제명'] else '' for i, row in df.iterrows()},
            '연구수행주체': {str(i): str(row['연구수행주체']) if row['연구수행주체'] else '' for i, row in df.iterrows()},
            '과학기술표준분류코드명1_대': {str(i): str(row['과학기술표준분류코드명1_대']) if row['과학기술표준분류코드명1_대'] else '' for i, row in df.iterrows()},
            '과제수행기관명': {str(i): str(row['과제수행기관명']) if row['과제수행기관명'] else '' for i, row in df.iterrows()},
            '연구개발단계': {str(i): str(row['연구개발단계']) if row['연구개발단계'] else '' for i, row in df.iterrows()},
            '과제수행년도': {str(i): str(row['과제수행년도']) if row['과제수행년도'] else '' for i, row in df.iterrows()},
            '지역코드': {str(i): str(row['지역코드']) if row['지역코드'] else '' for i, row in df.iterrows()},
            '키워드_국문': {str(i): str(row['키워드_국문']) if row['키워드_국문'] else '' for i, row in df.iterrows()},
            '총연구비_합계_원': {str(i): int(row['총연구비_합계_원']) if row['총연구비_합계_원'] else 0 for i, row in df.iterrows()},
            '과학기술표준분류코드1_대': {str(i): str(row['과학기술표준분류코드1_대']) if row['과학기술표준분류코드1_대'] else '' for i, row in df.iterrows()},
            '과학기술표준분류1_중': {str(i): str(row['과학기술표준분류1_중']) if row['과학기술표준분류1_중'] else '' for i, row in df.iterrows()},
            '연구개발단계코드': {str(i): str(row['연구개발단계코드']) if row['연구개발단계코드'] else '' for i, row in df.iterrows()}
        }
        
    finally:
        db.close()

    return result_dict

# =====================================================================
# 수정일 : 2025-08-28
# 수정내용 : main.py 함수 내 모델을 실행하기 위해 필요한 데이터 조회
# 수정사유 : TechRecItem 과 BizRecItem 에는 각각 아래와 같은 데이터가 들어있음
#           TechRecItem : 과제고유번호, 과학기술표준분류코드1_대, 과학기술표준분류1_중, 연구개발단계코드, 제출년도, 지역코드, 총연구비_합계_원, 참여연구원(명), 요약문_한글키워드
#           BizRecItem : 업체코드, 기업명, 설립일, 최근종업원수, 10차산업코드, 한글주요제품, 매출액, 자본총계, 자산총계, 사업목적
# TechRecItem = {
#   '과제고유번호': '1315001941', 
#   '과학기술표준분류코드1_대': 'EE', 
#   '과학기술표준분류1_중': 'EE13', 
#   '연구개발단계코드': '3', 
#   '제출년도': '2023', 
#   '지역코드': '1', 
#   '총연구비_합계_원': '625840000.000', 
#   '참여연구원(명)': '28', 
#   '요약문_한글키워드': 'ai,df'
# }
# BizRecItem = {
#   '업체코드': '692218',
#   '기업명': '(주)에스에프유',
#   '설립일': '2006-09-01',
#   '최근종업원수': '8',
#   '10차산업코드': 'C29271', 
#   '한글주요제품': '차세대 디스플레이 소재,광학시트,광학렌즈', 
#   '매출액': '15712400000.0', 
#   '자본총계': '34153200000.0', 
#   '자산총계': '37418400000.0', 
#   '사업목적': '광학시트,광학렌즈'
# }
# 수정자 : 오픈메이트 박승제 책임
# =====================================================================
def get_model_input_data(item: dict):
    """
    TechRecItem, BizRecItem 에서 모델 입력에 필요한 데이터 조회
    TechRecItem 이 입력되었을 경우 과학기술표준분류코드1_대, 과학기술표준분류1_중 의 한글명을 조회
    BizRecItem 이 입력되었을 경우 10차산업코드의 한글명을 조회
    """
    db = get_db_session()
    conn = db.connection()
    if not item:
        print("get_model_input_data: 입력 데이터가 없습니다.")
        return {}
    
    # ===========================================================================
    # 수정일 : 2025-09-09
    # 수정내용 : 조건절 수정
    # 수정사유 : 웹에서 필수항목 변경으로 인한 조건절 수정
    #           과제고유번호가 웹에서 받지못했을 경우 과학기술표준분류(대)와 과학기술표준분류(중) 정보를 얻어올 수 없음
    #           따라서 마스터 테이블을 이용하여 해당 내용 조회
    # 수정자 : 오픈메이트 박승제 책임
    # ===========================================================================
    elif '과제고유번호' in item or '과학기술표준분류코드1_대' in item:
        # TechRecItem 처리
        try:
            # 과학기술표준분류 코드 값 가져오기
            cat_1 = item.get("과학기술표준분류코드1_대", "")
            cat_2 = item.get("과학기술표준분류코드1_중", "")
            
            print(f"get_model_input_data - 대분류 코드: [{cat_1}], 중분류 코드: [{cat_2}]")
            
            # 대분류 조회
            if cat_1:
                sql_large = f"""
                    SELECT CODE_NM
                    FROM tb_grp_code
                    WHERE GRP_CODE = 4
                    AND CODE = '{cat_1}'
                    LIMIT 1
                """
                print(f"get_model_input_data - 대분류 SQL: {sql_large}")
                df_large = pd.read_sql(sql_large, conn)
                
                if not df_large.empty:
                    item['과학기술표준분류(대)'] = df_large.iloc[0]['CODE_NM']
                else:
                    item['과학기술표준분류(대)'] = ''
            else:
                item['과학기술표준분류(대)'] = ''
            
            # 중분류 조회
            if cat_2:
                sql_medium = f"""
                    SELECT CODE_NM
                    FROM tb_grp_code
                    WHERE GRP_CODE = 5
                    AND CODE = '{cat_2}'
                    LIMIT 1
                """
                print(f"get_model_input_data - 중분류 SQL: {sql_medium}")
                df_medium = pd.read_sql(sql_medium, conn)
                
                if not df_medium.empty:
                    item['과학기술표준분류(중)'] = df_medium.iloc[0]['CODE_NM']
                else:
                    item['과학기술표준분류(중)'] = ''
            else:
                item['과학기술표준분류(중)'] = ''
                
            return item
        
        except Exception as e:
            print(f"get_model_input_data 에서 TechRecItem 오류 발생: {str(e)}")
            item['과학기술표준분류(대)'] = ''
            item['과학기술표준분류(중)'] = ''
            return item
        
        finally:
            db.close()
    # ===========================================================================
    # 수정일 : 2025-09-09
    # 수정자 : 오픈메이트 박승제 책임
    # 수정 종료 지점
    # ===========================================================================

    elif '업체코드' in item or '기업명' in item:
        # BizRecItem 처리
        try:
            # 현재는 임시 테이블에서 조회, 최종 테이블 필요
            sql = f'''
                SELECT
                    *
                FROM
                    tb_industry_code
                WHERE
                    표준코드 = "{item.get("10차산업코드", "")}"
                LIMIT 1;
            '''
            df = pd.read_sql(sql, conn)
            print("get_model_input_data - BizRecItem SQL:", df)
            if not df.empty:
                item['10차산업코드명'] = df.iloc[0]['세세분류']
            else:
                item['10차산업코드명'] = ''
            return item
        
        except Exception as e:
            print(f"get_model_input_data 에서 BizRecItem 오류 발생: {str(e)}")

        finally:
            db.close()
    else:
        print("get_model_input_data: 유효한 항목이 아닙니다.")
        return {}   

# =====================================================================
# 수정일 : 2025-08-29
# 수정내용 : Milvus DB 에서 조회된 결과를 DataFrame으로 받아 
#           Aollo DB 에서 필요한 데이터를 조회하여 추가
# 수정사유 : API 응답 형식에 맞추기 위해
#           TechRecItem 과 BizRecItem 에는 각각 아래와 같은 데이터가 들어있음
#           TechRecItem : 과제고유번호, 과학기술표준분류코드1_대, 과학기술표준분류1_중, 연구개발단계코드, 제출년도, 지역코드, 총연구비_합계_원, 참여연구원(명), 요약문_한글키워드
#           BizRecItem : 업체코드, 기업명, 설립일, 최근종업원수, 10차산업코드, 한글주요제품, 매출액, 자본총계, 자산총계, 사업목적
# TechRecItem = {
#   '과제고유번호': '1315001941', 
#   '과학기술표준분류코드1_대': 'EE', 
#   '과학기술표준분류1_중': 'EE13', 
#   '연구개발단계코드': '3', 
#   '제출년도': '2023', 
#   '지역코드': '1', 
#   '총연구비_합계_원': '625840000.000', 
#   '참여연구원(명)': '28', 
#   '요약문_한글키워드': 'ai,df'
# }
# BizRecItem = {
#   '업체코드': '692218',
#   '기업명': '(주)에스에프유',
#   '설립일': '2006-09-01',
#   '최근종업원수': '8',
#   '10차산업코드': 'C29271', 
#   '한글주요제품': '차세대 디스플레이 소재,광학시트,광학렌즈', 
#   '매출액': '15712400000.0', 
#   '자본총계': '34153200000.0', 
#   '자산총계': '37418400000.0', 
#   '사업목적': '광학시트,광학렌즈'
# }
# 수정자 : 오픈메이트 박승제 책임
# =====================================================================
def enrich_data(out_dict: dict, search_type: str):
    """
    Milvus 결과 딕셔너리(out_dict)에 Apollo DB 메타를 병합해 응답 규격으로 정리
    """
    db = get_db_session()
    try:
        conn = db.connection()
        df = pd.DataFrame(out_dict)
        if df.empty:
            print("enrich_data: 입력 데이터가 없습니다.")
            return out_dict

        if search_type == 'project_to_company':
            # 1) 사업자번호 보존 (Milvus 'company' = 사업자번호)
            df['사업자번호'] = df['company'].astype(str)
            biznos = df['사업자번호'].tolist()
            if not biznos:
                return out_dict
            biz_str = ','.join(f"'{b}'" for b in biznos)

            # 2) 사업자번호 기준으로 상세 조회 (업체코드/한글업체명/시.도/매출액/자본총계/10차산업코드/최근종업원수/한글주요제품)
            sql = f"""
            SELECT 
                a.NICE_A_F_002 AS 사업자번호,
                a.NICE_A_F_001 AS 업체코드,
                a.NICE_A_F_004 AS 한글업체명,
                a.NICE_A_F_023 AS 설립일,
                a.NICE_A_F_051 AS `시.도`,
                IFNULL((
                    SELECT b.NICE_B_F_009
                    FROM vc_nice_b_tb_0002 b
                    WHERE b.NICE_B_F_001 = a.NICE_A_F_001
                    ORDER BY b.NICE_B_F_008 DESC
                    LIMIT 1), NULL) AS 매출액,
                IFNULL((
                    SELECT b.NICE_B_F_038
                    FROM vc_nice_b_tb_0002 b
                    WHERE b.NICE_B_F_001 = a.NICE_A_F_001
                    ORDER BY b.NICE_B_F_008 DESC
                    LIMIT 1), NULL) AS 자본총계,
                a.NICE_A_F_032 AS `10차산업코드`,
                a.NICE_A_F_025 AS 최근종업원수,
                a.NICE_A_F_037 AS 한글주요제품
            FROM vc_nice_a_tb_0001_asti_spcl a
            WHERE a.NICE_A_F_002 IN ({biz_str})
            ORDER BY FIELD(a.NICE_A_F_002, {biz_str});
            """
            df_details = pd.read_sql(sql, conn).fillna('')
            # 숫자형 안전 변환
            df_details['매출액'] = df_details['매출액'].apply(safe_int_convert)
            df_details['자본총계'] = df_details['자본총계'].apply(safe_int_convert)

            # 3) 사업자번호를 키로 병합
            by_bizno = df_details.set_index('사업자번호').to_dict(orient='index')
            for idx, row in df.iterrows():
                bizno = str(row['사업자번호'])
                d = by_bizno.get(bizno, {})
                # company(=응답용)에는 '업체코드'를 기록
                df.at[idx, 'company'] = d.get('업체코드', '')
                # 나머지 메타
                for key in ['한글업체명','설립일','시.도','매출액','10차산업코드','최근종업원수','한글주요제품','자본총계']:
                    df.at[idx, key] = d.get(key, '')

            # 4) 컬럼명 정리
            df.rename(columns={
                'company_promising_score': '유망성점수',
                'asti_company': 'ASTI기업',
                'special_zone_company': '특구기업'
            }, inplace=True)
            # 요청 포맷: project는 고정 'custom'
            df['project'] = 'custom'

            # 5) 딕셔너리(인덱스 문자열) 변환
            out = df.to_dict(orient='dict')
            for k in out:
                out[k] = {str(i): v for i, v in out[k].items()}
            return out

        elif search_type == 'company_to_project':
            # (기존 구현 유지) 과제 상세 보강
            projects = df['project'].tolist()
            if not projects:
                return out_dict
            projs_str = ','.join(f"'{p}'" for p in projects)
            sql = f"""
                SELECT
                    NTIS_A_F_011 AS 과제고유번호,
                    NTIS_A_F_014 AS 과제명,
                    NTIS_A_F_019 AS 연구수행주체,
                    NTIS_A_F_094 AS 과제수행기관명,
                    NTIS_A_F_024 AS 과학기술표준분류코드1_대,
                    NTIS_A_F_027 AS 과학기술표준분류코드1_중,
                    NTIS_A_F_025 AS 과학기술표준분류코드명1_대,
                    NTIS_A_F_028 AS 과학기술표준분류명1_중,
                    NTIS_A_F_088 AS 키워드_국문,
                    NTIS_A_F_097 AS 연구개발단계코드,
                    NTIS_A_F_098 AS 연구개발단계,
                    NTIS_A_F_126 AS 총연구비_합계_원,
                    NTIS_A_F_020 AS 지역코드,
                    (SELECT MAX(NTIS_B_F_026)
                     FROM VC_NTIS_B_TB_0002
                     WHERE NTIS_B_F_027 = NTIS_A_F_011) AS 과제수행년도
                FROM VC_NTIS_A_TB_0001
                WHERE NTIS_A_F_011 IN ({projs_str})
                ORDER BY FIELD(NTIS_A_F_011, {projs_str});
            """
            det = pd.read_sql(sql, conn).fillna('')
            by_pid = det.set_index('과제고유번호').to_dict(orient='index')
            for idx, row in df.iterrows():
                pid = row['project']
                info = by_pid.get(pid, {})
                for key in ['과제명','연구수행주체','과학기술표준분류코드명1_대','과학기술표준분류명1_중',
                            '과제수행기관명','연구개발단계','과제수행년도','지역코드','키워드_국문',
                            '총연구비_합계_원','과학기술표준분류코드1_대','과학기술표준분류코드1_중','연구개발단계코드']:
                    df.at[idx, key] = info.get(key, '')
            df.rename(columns={'project_promising_score': '유망성점수'}, inplace=True)
            df['company'] = 'custom'

            out = df.to_dict(orient='dict')
            for k in out:
                out[k] = {str(i): v for i, v in out[k].items()}
            return out

        else:
            print("enrich_data: 유효한 search_type이 아닙니다.")
            return out_dict

    except Exception as e:
        print(f"enrich_data 오류 발생: {str(e)}")
        return out_dict
    finally:
        db.close()
