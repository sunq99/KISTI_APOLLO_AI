#-*- coding: utf-8 -*-
import os
from dotenv import load_dotenv

env_name = os.environ.get("APP_ENV")
load_dotenv(dotenv_path=f'.env.{env_name}' if env_name else '.env')

from typing import List, Dict, Any
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import JSONResponse
from contextlib import asynccontextmanager

import pandas as pd
import numpy as np
import json
import os
import time
import textwrap
import uvicorn
import logging
import re

# import app.config
from add_json_data import *
# DB 보강
from add_json_data import add_comp_data, add_proj_data, Session, get_model_input_data  # Session middleware에서 사용
# Milvus
from pymilvus import connections, Collection, utility
from sbert_pool import get_sbert, encode_text  # SBERT 싱글톤
# 명사추출
from extract_nouns import extract_project_title_keywords

# ----------------------- logger ------------------------
model1_logger = logging.getLogger('model1')
model1_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] %(name)s:%(levelname)s - %(message)s")
os.makedirs('./logs', exist_ok=True)
model1_handler = logging.FileHandler('./logs/debug.log')
model1_handler.setFormatter(formatter)
if not model1_logger.handlers:
    model1_logger.addHandler(model1_handler)
# -------------------------------------------------------


import ast

import warnings
warnings.filterwarnings(action='ignore')

# ================================================================================
# 수정일 : 2025-08-28
# 수정 내용 : M1 모델 로직 변경
# 기존에는 Model 을 사용하여 모델 결과를 얻었으나, Milvus DB 의
# 벡터 검색을 통해 결과를 얻도록 변경
# 수정자 : 오픈메이트 박승제 책임
# ================================================================================
# 기존 모델 로직
# ================================================================================
# from kisti_edit import Model1
# model = Model1('./model/my-model-199999')
# ================================================================================
# 기존 모델 로직 끝
# ================================================================================
MILVUS_HOST = os.getenv("MILVUS_HOST", "203.250.238.26")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
PROJ_COLLECTION = os.getenv("MILVUS_PROJ_COLLECTION", "public_rnd_embeddings_250827")
COMP_COLLECTION = os.getenv("MILVUS_COMP_COLLECTION", "company_embeddings_250827")
NPROBE = int(os.getenv("MILVUS_NPROBE", "32"))
TOPK_SEARCH = int(os.getenv("TOPK_SEARCH", "200"))   # 유사도 상위 K
TOPN_FINAL  = int(os.getenv("TOPN_FINAL",  "100"))   # 유망성점수 Top N

class MilvusModel:
    """프로젝트/기업 ID 또는 키워드로 → 반대쪽 컬렉션 유사도 검색 → 유망성점수로 최종 정렬"""
    def __init__(self,
                 preload_sbert: bool = True,
                 sbert_model: str = None,
                 allow_hf_download: bool = False,
                 hf_token: str = None):
        self.alias = "default"
        if not connections.has_connection(self.alias):
            connections.connect(alias=self.alias, host=MILVUS_HOST, port=MILVUS_PORT)

        if not utility.has_collection(PROJ_COLLECTION):
            raise RuntimeError(f"Missing Milvus collection: {PROJ_COLLECTION}")
        if not utility.has_collection(COMP_COLLECTION):
            raise RuntimeError(f"Missing Milvus collection: {COMP_COLLECTION}")

        self.proj_col = Collection(PROJ_COLLECTION)
        self.comp_col = Collection(COMP_COLLECTION)
        self.search_params = {"metric_type": "COSINE", "params": {"nprobe": NPROBE}}
        self.proj_col.load()
        self.comp_col.load()

        # --- SBERT (선택) 사전 로드 & 워밍업 ---
        self.sbert = None
        if preload_sbert:
            # 환경변수로도 지정 가능 (SBERT_MODEL, ALLOW_HF_DOWNLOAD, HUGGINGFACE_HUB_TOKEN)
            if sbert_model is None:
                sbert_model = os.getenv("SBERT_MODEL", "/home/dev23_apollo/ai/model1/pro-sroberta")
            if hf_token is None:
                hf_token = os.getenv("HUGGINGFACE_HUB_TOKEN") or os.getenv("HF_TOKEN")
            allow_hf_download = allow_hf_download or bool(int(os.getenv("ALLOW_HF_DOWNLOAD", "0")))

            self.sbert = get_sbert(model_name=sbert_model,
                                   allow_hf_download=allow_hf_download,
                                   hf_token=hf_token)
            # 워밍업: 초기 인퍼런스 지연 제거
            _ = self.sbert.encode("warmup", convert_to_numpy=True)
            # ↑ 질문 주신 코드(워밍업)를 이곳에 합쳤습니다.

    # --------- 내부 유틸 ---------
    def _get_vec(self, col: Collection, id_field: str, vec_field: str, id_value: str) -> List[float]:
        rows = col.query(expr=f'{id_field} == "{id_value}"', output_fields=[vec_field])
        if not rows:
            raise ValueError(f"{id_field}={id_value} not found in {col.name}")
        vec = rows[0].get(vec_field)
        if not vec:
            raise ValueError(f"{id_field}={id_value} has empty vector in {col.name}")
        return vec

    def _search(self, target_col: Collection, anns_field: str, query_vec: List[float], output_fields: List[str]):
        res = target_col.search(
            data=[query_vec],
            anns_field=anns_field,
            param=self.search_params,
            limit=TOPK_SEARCH,
            output_fields=output_fields,
        )
        hits = res[0] if res else []
        out = []
        for h in hits:
            row = {f: h.entity.get(f) for f in output_fields}
            row["score"] = float(h.score)  # COSINE
            out.append(row)
        return out

    @staticmethod
    def _final_rank(items: List[Dict[str, Any]], promising_key: str, score_key: str = "score") -> List[Dict[str, Any]]:
        # 유망성점수 내림차순 → 유사도(score) 내림차순
        return sorted(items, key=lambda x: (float(x.get(promising_key) or 0.0), float(x.get(score_key) or 0.0)),
                      reverse=True)[:TOPN_FINAL]

    # --------- 키워드 추출기 ---------
    @staticmethod
    def _extract_keywords_from_dict(data: dict, keys_to_extract: list, keyword_field: list) -> list:
        """
        data에서 특정 키들의 값을 모아 리스트로 반환.
        keyword_field는 쉼표 구분을 분리.
        """
        result = []
        for key in keys_to_extract:
            if key not in data or data[key] is None:
                continue
            if key in keyword_field and data[key]:
                parts = str(data[key]).split(',')
                result.extend([p.strip() for p in parts if str(p).strip()])
            else:
                val = str(data[key]).strip()
                if val:
                    result.append(val)
        # 중복 제거(입력 순서 유지)
        seen = set(); out = []
        for v in result:
            if v not in seen:
                seen.add(v); out.append(v)
        return out

    def build_keywords_from_dict(self, payload: dict) -> list:
        """
        payload에서 키워드 추출
        payload는 TechRecItem 또는 BizRecItem 형태
        """
        # ---- 과제 레코드 분기 ----
        if "과제고유번호" in payload or "과학기술표준분류코드1_대" in payload:
            keys_to_extract = ["과학기술표준분류(대)", "과학기술표준분류(중)", "요약문_한글키워드"]
            keyword_field = ["요약문_한글키워드"]
            keywords = self._extract_keywords_from_dict(payload, keys_to_extract, keyword_field)

            # ----- 과제명 명사 토큰 병합 -----
            title = payload.get("과제명")
            title_tokens = []

            if title:
                # (A) 우선 extract_nouns 모듈 사용을 시도 (지연 임포트)
                _extract = None
                try:
                    from extract_nouns import extract_project_title_keywords as _extract  # lazy import
                except Exception as e:
                    model1_logger.warning("extract_nouns lazy import failed: %s", e)
                    _extract = None

                try:
                    if callable(_extract):
                        title_tokens = _extract(title) or []
                except Exception as e:
                    model1_logger.warning("title noun extraction failed: %s", e)
                    title_tokens = []

                # (B) 폴백: 정규식 기반 간이 토크나이저 (JVM/konlpy 불가 시에도 동작)
                if not title_tokens:
                    t = str(title)
                    # 괄호/따옴표 제거 및 공백 정리
                    t_norm = re.sub(r'[()\[\]{}"“”‘’]', ' ', t)
                    # 보존할 패턴 먼저 수집 (PM1.0, Level 3 등)
                    keep = []
                    keep += re.findall(r'\bPM\s*\d+(?:\.\d+)?\b', t_norm, flags=re.I)
                    keep += re.findall(r'\bLevel\s*\d+\b', t_norm, flags=re.I)
                    # 영문/숫자/한글 토큰
                    keep += re.findall(r'[A-Za-z]{2,}[A-Za-z0-9./-]*', t_norm)
                    keep += re.findall(r'[가-힣]{2,}', t_norm)
                    # 중복 제거(순서 보존) + 조사 간단 제거 + 불용어 제거
                    STOP = {"및", "위한", "통한", "의", "등"}
                    seen, toks = set(), []
                    for tok in keep:
                        tok = tok.strip()
                        # 조사(을/를/이/가/은/는/에/의/와/과/로/으로/에서/까지/부터) 1회 제거
                        tok = re.sub(r'(을|를|이|가|은|는|에|의|와|과|로|으로|에서|까지|부터)$', '', tok)
                        if tok and tok not in seen and tok not in STOP and len(tok) > 1:
                            seen.add(tok); toks.append(tok)
                    title_tokens = toks

                model1_logger.debug("title tokens (%d): %s", len(title_tokens), title_tokens[:12])

                # (C) 병합 + 중복 제거(순서 보존)
                for tok in title_tokens:
                    if tok and tok not in keywords:
                        keywords.append(tok)

            return keywords

        # ---- 기업 레코드 분기 ----
        if "업체코드" in payload or "사업자번호" in payload or "기업명" in payload:
            keys_to_extract = ["10차산업코드명", "한글주요제품", "사업목적"]
            keyword_field = ["한글주요제품", "사업목적"]
            return self._extract_keywords_from_dict(payload, keys_to_extract, keyword_field)

        # ---- 기본 폴백 ----
        return []

    # --------- 키워드 → 벡터 ---------
    def _encode_keywords(self, keywords: List[str]) -> np.ndarray:
        if not self.sbert:
            raise RuntimeError("SBERT not preloaded. MilvusModel(preload_sbert=True)로 초기화하세요.")
        text = ';'.join(keywords)
        vec = encode_text(text, model=self.sbert)  # L2 정규화된 np.float32[768]
        return vec

    # --------- 키워드 리스트 → 기업 ---------
    def keywords_to_company(self, keywords: List[str]) -> pd.DataFrame:
        '''
        keywords: 키워드 리스트
        return: DataFrame with columns: company, company_promising_score, asti_company, special_zone_company, rank, score
        '''
        qvec = self._encode_keywords(keywords)
        print("keywords_to_company qvec : ", qvec)
        raw = self._search(
            target_col=self.comp_col,
            anns_field="company_text_embedding",
            query_vec=qvec.tolist(),
            output_fields=["company_code", "company_name", "company_promising_score", "asti_company", "special_zone_company", "industry_code_name", "keyword_list"]
        )
        picked = self._final_rank(raw, "company_promising_score")
        return {
            "company": [r["company_code"] for r in picked],  # 사업자번호
            "company_promising_score": [r.get("company_promising_score", None) for r in picked],
            "asti_company": [bool(r.get("asti_company", False)) for r in picked],
            "special_zone_company": [bool(r.get("special_zone_company", False)) for r in picked],
            "rank": list(range(1, len(picked) + 1)),
            "score": [r["score"] for r in picked],
        }

    # --------- 키워드 리스트 → 과제 ---------
    def keywords_to_project(self, keywords: List[str]) -> pd.DataFrame:
        '''
        keywords: 키워드 리스트
        return: DataFrame with columns: project, project_promising_score, rank, score
        '''
        qvec = self._encode_keywords(keywords)
        print("keywords_to_project qvec : ", qvec)
        raw = self._search(
            target_col=self.proj_col,
            anns_field="project_text_embedding",
            query_vec=qvec.tolist(),
            output_fields=["project_id", "project_name", "keyword_list", "project_promising_score"]
        )
        picked = self._final_rank(raw, "project_promising_score")
        return {
            "project": [r["project_id"] for r in picked],
            "project_promising_score": [r.get("project_promising_score", None) for r in picked],
            "rank": list(range(1, len(picked) + 1)),
            "score": [r["score"] for r in picked],
        }

# ================================================================================
# 2025-08-28 : M1 모델 로직 변경 끝
# ================================================================================

origins = [
    "*",
    ]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BizRecItem(BaseModel):
    REG_NUM : Dict[str, str]             # 사업자등록번호

class TechRecItem(BaseModel):
   PROJECT : Dict[str, str]              # 과제고유번호


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    response = await call_next(request)
    Session.remove()
    return response


# ================================================================================
# 2025-08-28 : M1 모델 로직 변경
# 앱 시작시 MilvusModel 인스턴스 생성
# 수정자 : 오픈메이트 박승제 책임
# ================================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    model1_logger.info("Connecting to Milvus and loading collections...")
    app.state.model = MilvusModel()
    # SBERT 싱글톤 모델 사전 로드
    sbert_path = os.getenv("SBERT_MODEL", "/home/dev23_apollo/ai/model1/pro-sroberta")
    allow_hf = bool(int(os.getenv("ALLOW_HF_DOWNLOAD", "0")))
    hf_token = os.getenv("HUGGINGFACE_HUB_TOKEN") or os.getenv("HF_TOKEN")
    app.state.sbert = get_sbert(model_name=sbert_path, allow_hf_download=allow_hf, hf_token=hf_token)
    # 워밍업
    _ = app.state.sbert.encode("warmup", convert_to_numpy=True)
    model1_logger.info("Milvus model & SBERT encoder are ready.")
    yield

app.router.lifespan_context = lifespan
# ================================================================================
# 2025-08-28 : M1 모델 로직 변경 끝
# ================================================================================


# ====================================================================
# 기존 API
# =====================================================================

# 기술 custom 입력 -> 회사 출력
# @app.post('/api/model1_custom/tech/predict')
# async def tech_custom_predict(item: TechRecItem):
#     st = time.time()

#     model1_logger.info(f'{os.getpid()}: tech custom 호출')

#     dicted_item = dict(item)
#     dicted_input = dict(dicted_item['PROJECT'])
#     print("dicted_input", dicted_input)

#     out_df = model.predict([dicted_input], 2)
#     out_df = out_df.fillna('')

#     result_dict = out_df.to_dict()
#     #print(result_dict)

#     result = add_comp_data(result_dict)
#     return JSONResponse(result)


# # 회사 custom 입력 -> 기술 출력
# @app.post('/api/model1_custom/biz/predict')
# async def biz_custom_predict(item: BizRecItem):
#     st = time.time()

#     model1_logger.info(f'{os.getpid()}: biz custom 호출')

#     dicted_item = dict(item)
#     dicted_input = dict(dicted_item['REG_NUM'])
#     print("dicted_input", dicted_input)

#     out_df = model.predict([dicted_input], 1)
#     out_df = out_df.fillna('')

#     result_dict = out_df.to_dict()

#     model1_logger.info(f'model1 biz custom time: {round(time.time() - st, 2)}')

#     result = add_proj_data(result_dict)
#     return JSONResponse(result)

# =====================================================================
# NEW: 시연용 임시 API 추가
# =====================================================================

# # [시연용] 기술 custom 입력 -> 회사 출력 (임시 테이블 조회)
# @app.post('/api/model1_custom/tech/predict_temp')
# async def tech_predict_temp(item: TechRecItem):
#     st = time.time()
#     model1_logger.info(f'{os.getpid()}: [tech] tech_predict_temp (시연용) 호출')

#     # 입력받은 데이터에서 프로젝트 정보 추출
#     input_project_dict = dict(item.PROJECT)
#     print("[tech] predict_temp_item : ", item)
#     print("[tech] predict_temp_input_project_dict : ", input_project_dict)

#     # AI 모델 예측을 생략하고 바로 임시 데이터 조회 함수 호출
#     # add_comp_data_temp 함수는 프로젝트 정보를 기반으로 임시 테이블을 조회합니다.
#     if input_project_dict.get('과제고유번호') == '1415164339':
#         result = get_temp_p2c_result(input_project_dict, '반도체디스플레이')

#     elif input_project_dict.get('과제고유번호') == '1711203350':
#         result = get_temp_p2c_result(input_project_dict, '수소')

#     elif input_project_dict.get('과제고유번호') == '1425164509':
#         result = get_temp_p2c_result(input_project_dict, '인공지능')

#     elif input_project_dict.get('과제고유번호') == '1711197858':
#         result = get_temp_p2c_result(input_project_dict, '사이버보안')

#     elif input_project_dict.get('과제고유번호') == '1415185971':
#         result = get_temp_p2c_result(input_project_dict, '첨단바이오')

#     else:
#         dicted_item = dict(item)
#         dicted_input = dict(dicted_item['PROJECT'])
#         out_df = model.predict([dicted_input], 2)
#         out_df = out_df.fillna('')
#         result_dict = out_df.to_dict()
#         result = add_comp_data(result_dict)

#     print("[tech] result : ", result)
#     model1_logger.info(f'[tech] {os.getpid()} : tech_predict_temp (시연용) 완료')
#     model1_logger.info(f'[tech] tech_predict_temp time : {round(time.time() - st, 2)}')
#     return JSONResponse(result)

# # [시연용] 회사 custom 입력 -> 기술 출력 (임시 테이블 조회)
# @app.post('/api/model1_custom/biz/predict_temp')
# async def biz_predict_temp(item: BizRecItem):
#     st = time.time()
#     model1_logger.info(f'{os.getpid()}: biz_predict_temp (시연용) 호출')
    
#     # 입력받은 데이터에서 회사 정보 추출
#     input_company_dict = dict(item.REG_NUM)
#     print("[biz] predict_temp_item : ", item)
#     print("[biz] predict_temp_input_company_dict : ", input_company_dict)
    
#     # AI 모델 예측을 생략하고 바로 임시 데이터 조회 함수 호출
#     # add_proj_data_temp 함수는 회사 정보를 기반으로 임시 테이블을 조회합니다.    
#     if input_company_dict.get('업체코드') == 'HO8946':
#         result = get_temp_c2p_result(input_company_dict, '반도체디스플레이_1')

#     elif input_company_dict.get('업체코드') == '692218':
#         result = get_temp_c2p_result(input_company_dict, '반도체디스플레이_2')

#     elif input_company_dict.get('업체코드') == 'JX8741':
#         result = get_temp_c2p_result(input_company_dict, '수소')

#     elif input_company_dict.get('업체코드') == '166646':
#         result = get_temp_c2p_result(input_company_dict, '인공지능')

#     elif input_company_dict.get('업체코드') == 'L50004':
#         result = get_temp_c2p_result(input_company_dict, '사이버보안')

#     elif input_company_dict.get('업체코드') == 'J02700':
#         result = get_temp_c2p_result(input_company_dict, '첨단바이오')

#     else:
#         dicted_item = dict(item)
#         dicted_input = dict(dicted_item['REG_NUM'])
#         out_df = model.predict([dicted_input], 1)
#         out_df = out_df.fillna('')
#         result_dict = out_df.to_dict()
#         result = add_proj_data(result_dict)

#     print("[biz] result : ", result)
#     model1_logger.info(f'[biz] {os.getpid()} : biz_predict_temp (시연용) 완료')
#     model1_logger.info(f'[biz] biz_predict_temp time : {round(time.time() - st, 2)}')
#     return JSONResponse(result)

# =====================================================================
# 2025-08-28 : M1 모델 로직 변경
# 기존에는 Model 을 사용하여 모델 결과를 얻었으나, Milvus DB 의 벡터 검색을 통해 결과를 얻도록 변경
# =====================================================================
# 기술 custom 입력 -> 회사 출력
@app.post('/api/model1_custom/tech/predict_temp')
async def tech_custom_predict_temp(item: TechRecItem):
    '''
    TechRecItem 형태
    {"PROJECT": 
       {
        '과제고유번호': '1315001941',
        '과학기술표준분류코드1_대': 'EE',
        '과학기술표준분류1_중': 'EE13',
        '연구개발단계코드': '3',
        '제출년도': '2023',
        '지역코드': '1',
        '총연구비_합계_원': '625840000.000',
        '참여연구원(명)': '28',
        '요약문_한글키워드': 'ai,df'
        }
    }
    out_dict: 
    {
        "company": ['6778600522', '1298678860', ...], # 사업자번호
        "company_promising_score": [87.700531, 85.12345, ...],
        "asti_company": [True, False, ...],
        "special_zone_company": [False, True, ...],
        "rank": [1, 2, ...],
        "score": [0.87, 0.85, ...],
    }
    return:
    {
        "project": {"0": "custom", "1": "custom", ...},
        "rank": {"0": 1, "1": 2, ...},
        "company": {"0": "692218", "1": "692218", ...}, # 업체코드
        "score": {"0": 0.87, "1": 0.85, ...}, # 유사도 점수
        "유망성점수": {"0": 87.700531, "1": 85.12345, ...},
        "사업자번호": {"0": "6778600522", "1": "1298678860", ...}, # 사업자번호
        "최근종업원수": {"0": "8", "1": "8", ...},
        "10차산업코드": {"0": "C29271", "1": "C29271", ...},
        "한글주요제품": {"0": "차세대 디스플레이 소재,광학시트,광학렌즈", "1": "차세대 디스플레이 소재,광학시트,광학렌즈", ...},
        "자본총계": {"0": "34153200000.0", "1": "34153200000.0", ...},
        "한글업체명": {"0": "(주)에스에프유", "1": "(주)에스에프유", ...},
        "설립일": {"0": "20060901", "1": "20060901", ...},
        "시.도": {"0": "서울", "1": "서울", ...},
        "매출액": {"0": "15712400000.0", "1": "15712400000.0", ...},
        "ASTI기업": {"0": True, "1": True, ...},
        "특구기업": {"0": False, "1": False, ...}, 
    }
    '''
    st = time.time()
    model1_logger.info(f'{os.getpid()}: [tech_custom_predict_temp] 호출')
    print("[tech_custom_predict_temp] 호출")
    print("[tech_custom_predict_temp] TechRecItem : ", item)
    payload = dict(item.PROJECT)
    # 1) 코드 → 한글명 보강 (과학기술표준분류(대)/(중) 확보)
    print("[tech_custom_predict_temp] input payload : ", payload)
    payload = get_model_input_data(payload) or payload  # 없으면 원본 유지
    print("[tech_custom_predict_temp] payload after get_model_input_data : ", payload)

    # 2) 키워드 구성 (과제용: 과기표준(대)/(중), 요약문_한글키워드)
    keywords = app.state.model.build_keywords_from_dict(payload)
    print("[tech_custom_predict_temp] keywords : ", keywords)

    out_dict = app.state.model.keywords_to_company(keywords)
    result = enrich_data(out_dict, 'project_to_company') # 과제→기업 보강

    print("[tech_custom_predict_temp] result : ", result)
    model1_logger.info(f'model1 [tech_custom_predict_temp] time: {round(time.time() - st, 2)}s')
    return JSONResponse(result)

# 회사 custom 입력 -> 기술 출력
@app.post('/api/model1_custom/biz/predict_temp')
async def biz_custom_predict_temp(item: BizRecItem):
    '''
    item: BizRecItem 형태
    {"REG_NUM": 
       {
        '업체코드': '692218',
        '기업명': '(주)에스에프유',
        '설립일': '2006-09-01',
        '최근종업원수': '8',
        '10차산업코드': 'C29271', 
        '한글주요제품': '차세대 디스플레이 소재,광학시트,광학렌즈', 
        '매출액': '15712400000.0', 
        '자본총계': '34153200000.0', 
        '자산총계': '37418400000.0', 
        '사업목적': '광학시트,광학렌즈'
       }
    }
    out_dict: 
    {
        "project": ['9991008418', '9991008418', ...],
        "project_promising_score": [87.700531, 85.12345, ...],
        "rank": [1, 2, ...],
        "score": [0.87, 0.85, ...],
    }
    return: 
    {
        "company": {"0": "custom", "1": "custom", ...},
        "rank": {"0": 1, "1": 2, ...},
        "project": {"0": "692218", "1": "692218", ...}, # 과제고유번호
        "score": {"0": 0.87, "1": 0.85, ...},
        "유망성점수": {"0": 0.95, "1": 0.93, ...},
        "지역코드": {"0": "08", "1": "99", ...},
        "키워드_국문": {"0": "", "1": "스마트 홈;홈네트워크;사물인터넷;", ...},
        "총연구비_합계_원" : {"0": "186250000", "1": "679688000", ...},
        "과학기술표준분류코드1_대": {"0": "EE", "1": "ED", ...},
        "과학기술표준분류1_중": {"0": "EE08", "1": "ED06", ...},
        "연구개발단계코드": {"0": "(주)에스에프유", "1": "(주)에스에프유", ...},
        "과제명": {"0": "Z-wave 기반의 Smart Home IoT 허브", "1": "AI Display 기반 능동형 홈케어 솔루션 디자인 개발", ...},
        "연구수행주체": {"0": "중소기업", "1": "중견기업", ...},
        "과학기술표준분류코드명1_대": {"0": "정보/통신", "1": "전기/전자", ...},
        "과학기술표준분류명1_중": {"0": "재난정보통신", "1": "의약품/의약품개발기술", ...},
        "과제수행기관명": {"0": "주식회사엔플러그", "1": "(주)코맥스", ...},
        "연구개발단계": {"0": "기초연구", "1": "개발연구", ...},
        "과제수행년도": {"0": "2003", "1": "2019", ...},
    }
    '''
    st = time.time()
    model1_logger.info(f'{os.getpid()}: [biz_custom_predict_temp] 호출')
    print("[biz_custom_predict_temp] 호출")
    print("[biz_custom_predict_temp] BizRecItem : ", item)
    payload = dict(item.REG_NUM)
    print("[biz_custom_predict_temp] payload before get_model_input_data : ", payload)
    # 1) 코드 → 한글명 보강 (10차산업코드명 확보)
    payload = get_model_input_data(payload) or payload
    print("[biz_custom_predict_temp] payload after get_model_input_data : ", payload)

    # 2) 키워드 구성 (기업용: 10차산업코드명 + 주요제품 + 사업목적)
    keywords = app.state.model.build_keywords_from_dict(payload)
    print("[biz_custom_predict_temp] keywords : ", keywords)

    out_dict = app.state.model.keywords_to_project(keywords)
    result = enrich_data(out_dict, 'company_to_project') # 기업→과제 보강

    print("[biz_custom_predict_temp] result : ", result)
    model1_logger.info(f'model1 [biz_custom_predict_temp] time: {round(time.time() - st, 2)}s')
    return JSONResponse(result)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )