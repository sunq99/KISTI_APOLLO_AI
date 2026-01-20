import os
from typing import Dict, Any, List
from urllib.parse import quote_plus
from dotenv import load_dotenv

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection

# .env 파일 로드
load_dotenv()

# =========================
# Milvus 설정
# =========================
MILVUS_HOST = os.getenv("MILVUS_HOST", "203.250.238.26")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
COLLECTION_NAME = os.getenv("MILVUS_COLLECTION", "ntb_20260102")

EMBED_MODEL_NAME = os.getenv("EMBED_MODEL", "jhgan/ko-sroberta-multitask")

FIELD_ID = "NTB_A_F_001"
FIELD_EMB = "embedding"

MILVUS_SEARCH_PARAMS = {"metric_type": "IP", "params": {"ef": 64}}

# =========================
# MySQL 설정 (.env 파일에서 로드)
# =========================
MYSQL_HOST = os.getenv("DB_HOST")
MYSQL_PORT = int(os.getenv("DB_PORT"))
MYSQL_USER = os.getenv("DB_USER")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD")
MYSQL_DB = os.getenv("DB_NM")

# 조회할 테이블
MYSQL_TABLE = "vc_ntb_a_tb_0001"
MYSQL_TABLE2 = "vc_ntb_a_tb_0001_summary"


# 컬럼명 매핑
NAME_DICT = {
    'NTB_A_F_034': '개발자회사',
    'NTB_A_F_014': '기술명',
    'NTB_A_F_009': '산업분류_대분류 > 산업분류_중분류 > 산업분류_세분류',
    'NTB_A_F_011': '기술분류_세분류',
    'NTB_A_F_018': '키워드',
    'NTB_A_F_021': '개발상태',
    'NTB_A_F_024': '희망거래유형',
    'NTB_A_F_001': '판매기술코드',
    'NTB_A_F_019': '기술문서상세내용',
    '카테고리 분류': '추천유형',
    'src_sent_list': 'src_sent_list',
    'key_sent_list': 'key_sent_list'
}


def mysql_connect():
    """SQLAlchemy engine 생성 with Connection Pool"""
    password_encoded = quote_plus(MYSQL_PASSWORD)

    connection_string = (
        f"mysql+pymysql://{MYSQL_USER}:{password_encoded}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        f"?charset=utf8mb4"
    )
    return create_engine(
        connection_string,
        pool_size=30,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=3600,
        pool_pre_ping=True
    )


class RecommendationRetriever:
    """
    추천유형별 검색 결과를 반환하는 Retriever
    - add_ntis_ntb_data 함수와 동일한 형식으로 반환
    """

    def __init__(self):
        # 1) 임베더
        self.embedder = SentenceTransformer(EMBED_MODEL_NAME)

        # 2) Milvus 연결
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)
        self.col = Collection(COLLECTION_NAME)
        self.col.load()

        # 3) SQLAlchemy engine
        self._engine = mysql_connect()

    def close(self):
        try:
            self._engine.dispose()
        except Exception:
            pass

    def _fetch_details_from_mysql(self, codes: List[str], tag: str) -> pd.DataFrame:
        """
        판매기술코드 리스트로 DB 조회 후 추천유형과 순위 추가

        Args:
            codes: 판매기술코드 리스트
            tag: 추천유형 (추천문서 태그 번호)

        Returns:
            DataFrame with 순위, 추천유형 컬럼 포함
        """
        if not codes:
            return pd.DataFrame()

        df_list = []

        for rank, code in enumerate(codes, 1):
            sql = f"""
                SELECT a.NTB_A_F_034, a.NTB_A_F_014, 
                       concat(a.NTB_A_F_005, ' > ', a.NTB_A_F_007, ' > ', a.NTB_A_F_009) AS NTB_A_F_009, 
                       a.NTB_A_F_011, 
                       a.NTB_A_F_018, 
                       a.NTB_A_F_021, 
                       a.NTB_A_F_024, 
                       a.NTB_A_F_001, 
                       a.NTB_A_F_019,
                       b.`카테고리 분류`,
                       '' as src_sent_list,
                       '[]' as key_sent_list
                FROM {MYSQL_TABLE} a
                join {MYSQL_TABLE2} b
                on a.NTB_A_F_001 = b.판매기술코드
                WHERE a.NTB_A_F_001 = :code
            """



            with self._engine.connect() as conn:
                df = pd.read_sql(text(sql), conn, params={"code": code})

            if not df.empty:
                # 카테고리가 DB에서 이미 조회되므로 수동으로 추가하지 않음
                # tag 파라미터는 카테고리가 NULL일 경우의 기본값으로만 사용
                if tag and (df['`카테고리 분류`'].isna().any() or df['`카테고리 분류`'].eq('').any()):
                    df['`카테고리 분류`'] = df['`카테고리 분류`'].fillna(tag).replace('', tag)

                df.insert(0, '순위', rank)
                df_list.append(df)

        if df_list:
            return pd.concat(df_list, ignore_index=True)
        else:
            return pd.DataFrame()

    def search_single(
            self,
            text: str,
            tag: str,
            top_k: int = 10
    ) -> Dict[str, Any]:
        """
        단일 쿼리 검색

        Args:
            text: 검색 쿼리
            tag: 추천유형 태그
            top_k: 반환할 결과 개수

        Returns:
            {"dataframe": DataFrame, "dict": dict} 형식으로 반환
        """
        # 임베딩
        qvec = self.embedder.encode(text, convert_to_numpy=True).astype(np.float32)

        # Milvus 검색
        res = self.col.search(
            data=[qvec.tolist()],
            anns_field=FIELD_EMB,
            param=MILVUS_SEARCH_PARAMS,
            limit=top_k,
            output_fields=[FIELD_ID],
        )

        # 판매기술코드 수집
        codes = []
        for hit in res[0]:
            doc_id = hit.entity.get(FIELD_ID)
            if doc_id:
                codes.append(str(doc_id).strip())

        # MySQL에서 상세 정보 조회
        df = self._fetch_details_from_mysql(codes, tag)

        # 컬럼명 한글로 변경
        if not df.empty:
            df.rename(columns=NAME_DICT, inplace=True)

        # dict 형식으로도 반환
        return {
            "dataframe": df,
            "dict": df.to_dict(orient='dict') if not df.empty else {}
        }

    def search_multiple(
            self,
            queries: List[str],
            tags: List[str],
            top_k: int = 10
    ) -> Dict[str, Any]:
        """
        여러 쿼리 검색 (add_ntis_ntb_data와 동일한 형식)

        Args:
            queries: 검색 쿼리 리스트
            tags: 추천유형 태그 리스트 (queries와 같은 길이)
            top_k: 각 쿼리당 반환할 결과 개수

        Returns:
            {"dataframe": DataFrame, "dict": dict}
        """
        df_list = []

        for query, tag in zip(queries, tags):
            result = self.search_single(query, tag, top_k)
            df = result["dataframe"]
            if not df.empty:
                df_list.append(df)

        if df_list:
            df_all = pd.concat(df_list, ignore_index=True)

            return {
                "dataframe": df_all,
                "dict": df_all.to_dict(orient='dict')
            }
        else:
            return {
                "dataframe": pd.DataFrame(),
                "dict": {}
            }

    def search_from_result(
            self,
            result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        기존 result dict 형식을 받아서 처리
        (add_ntis_ntb_data 함수의 입력 형식과 동일)

        Args:
            result: {
                '추천문서 태그 번호': [tag1, tag2, ...],
                '유사문서 doc id': ["['code1', 'code2']", "['code3', 'code4']", ...]
            }

        Returns:
            {"dataframe": DataFrame, "dict": dict}
        """
        import ast

        df_list = []

        tags = result.get('추천문서 태그 번호', [])
        doc_ids = result.get('유사문서 doc id', [])

        for tag, doc_id_str in zip(tags, doc_ids):
            # 문자열을 리스트로 변환
            codes = ast.literal_eval(doc_id_str) if isinstance(doc_id_str, str) else doc_id_str

            # MySQL에서 조회
            df = self._fetch_details_from_mysql(codes, tag)
            if not df.empty:
                df_list.append(df)

        if df_list:
            df_all = pd.concat(df_list, ignore_index=True)
            # 컬럼명 한글로 변경
            df_all.rename(columns=NAME_DICT, inplace=True)

            return {
                "dataframe": df_all,
                "dict": df_all.to_dict(orient='dict')
            }
        else:
            return {
                "dataframe": pd.DataFrame(),
                "dict": {}
            }


# ========================================
# 싱글톤 인스턴스 관리
# ========================================
_retriever_instance = None


def get_factory():
    """
    RecommendationRetriever 싱글톤 인스턴스 반환
    """
    global _retriever_instance

    if _retriever_instance is None:
        _retriever_instance = RecommendationRetriever()

    return _retriever_instance


if __name__ == "__main__":
    r = RecommendationRetriever()

    try:
        # 방법 1: 직접 쿼리 검색
        print("=" * 100)
        print("방법 1: 직접 쿼리 검색")
        print("=" * 100)

        queries = [
            "전기차 배터리 수명 연장과 BMS 알고리즘 개선",
            "AI 기반 에너지 관리 시스템"
        ]
        tags = ["추천1", "추천2"]

        result1 = r.search_multiple(queries, tags, top_k=5)
        print(f"\n검색 결과: {len(result1['dataframe'])}건")
        if not result1['dataframe'].empty:
            print(result1['dataframe'][['순위', '추천유형', '판매기술코드', '기술명']].to_string(index=False))

    finally:
        r.close()