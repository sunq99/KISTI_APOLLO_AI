from src.vector_db import get_collection
from src.search_engine import search


# ---------------------------------------------------------------------
# kwd: 문장/설명 기반(언어 라우팅 유지)
# ---------------------------------------------------------------------
def kwd(keyword, k=100):
    """
    언어 자동 라우팅 + 유사도 상위 제목 리스트 반환 (Chroma)
    """
    db = get_collection(keyword)
    if db is None:
        return []

    try:
        docs = db.similarity_search_with_relevance_scores(keyword, k=k)
    except Exception as e:
        print("[kwd] Chroma similarity search failed:", e)
        return []

    suggestions = []
    for _, re_ in enumerate(docs):
        try:
            title = re_[0].dict()['metadata'].get('title') or re_[0].dict()['metadata'].get('event_nm') or ""
            suggestions.append(title)
        except Exception:
            pass
    return suggestions

# ---------------------------------------------------------------------
# kwdForName: 이름/키워드 중심 검색 — ES 사용
# ---------------------------------------------------------------------
def kwdForName(keyword, k=100):
    """
    이름/키워드 검색: **Elasticsearch 전용**.
    ES에서 결과를 받아 중복 제거 후 상위 k개 제목 리스트를 반환합니다.
    ES 사용 불가/예외 시 빈 리스트를 반환합니다.
    """
    q = (keyword or "").strip()
    if not q:
        return []

    try:
        es_results = search(q, size=k)
        if not es_results:
            print('[kwdForName] ES returned no results')
            return []

        # dedupe while preserving order
        seen = set()
        out = []
        for t in es_results:
            if not t:
                continue
            key = t
            if key and key not in seen:
                seen.add(key)
                out.append(t)
            if len(out) >= k:
                break
        print(out)
        print(f'[kwdForName] ES returned {len(out)} deduped results')
        return out[:k]
    except Exception as e:
        print("[kwdForName] ES search exception:", e)
        return []

# 기존 코드의 맨 마지막 부분에 추가
# if __name__ == "__main__":
#     init()
#     print("-" * 30)
#
#     # 1. Chroma 벡터 검색 (kwd) 테스트 - 한국어
#     test_query_ko = "바이오 잉크는"
#     print(f"**테스트 1: Chroma (KO) 검색 — Query: '{test_query_ko}'**")
#     results_ko = kwd(test_query_ko, k=5)
#     print(f"검색 결과 ({len(results_ko)}개): {results_ko}")
#     print("-" * 30)
#
#     # 2. Chroma 벡터 검색 (kwd) 테스트 - 영어
#     test_query_en = "3d printing is"
#     print(f"**테스트 2: Chroma (EN) 검색 — Query: '{test_query_en}'**")
#     results_en = kwd(test_query_en, k=5)
#     print(f"검색 결과 ({len(results_en)}개): {results_en}")
#     print("-" * 30)
#
#     # 3. Elasticsearch 이름 검색 (kwdForName) 테스트
#     test_name = "바이오잉크"
#     print(f"**테스트 3: Elasticsearch 이름 검색 — Query: '{test_name}'**")
#     results_es = kwdForName(test_name, k=5)
#     print(f"검색 결과 ({len(results_es)}개): {results_es}")
#     print("-" * 30)