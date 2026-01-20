import logging
from elasticsearch import Elasticsearch, helpers
from src.config import settings
from src.utils import is_kor


logger = logging.getLogger(__name__)

# ------------------------------------- Elastic Search Setup --------------------------------------------------
es_client = None

# ---------------------------------------------------------------------------------------
# Elastic Search Init
# ---------------------------------------------------------------------------------------
def init_search_engine():
    global es_client
    es_client = Elasticsearch([settings.ES_URL])
    if not es_client.ping():
        raise Exception("Elasticsearch connection failed")

# ---------------------------------------------------------------------------------------
# Elastic Search Search Query 조회
# ---------------------------------------------------------------------------------------
def get_search_query(query: str, size: int = 30):
    is_korean = is_kor(query)
    if is_korean:
        fields = ["event_nm^3", "title_kor^3", "title^1", "title.std^1"]
    else:
        fields = ["title.std^4", "title^3", "event_nm^2", "title_kor^1"]

    body = {
        "query": {
            "bool": {
                "should": [
                    {"match_phrase": {"event_nm": {"query": query, "boost": 5}}},
                    {"multi_match": {
                        "query": query,
                        "fields": fields,
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }},
                    {"match": {"event_nm": {"query": query, "operator": "or", "fuzziness": "AUTO"}}}
                ]
            }
        },
        "size": size
    }
    return body

# ---------------------------------------------------------------------------------------
# Elastic Search Search Query 조회
# ---------------------------------------------------------------------------------------
def search(query: str, size: int = 30):
    if es_client is None:
        logger.error("Elasticsearch client None")
        return []

    body = get_search_query(query, size=size)
    try:
        resp = es_client.search(index=settings.ES_INDEX, body=body)
    except Exception as e:
        logger.error("[ES] direct search failed:", e)
        return []

    hits = resp.get("hits", {}).get("hits", [])
    results = []
    for h in hits:
        src = h.get("_source", {}) or {}
        # 무조건 영문명으로 조회하도록 되어 있음 (m6 endpoint에서)
        results.append(src.get("title"))
    return results

# ---------------------------------------------------------------------------------------
# Index 생성
# ---------------------------------------------------------------------------------------
def create_wiki_index():
    index_name = settings.ES_INDEX

    settings_with_title_std = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "custom_nori_analyzer": {
                        "type": "custom",
                        "tokenizer": "nori_tokenizer",
                        "filter": ["nori_readingform", "custom_pos_filter"]
                    }
                },
                "tokenizer": {
                    "nori_tokenizer": {
                        "type": "nori_tokenizer",
                        "decompound_mode": "mixed"
                    }
                },
                "filter": {
                    "custom_pos_filter": {
                        "type": "nori_part_of_speech",
                        "stoptags": ["JKS", "JKC", "JKG", "JKO", "JKB", "JKQ", "JX", "JC",
                                     "EP", "EC", "EF", "ETN", "ETM"]
                        }
                    }
                }
            },
        "mappings": {
            "properties": {
                "event_nm": {"type": "text", "analyzer": "custom_nori_analyzer"},
                "region_nm": {"type": "text", "analyzer": "custom_nori_analyzer"},
                "regist_id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "custom_nori_analyzer",
                    "fields": {"std": {"type": "text", "analyzer": "standard"}}
                },
                "title_kor": {"type": "text", "analyzer": "custom_nori_analyzer"}
            }
        }
    }

    try:
        exists = es_client.indices.exists(index=index_name)
    except Exception:
        exists = False

    if exists:
        logger.info(f"기존 인덱스 삭제: {index_name}")
        try:
            es_client.indices.delete(index=index_name)
            logger.info(f"{index_name} 삭제 완료")
        except Exception as e:
            logger.error(f"{index_name} 인덱스 삭제 실패:")
            raise e

    logger.info(f"Nori 설정으로 인덱스 생성 시도: {index_name}")
    es_client.indices.create(index=index_name, body=settings_with_title_std)
    logger.info(f"Nori analyzer로 인덱스 생성됨: {index_name}")

# ---------------------------------------------------------------------------------------
# Index 갱신
# ---------------------------------------------------------------------------------------
def update_wiki_index(rows):
    index_name = settings.ES_INDEX

    indexes = []
    for row in rows:
        indexes.append({
            "_op_type": "index",
            "_index": index_name,
            "_id": str(row['id']),
            "_source": {
                "title": row['title'],
                "title_kor": row['title_kor'],
                "regist_id": str(row['id'])
            }
        })

    try:
        success, _ = helpers.bulk(es_client, indexes, chunk_size=len(rows), stats_only=True)
        logger.info(f"인덱스 갱신 {success} 건")
    except Exception as e:
        logger.error(f"인덱스 갱신 실패: {e}")
        raise