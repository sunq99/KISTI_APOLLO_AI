import logging
import chromadb
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from src.config import settings
from src.utils import is_kor


logger = logging.getLogger(__name__)

# ------------------------------------- ChromaDB Setup --------------------------------------------------
client = None
collection_cache = {
    'en': None,
    'ko': None
}

# ---------------------------------------------------------------------------------------
# Vector DB Init
# ---------------------------------------------------------------------------------------
def init_vector_db():
    global client
    client = chromadb.PersistentClient(path=settings.CHROMADB_PATH)

# ---------------------------------------------------------------------------------------
# 언어별 모델 조회
# ---------------------------------------------------------------------------------------
def _get_model_by_lang(is_korean: bool):
    if is_korean:
        return "jhgan/ko-sroberta-multitask"
    else:
        return "sentence-transformers/all-mpnet-base-v2"

# ---------------------------------------------------------------------------------------
# 언어별 모델 조회
# ---------------------------------------------------------------------------------------
def get_collection(query: str = None, is_korean: bool = False, use_cache: bool = True):
    global client
    if client is None:
        init_vector_db()

    if query is not None:
        is_korean = is_kor(query)

    collection_name = "ko" if is_korean else "en"

    if use_cache and collection_cache[collection_name] is not None:
        logger.info(f"Using cached {collection_name} collection")
        return collection_cache[collection_name]

    logger.info(f"Create cache {collection_name} collection")
    embeddings = HuggingFaceEmbeddings(model_name=_get_model_by_lang(is_korean),
                                       model_kwargs={'device': settings.DEVICE},
                                       encode_kwargs={'normalize_embeddings': True})

    collection = Chroma (
        client=client,
        collection_name=settings.CHROMA_COLLECTION_KO if is_korean else settings.CHROMA_COLLECTION,
        embedding_function=embeddings
    )
    collection_cache[collection_name] = collection

    return collection