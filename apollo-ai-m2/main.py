import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import JSONResponse
import time
import uvicorn
import logging
import warnings
import sys

# 현재 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# optional device listing
try:
    from tensorflow.python.client import device_lib
    _HAS_DEVICE_LIB = True
except Exception:
    device_lib = None
    _HAS_DEVICE_LIB = False

# dotenv
env_name = os.environ.get("APP_ENV")
load_dotenv(dotenv_path=f'.env.{env_name}' if env_name else '.env')

# 로거 설정
app_logger = logging.getLogger('app')
app_logger.setLevel(logging.DEBUG)

log_path = os.getenv("LOG_PATH") or "."
os.makedirs(log_path, exist_ok=True)

if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith('debug.log') for h in app_logger.handlers):
    formatter = logging.Formatter("[%(asctime)s] %(name)s:%(levelname)s - %(message)s")
    app_handler = logging.FileHandler(f'{log_path}/debug.log')
    app_handler.setFormatter(formatter)
    app_logger.addHandler(app_handler)

# app 내부 모듈 로드
get_factory = None

try:
    app_logger.info("df_out 모듈 import 시도")
    from app.df_out import get_factory
    app_logger.info("✓ app.df_out import 성공")
except ImportError as e:
    app_logger.warning(f"app.df_out import 실패: {e}")
    try:
        from df_out import get_factory
        app_logger.info("✓ df_out import 성공")
    except ImportError as e:
        app_logger.error(f"df_out import 실패: {e}")
        try:
            from .df_out import get_factory
            app_logger.info("✓ 상대 경로 import 성공")
        except ImportError as e:
            app_logger.error(f"모든 import 시도 실패: {e}")

warnings.filterwarnings(action='ignore')

origins = ["*"]

# 전역 retriever 인스턴스 저장
_retriever = None


class TechDocItem(BaseModel):
    text: str


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _retriever

    app_logger.info("=== Application 시작 ===")
    app_logger.info(f"get_factory 함수 존재: {get_factory is not None}")

    try:
        if get_factory is not None:
            app_logger.info("RecommendationRetriever 초기화 시작...")
            _retriever = get_factory()
            app_logger.info("✓ RecommendationRetriever 초기화 완료")
        else:
            app_logger.error("✗ get_factory 함수가 None입니다. df_out.py import를 확인하세요.")
            app_logger.error(f"현재 작업 디렉토리: {os.getcwd()}")
            app_logger.error(f"Python 경로: {sys.path[:3]}")
    except Exception as e:
        app_logger.exception("RecommendationRetriever 초기화 중 오류 발생")

    yield

    # cleanup
    app_logger.info("=== Application 종료 ===")
    if _retriever and hasattr(_retriever, 'close'):
        try:
            _retriever.close()
            app_logger.info("✓ RecommendationRetriever 종료 완료")
        except Exception as e:
            app_logger.exception("RecommendationRetriever 종료 중 오류")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/')
def read_root():
    if _HAS_DEVICE_LIB and device_lib:
        devices = device_lib.list_local_devices()
        return {'GPU 사용가능여부': str(devices)}
    else:
        try:
            import torch
            return {
                'GPU 사용가능여부': {
                    'torch_cuda_available': torch.cuda.is_available(),
                    'cuda_count': torch.cuda.device_count()
                }
            }
        except Exception:
            return {'GPU 사용가능여부': 'device information not available'}


@app.post('/api/model2/predict/detail')
async def predict_detail(item: TechDocItem, request: Request):
    st = time.time()
    client_ip = request.client.host if request.client else "unknown"
    app_logger.info(f'{os.getpid()}: predict/detail 호출 from {client_ip}')
    app_logger.info(f'요청 텍스트: {item.text[:100]}...')

    dicted_item = item.dict()

    try:
        if _retriever is None:
            app_logger.error("_retriever가 None입니다. 초기화 로그를 확인하세요.")
            return JSONResponse(
                {
                    "error": "RecommendationRetriever가 초기화되지 않았습니다.",
                    "detail": "서버 로그를 확인하세요. df_out.py import 실패 가능성이 있습니다."
                },
                status_code=500
            )

        app_logger.info("검색 시작...")

        # search_single은 dict를 반환
        result = _retriever.search_single(
            text=dicted_item['text'],
            tag='',
            top_k=10
        )

        # result는 {"dataframe": df, "dict": dict} 형식
        # API 응답으로는 dict 부분만 반환
        response_data = result.get("dict", {})

        result_count = len(response_data) if isinstance(response_data, list) else 0

        app_logger.info(f'검색 완료: {result_count}건')
        app_logger.info(f'처리 시간: {round(time.time() - st, 2)}초')

        return JSONResponse(response_data)

    except Exception as e:
        app_logger.exception("predict_detail 처리 중 오류")
        return JSONResponse(
            {
                "error": "predict_detail failed",
                "detail": str(e),
                "type": str(type(e).__name__)
            },
            status_code=500
        )


@app.get('/health')
async def health_check():
    """헬스체크 엔드포인트"""
    return {
        "status": "healthy",
        "retriever_initialized": _retriever is not None,
        "get_factory_available": get_factory is not None
    }


@app.get('/debug/info')
async def debug_info():
    """디버깅 정보 엔드포인트"""
    return {
        "current_dir": os.getcwd(),
        "file_location": __file__,
        "sys_path": sys.path[:5],
        "df_out_exists": os.path.exists(os.path.join(current_dir, "df_out.py")),
        "get_factory": get_factory is not None,
        "_retriever": _retriever is not None
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )