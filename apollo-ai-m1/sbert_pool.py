# -*- coding: utf-8 -*-
"""
sbert_pool.py
- SentenceTransformer(pro-sroberta) 단일 인스턴스를 프로세스 내에서 재사용하기 위한 로더
- 로컬 경로 우선 → (옵션) HF 토큰으로 다운로드
- thread-safe + LRU 캐시(최대 1개 모델)

사용법:
    from sbert_pool import get_sbert, encode_text

    model = get_sbert("/path/to/pro-sroberta")
    vec = encode_text("키워드;키워드2", model=model)  # 또는 encode_text(..., model_name="...")

환경변수:
    SBERT_MODEL_NAME: 로컬 경로 또는 HF 모델 ID
    HUGGINGFACE_HUB_TOKEN / HF_TOKEN: HF 토큰
"""
from __future__ import annotations

import os
from typing import Optional, Tuple
from functools import lru_cache
from threading import Lock

import numpy as np

try:
    from sentence_transformers import SentenceTransformer
    _SBERT_AVAILABLE = True
except ImportError as e:
    import sys
    _SBERT_AVAILABLE = False
    print(f"Python executable: {sys.executable}")
    print(f"Python path: {sys.path}")
    raise RuntimeError(f"sentence-transformers import failed: {str(e)}")

# pro-sroberta 로컬 경로 자동 탐색 후보
CANDIDATE_LOCAL_MODEL_DIRS = [
    "/home/dev23_apollo/ai/model1/model/pro-sroberta",
    "/home/dev23_apollo/ai/model1/pro-sroberta",
    "/root/.cache/torch/sentence_transformers/pro-sroberta",
    "/home/dev23_apollo/.cache/torch/sentence_transformers/pro-sroberta",
    "./pro-sroberta"
]

_LOCK = Lock()


def l2_normalize(v: np.ndarray) -> np.ndarray:
    v = np.asarray(v, dtype=np.float32)
    n = np.linalg.norm(v)
    if n == 0:
        return v
    return v / n


def resolve_sbert_model(
    model_name_opt: Optional[str],
    allow_hf_download: bool,
) -> Tuple[str, Optional[str]]:
    """
    최적의 SBERT 모델 경로/ID를 결정
    반환: (path_or_id, hf_token)
    우선순위:
      1) model_name_opt 인자(경로면 로컬 사용, 아니면 HF ID)
      2) SBERT_MODEL_NAME 환경변수(경로면 로컬 사용, 아니면 HF ID)
      3) 로컬 후보 폴더 자동 탐색(CANDIDATE_LOCAL_MODEL_DIRS)
      4) allow_hf_download=True 인 경우 HF ID 'sentence-transformers/pro-sroberta'
    """
    env_name = os.getenv("SBERT_MODEL_NAME")
    hf_token_env = os.getenv("HUGGINGFACE_HUB_TOKEN") or os.getenv("HF_TOKEN")
    hf_token = hf_token_env  # 인자 기반 토큰은 get_sbert에서 적용

    # 1) CLI 인자
    if model_name_opt:
        if os.path.isdir(model_name_opt):
            return model_name_opt, None  # 로컬 디렉토리
        if allow_hf_download:
            return model_name_opt, hf_token

    # 2) ENV
    if env_name:
        if os.path.isdir(env_name):
            return env_name, None
        if allow_hf_download:
            return env_name, hf_token

    # 3) 로컬 후보 자동 탐색
    for p in CANDIDATE_LOCAL_MODEL_DIRS:
        if os.path.isdir(p):
            return p, None

    # 4) HF 다운로드 시도 (최후)
    if allow_hf_download:
        return "sentence-transformers/pro-sroberta", hf_token

    raise RuntimeError(
        "로컬 'pro-sroberta' 모델을 찾을 수 없습니다.\n"
        " - --sbert_model /path/to/pro-sroberta  (로컬)\n"
        " - SBERT_MODEL_NAME=/path/to/pro-sroberta (env)\n"
        " - 또는 --allow_hf_download 와 HUGGINGFACE_HUB_TOKEN 설정"
    )


@lru_cache(maxsize=1)
def _cached_load(path_or_id: str, allow_hf_download: bool, hf_token: Optional[str]) -> "SentenceTransformer":
    if not _SBERT_AVAILABLE:
        raise RuntimeError("sentence-transformers가 필요합니다. (pip install sentence-transformers)")
    # use_auth_token은 공개 모델이면 None이어도 무방
    model = SentenceTransformer(path_or_id, device="cpu", use_auth_token=hf_token)
    model.eval()
    return model


def get_sbert(
    model_name: Optional[str] = None,
    allow_hf_download: bool = False,
    hf_token: Optional[str] = None
) -> "SentenceTransformer":
    """
    프로세스 내 단일 인스턴스 반환 (최초 1회 로드, 이후 캐시 재사용)
    """
    path_or_id, token_env = resolve_sbert_model(model_name, allow_hf_download)
    token_final = hf_token or token_env
    # lru_cache는 스레드 경쟁 상태에서 중복 로드를 일으킬 수 있으므로 락으로 보호
    with _LOCK:
        return _cached_load(path_or_id, allow_hf_download, token_final)


def encode_text(
    text: str,
    model: Optional["SentenceTransformer"] = None,
    model_name: Optional[str] = None,
    allow_hf_download: bool = False,
    hf_token: Optional[str] = None
) -> np.ndarray:
    """
    텍스트를 임베딩하고 L2 정규화하여 반환(np.float32).
    """
    if model is None:
        model = get_sbert(model_name=model_name, allow_hf_download=allow_hf_download, hf_token=hf_token)
    emb = model.encode(text, convert_to_numpy=True, show_progress_bar=False)
    return l2_normalize(emb).astype(np.float32)
