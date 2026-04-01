# KISTI (한국과학기술정보연구원)
## KISTI_APOLLO.dev

### 개요 (Overview)
이 저장소는 KISTI APOLLO 프로젝트를 위해 개발된 소스 코드와 데이터 처리 파이프라인을 포함하고 있습니다.

- **프로젝트 기간**: 2025.09.08 ~ 2026.01.01
- **역할**: 데이터 엔지니어 & 프로그래머

---

### 기술 스택 (Tech Stack)

| 카테고리 | 기술 |
|---------|------|
| 언어 | Python, SQL |
| 데이터 엔지니어링 | Pandas, NumPy, MySQL / MariaDB |
| AI / 자연어 처리 | Hugging Face Transformers, Sentence-Transformers, LLM (Gemma, LLaMA via Ollama) |
| 벡터 데이터베이스 | Milvus (HNSW, IVF), ChromaDB |
| 백엔드 | FastAPI, Uvicorn, Streamlit |
| 인프라 | Linux, Docker, NVIDIA GPU (CUDA), Jenkins |
| 도구 | GitHub, Postman, Gitea, Elasticsearch |

---

### 시스템 아키텍처 (System Architecture)

```
[데이터 수집]                [임베딩 & 벡터 DB]          [API 제공]
apollo-ai-m6-crawler  ──▶  apollo-ai-m6-chroma  ──▶  apollo-ai-m6
                                                           │
                                               Apollo Back-End 연계
                                                     │         │
                                             apollo-ai-m1  apollo-ai-m2
```

데이터 수집 → LLM 기반 요약·분류 → 임베딩 생성 → 벡터 데이터베이스(Milvus / ChromaDB) 저장 → 검색 및 추천 API 제공

---

### 저장소 구조 (Repository Structure)

| 모듈 | 기능 |
|------|------|
| [apollo-ai-m1](./apollo-ai-m1) | **〈유망 사업화 국가 R&D 예측〉** — 임베딩된 NTIS R&D 문서와 NICE 기업 정보 간 코사인 유사도 검색으로 유망 사업화 가능성 예측 |
| [apollo-ai-m2](./apollo-ai-m2) | **〈이전 가능 기술 추천〉** — NTIS 문서와 임베딩된 R&D 문서 간 코사인 유사도 검색으로 관련 기술 추천 |
| [apollo-ai-m6](./apollo-ai-m6) | **〈글로벌 유망 아이템 탐색〉** — Apollo Back-End의 요청을 받아 m6-chroma를 호출하고 DB WIKI 정보를 병합하여 결과 반환 |
| [apollo-ai-m6-chroma](./apollo-ai-m6-chroma) | **〈벡터 유사도 검색〉** — ChromaDB 기반 임베딩 저장 및 검색. 영어는 임베딩 기반, 한글은 Elasticsearch 기반 검색 |
| [apollo-ai-m6-crawler](./apollo-ai-m6-crawler) | **〈위키피디아 데이터 수집〉** — Streamlit UI 기반 단계별 Wikipedia 데이터 크롤링 전용 모듈 |
