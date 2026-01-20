# KISTI (한국과학기술정보연구원)
## KISTI_APOLLO.dev

### 개요 (Overview)
이 저장소는 KISTI APOLLO 프로젝트를 위해 개발된 소스 코드와 데이터 처리 파이프라인을 포함하고 있습니다.

- **프로젝트 기간**: 2025.09.08 ~ 2026.01.01  
- **역할**: 데이터 엔지니어 & 프로그래머  

### 기술 스택 (Tech Stack)
- **언어**: Python, SQL  
- **데이터 엔지니어링**: Pandas, NumPy, MySQL / MariaDB  
- **AI / 자연어 처리**: Hugging Face Transformers, Sentence-Transformers, LLM (Gemma, LLaMA via Ollama)  
- **벡터 데이터베이스**: Milvus (HNSW, IVF), Chroma  
- **백엔드**: FastAPI, Uvicorn  
- **인프라**: Linux, Docker, NVIDIA GPU (CUDA), Jenkins
- **도구**: GitHub, Postman, Gitea  

### 시스템 아키텍처 (System Architecture)
데이터 수집 → LLM 기반 요약·분류 → 임베딩 생성 → 벡터 데이터베이스(Milvus / Chroma) 저장 → 검색 및 추천 API 제공

### 저장소 구조 (Repository Structure)

본 저장소는 APOLLO 프로젝트의 일환으로 개발된 여러 기능별 모듈로 구성되어 있습니다.

- **apollo-ai-m1**  
  임베딩된 NTIS R&D 문서와 NICE에 적재된 기업 정보를 기반으로 코사인 유사도 검색을 수행하여,  
  기업의 유망한 사업화 가능성을 예측하는 APOLLO 기능인  
  **〈유망 사업화 국가 R&D 예측〉** 모듈

- **apollo-ai-m2**  
  NTIS 문서와 임베딩된 R&D 문서 간 코사인 유사도 검색을 통해 관련 기술을 추천하는  
  APOLLO 기능인  
  **〈이전 가능 기술 추천〉** 파이프라인 모듈

- **apollo-ai-m6**  
  위키피디아 데이터를 12개 국가전략기술 분야로 분류하고,  
  각 기술 분야별 TOP 100 아이템을 추천하는 APOLLO 기능인  
  **〈글로벌 유망 아이템 탐색〉** 을 담당하는 모듈

- **apollo-ai-m6-crawler**  
  위키피디아 데이터를 수집하기 위한 크롤링 전용 모듈

- **apollo-ai-m6-chroma**  
  위키피디아 데이터를 대상으로 임베딩 생성 및 유사도 검색을 수행하는 벡터 데이터베이스 모듈로,  
  **〈아이템 검색〉** 기능에서는 검색어가 영어인 경우 영어 임베딩 기반 유사도 검색을,  
  검색어가 한글인 경우 Elasticsearch 기반 유사도 검색을 수행하며,  
  **〈설명 기반 검색〉** 기능에서는 한글·영문 설명을 각각 임베딩하여  
  검색어 언어에 따라 대응되는 임베딩 공간에서 유사도 검색을 수행함
