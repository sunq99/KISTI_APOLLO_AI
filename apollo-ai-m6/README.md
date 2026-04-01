## Apollo Model6 Endpoints Server

- Apollo Back-End에서 호출하는 글로벌 유망 아이템 탐색용 Endpoints 서버
- FastAPI 기반 REST API 서버로, 내부적으로 apollo-ai-m6-chroma 서버를 호출

### 모듈 연계 구조

```
Apollo Back-End
    └─▶ apollo-ai-m6 (Endpoints Server)
             └─▶ apollo-ai-m6-chroma (ChromaDB 유사도 검색)
```

### API

#### `POST /itemsearch` — 글로벌 유망 아이템 탐색

1. apollo-ai-m6-chroma 서버로 기업명 또는 설명 기반 유사도 검색 요청
2. 검색 결과와 DB의 WIKI 정보를 병합하여 최종 결과 반환
