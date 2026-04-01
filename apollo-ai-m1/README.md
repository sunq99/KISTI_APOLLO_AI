## Apollo Model1 API Server

- 유망 사업화 국가 R&D 예측 및 수요기업 예측 모델
- NTIS R&D 문서와 NICE 기업정보를 임베딩하여 Milvus 벡터 DB에 저장
- 기존 키워드 매칭 방식에서 임베딩 벡터 기반 코사인 유사도 검색 방식으로 전환
- FastAPI 기반 REST API 서버로 Apollo Back-End의 예측 요청을 처리
