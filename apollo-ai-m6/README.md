## Apollo Model6 Endpoints Server

- Apollo Back-End 에서 호출하는 Endpoints Server
- /itemsearch API는 글로벌 유망 아이템 탐색으로 다음과 같이 처리함 
  - Apollo Model6 Chroma Server로 호출
  - 기업명 또는 설명으로 유사도 랭크에 따라 Response 데이터와 DB WIKI를 병합하여 결과 전송