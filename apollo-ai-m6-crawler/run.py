# streamlit run run.py
import streamlit as st

# 표지 제목
st.markdown("# 📊 M6 수집 프로그램")
st.markdown("### 데이터 수집 도구 - 사용 설명서")

# 목차/사용법
st.markdown("""
---

## 📌 목차
1. **WIKI DUMP DOWNLOAD**
2. **WIKI EDIT DOWNLOAD**
3. **MAKE ITEM LIST**
4. **WIKI PAGEVIEW CRAWL**
5. **WIKI MAKE TABLE**

---

## 🛠 사용법
- 좌측 메뉴에서 기능을 선택합니다.
1. WIKI DUMP DOWNLOAD
- Item Dump 수집과 데이터 처리의 기능을 가지고 있으며 수집된 파일에 대한 데이터 처리 후 적재를 제공합니다.
- 테스트 용 버튼을 통해 일부 파일만 진행 가능합니다.
2. WIKI EDIT DOWNLOAD
- Edit Dump 수집 -> 데이터 처리 -> 연도별 집계 -> DB 적재의 기능을 가지고 있으며, 순서대로 진행됩니다.
- DB 적재 시에는 ITEMMASTER의 ITEM을 기반으로 합니다.
3. MAKE ITEM LIST
- Item List 생성 -> 진입 Seed 확장 -> Item Filter -> Seealso Filter 의 기능을 가지고 있습니다.
4. MAKE ITEM LIST
- Pageview 수집의 기능을 가지고 있으며 신규 아이템에 대한 전체 기간 수집과 기존 아이템에 대한 업데이트만 수집도 가능합니다.
- ITEMMASTER의 ITEM을 기반으로 합니다.
5. WIKI MAKE TABLE
- PageRank 테이블 생성 -> 연도별 지표 테이블 생성 -> Item list 테이블 생성 -> Search 테이블 생성의 기능을 가지고 있습니다.
---

""")