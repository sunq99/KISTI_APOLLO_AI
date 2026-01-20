import streamlit as st
import datetime
import module.proc_wiki_pageview as pwp
import config.path as path

CONFIG_PATH=path.conf_path

st.title("WIKI PAGEVIEW 수집")

if 'crawl_wiki_pageview' not in st.session_state:
    st.session_state['crawl_wiki_pageview'] = [False,False] # 0 : set값, 1: 버튼 클릭

current_year = datetime.datetime.now().year
update_yn = st.toggle("업데이트만")
if update_yn:
    years = list(range(2015, current_year))
    year_range = st.select_slider(
        "기간 선택",
        options=years,
        value=(2015, current_year-1)
    )
    from_year=year_range[0]
    to_year=year_range[1]
    only_update='y'
    st.write(f"선택한 연도 : {from_year} ~ {to_year} 로 업데이트 진행합니다.")
    st.session_state['crawl_wiki_pageview'][0]=True
else:
    from_year=2015
    to_year=current_year-1
    only_update='n'
    st.write(f"신규 아이템에 대한 {from_year} ~ {to_year} 로 수집 진행합니다.")
    st.session_state['crawl_wiki_pageview'][0]=True

if st.button('Pageview 수집', key="button_crawl_pageview_1",disabled=st.session_state['crawl_wiki_pageview'][1]):
    if st.session_state['crawl_wiki_pageview'][0]:
        st.session_state["crawl_wiki_pageview"][1]=True
    else:
        st.write("조건을 확인하고 진행해주세요")
if st.session_state["crawl_wiki_pageview"][1]==True:
    conf_name=CONFIG_PATH
    progress_pageview = st.empty()
    log_container_pageview= st.empty()
    st.session_state["log crawl pageview"] = []
    pwp.xtools_crawl(log_container_pageview,"log crawl pageview",progress_pageview,conf_name,from_year,to_year,only_update)
    st.session_state["crawl_wiki_pageview"][1]=False
    st.success("작업 완료!")  
