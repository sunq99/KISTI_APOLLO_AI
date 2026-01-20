import streamlit as st

import module.proc_metrics_tb as pmt
import config.path as path

CONFIG_PATH=path.conf_path

st.title("대상 테이블 생성")

tab1, tab2, tab3, tab4 = st.tabs(['1. PageRank 테이블 생성','2. 연도별 지표 테이블 생성','3. Item list 테이블 생성','4. Search 테이블 생성'])
if 'make_table' not in st.session_state:
    st.session_state['make_table'] = [False,False,False,False]
    
with tab1:
    st.write('1. PageRank 테이블 생성')
    if st.button('Pagerank 수집', key="button_make_table_1",disabled=st.session_state['make_table'][0]):
        st.session_state['make_table'][0]=True
    if st.session_state['make_table'][0]==True:
        conf_name=CONFIG_PATH
        log_container_pagerank= st.empty()
        st.session_state["log pagerank"] = []
        pmt.make_pagerank(conf_name,log_container_pagerank,"log pagerank")
        st.session_state['make_table'][0]=False
        st.success("작업 완료!")
        
with tab2:
    st.write('2. 연도별 지표 테이블 생성')
    if st.button('테이블 생성', key="button_make_table_2",disabled=st.session_state['make_table'][1]):
        st.session_state['make_table'][1]=True
    if st.session_state['make_table'][1]==True:
        conf_name=CONFIG_PATH
        log_container_statistics= st.empty()
        st.session_state["log statistics"] = []
        pmt.make_statistics(conf_name,log_container_statistics,"log statistics")
        st.session_state['make_table'][1]=False
        st.success("작업 완료!")

with tab3:
    st.write('3. Item list 테이블 생성')
    if st.button('테이블 생성', key="button_make_table_3",disabled=st.session_state['make_table'][2]):
        st.session_state['make_table'][2]=True
    if st.session_state['make_table'][2]==True:
        conf_name=CONFIG_PATH
        log_container_itemlist= st.empty()
        st.session_state["log itemlist"] = []
        pmt.make_itemlist(conf_name,log_container_itemlist,"log itemlist")
        st.session_state['make_table'][2]=False
        st.success("작업 완료!")
        
with tab4:
    st.write('4. Search 테이블 생성')
    if st.button('테이블 생성', key="button_make_table_4",disabled=st.session_state['make_table'][3]):
        st.session_state['make_table'][3]=True
    if st.session_state['make_table'][3]==True:
        conf_name=CONFIG_PATH
        log_container_itemlist= st.empty()
        st.session_state["log searchtb"] = []
        pmt.make_search_tb(conf_name,log_container_itemlist,"log searchtb")
        st.session_state['make_table'][3]=False
        st.success("작업 완료!")