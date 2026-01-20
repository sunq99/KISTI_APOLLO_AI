import streamlit as st
import module.proc_enwiki_dump as pr_end
import config.path as path

WIKIDUMP_PATH=path.wikidump_path
CONFIG_PATH=path.conf_path

st.title('wiki item dump 처리')

tab1, tab2 =st.tabs(['1. Item Dump 수집','2. Item Dump 데이터 처리'])

if 'itemdump' not in st.session_state:
    st.session_state['itemdump'] = ['','']
if 'itemdump_button' not in st.session_state:
    st.session_state['itemdump_button'] = [False,False] # 0 : set값, 1: 버튼 클릭

with tab1:
    file_list = []
    page_folder = '../pages'
    text_area_height = 400
    st.header('1. Item Dump 수집')
    check_test = st.checkbox('테스트용', key="TEST_tab1") 
    if st.button('Dump 수집 진행', key="button_itemdump1",disabled=st.session_state['itemdump_button'][0]):
        st.session_state["itemdump_button"][0] = True
    if st.session_state["itemdump_button"][0]==True:
        file_list=pr_end.make_file_list(check_test)
        st.write(f'{len(file_list)}건의 파일 처리 필요')
        progress_itemdump_1 = st.empty()
        log_itemdump_1 = st.empty()
        st.session_state["log crawl itemdump"] = []
        pr_end.download_item_file(log_itemdump_1,"log_itemdump_1",progress_itemdump_1,WIKIDUMP_PATH,file_list)
        st.success("작업 완료!")
        st.session_state["itemdump_button"][0] = False  # 다시 버튼 활성화

with tab2:
    st.header('2. Item Dump 데이터 처리')
    check_test = st.checkbox('테스트용', key="TEST_tab2") 
    if st.button('Item Parse 진행', key="button_itemdump2",disabled=st.session_state['itemdump_button'][1]):
        st.session_state["itemdump_button"][1] = True
    if st.session_state["itemdump_button"][1]==True:
        progress_itemdump_2 = st.empty()
        log_itemdump_2 = st.empty()
        st.session_state["log enwiki parse"] = []
        pr_end.enwiki_parse(CONFIG_PATH,log_itemdump_2,"log enwiki parse",progress_itemdump_2,check_test)
        st.success("작업 완료!")
        st.session_state["itemdump_button"][1] = False  # 다시 버튼 활성화
    
