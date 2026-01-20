import streamlit as st
import datetime
import module.proc_edit_dump as ped
import module.proc_data as prd
import config.path as path
import requests
st.title('wiki edit dump 처리')

EDIT_PATH=path.editdump_path

tab1, tab2, tab3, tab4=st.tabs(['1. Edit Dump 수집','2. Edit Dump 데이터 처리','3. Edit 데이터 연도별 집계','4. Edit 데이터 DB 적재'])
if 'editdump' not in st.session_state: #다음 Step으로 넘어가기 위함
    st.session_state['editdump'] = ['','','']
if 'editdump_button' not in st.session_state:
    st.session_state['editdump_button'] = [False,False,False,False]

with tab1:
    st.header('1. Edit Dump 수집')
    check_edit = st.checkbox('Edit Dump 추가 수집이 필요한 지 확인', key="check_edit")
    message = ''
    yearmonth = []
    if check_edit:
        message, yearmonth = ped.check_edit_file(EDIT_PATH)
        st.write(message)
    if check_edit and len(yearmonth) > 0:
        st.write('추가 수집을 진행하세요')
        if st.button('추가 수집 실행', key="button_editdump1",disabled=st.session_state['editdump_button'][0]):
            st.session_state["editdump_button"][0]=True
        if st.session_state["editdump_button"][0]==True:
            log_editdump_1 = st.empty()
            st.session_state["log crawl editdump"] = []
            today = datetime.datetime.now()
            today_month = f"{today.month - 1:02d}" if today.month!=1 else 12
            today_year = today.year if today.month!=1 else today.year-1
            target_folder = f'{EDIT_PATH}/org_data'
            base_url = f'https://dumps.wikimedia.org/other/mediawiki_history/{today_year}-{today_month}/enwiki/'
            response = requests.get(base_url)
            if response.status_code==404:
                if today_month=='01':
                    base_url = f'https://dumps.wikimedia.org/other/mediawiki_history/{today_year-1}-12/enwiki/'
                else:
                    base_url = f'https://dumps.wikimedia.org/other/mediawiki_history/{today_year}-{today.month - 2:02d}/enwiki/'
            down_stat = ped.download_files_from_year(log_editdump_1, "log crawl editdump", base_url, target_folder, yearmonth)
            st.success("작업 완료!")
            st.session_state["editdump_button"][0] = False
    if check_edit:
        st.session_state['editdump'][0]='comp_file'

with tab2:
    st.header('2. Edit Dump 데이터 처리')
    if st.session_state['editdump'][0]=='comp_file':
        if st.button('데이터 처리 진행', key="button_editdump2",disabled=st.session_state['editdump_button'][1]):
            st.session_state["editdump_button"][1]=True
        if st.session_state["editdump_button"][1]==True:
            log_editdump_2 = st.empty()
            st.session_state["log process editdump"] = []
            ped.process_files_in_folder(log_editdump_2, "log process editdump",EDIT_PATH)
            st.success("작업 완료!")
            st.session_state["editdump_button"][1] = False
        st.session_state['editdump'][1] = 'edit_process'

with tab3:
    st.header('3. Edit 데이터 연도별 집계')
    if st.session_state['editdump'][1] == 'edit_process':
        need_year=ped.check_yearly(EDIT_PATH)
        if len(need_year)==0:
            st.write('처리할 건이 없습니다.')
        else:
            if st.button('연별 처리 진행', key="button_editdump3",disabled=st.session_state['editdump_button'][2]):
                st.session_state["editdump_button"][2]=True
                log_editdump_3 = st.empty()
                st.session_state["log process edityealy"] = []
            if st.session_state["editdump_button"][2]==True:
                for y in need_year:
                    ped.yearly_edit(log_editdump_3,"log process edityealy",EDIT_PATH,y)
                st.success("작업 완료!")
                st.session_state["editdump_button"][2] = False
        st.session_state['editdump'][2] = 'edit_db'

with tab4:
    st.header('4. Edit 데이터 DB 적재') #선행 조건 : item master에 있는 값으로만 움직임
    if st.session_state['editdump'][2] == 'edit_db':
        len_item=prd.check_data('DUMP_EDIT', 0,'source')
        if len_item>0:
            if st.button('DB 적재 진행', key="button_editdump4",disabled=st.session_state['editdump_button'][3]):
                st.session_state["editdump_button"][3]=True
                log_editdump_4 = st.empty()
                st.session_state["log db edit"] = []
            if st.session_state["editdump_button"][3]==True:
                ped.make_editdb(log_editdump_4, "log db edit", EDIT_PATH)
                st.success("작업 완료!")
                st.session_state["editdump_button"][3] = False
