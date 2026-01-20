import streamlit as st
import pandas as pd
import module.proc_seed_expand as pse
import config.path as path

CONFIG_PATH=path.conf_path

st.title('ITEM LIST 생성')

tab1, tab2, tab3, tab4 = st.tabs(['1. Item List 생성','2. 진입 Seed 확장','3. Item Filter','4. Seealso Filter'])
if 'make_item_list' not in st.session_state:
    st.session_state['make_item_list'] = [False,False,False,False]
with tab1:
    st.header('1. Item List 생성')
    # 라디오 버튼으로 선택
    input_method = st.radio(
        "키워드 입력 방법을 선택하세요:",
        ("직접 입력", "파일 업로드")
    )
    # 직접 입력인 경우
    if input_method == "직접 입력":
        keyword = st.text_input("키워드를 입력하세요")
        if keyword:
            st.success(f"입력한 키워드: {keyword}")
            input_data=keyword
            input_data_cnt=1
        else:
            input_data=None 

    # 파일 업로드인 경우
    elif input_method == "파일 업로드":
        uploaded_file = st.file_uploader("EXCEL/CSV 파일을 업로드하세요", type=['csv','xlsx','xls'])
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('csv') else pd.read_excel(uploaded_file)
                st.write("📄 데이터 미리보기:")
                st.dataframe(df)
                input_data=df
                input_data_cnt=len(input_data)
            except Exception as e:
                print(e)
        else:
            input_data=None
    if input_data is not None:
        if st.button('시드 입력', key="button_item_list_1",disabled=st.session_state['make_item_list'][0]):
            st.session_state["make_item_list"][0] = True
        if st.session_state["make_item_list"][0]==True:
            conf_name=CONFIG_PATH
            pse.get_check_seed(conf_name,input_data)
            st.session_state["make_item_list"][0] = False
            st.success("작업 완료!")  
        
with tab2:
    st.header('2. 진입 Seed 확장')
    if st.button('진입 Seed 확장', key="button_item_list_2",disabled=st.session_state['make_item_list'][1]):
        st.session_state["make_item_list"][1]=True
    if st.session_state["make_item_list"][1]==True:
        conf_name=CONFIG_PATH
        from_degree=1
        to_degree=5
        progress_tab2 = st.empty()
        log_container_tab2 = st.empty()
        st.session_state["log seealso expand"] = []
        for n in range(from_degree,to_degree+1):
            pse.seealso_expand(log_container_tab2,"log seealso expand",progress_tab2,conf_name,n)
        st.session_state["make_item_list"][1] = False
        st.success("작업 완료!")      

with tab3:
    st.header('3. Item Filter')
    if st.button('Item Filter', key="button_item_list_3",disabled=st.session_state['make_item_list'][2]):
        st.session_state["make_item_list"][2]=True
    if st.session_state["make_item_list"][2]==True:
        conf_name=CONFIG_PATH
        progress_tab3 = st.empty()
        log_container_tab3 = st.empty()
        st.session_state["log seealso filter"] = []
        pse.filter_item(log_container_tab3,"log seealso filter",progress_tab3,conf_name)
        st.session_state["make_item_list"][2] = False
        st.success("작업 완료!")      

with tab4:
    st.header('4. Network Filter')
    if st.button('Network Filter', key="button_item_list_4",disabled=st.session_state['make_item_list'][3]):
        st.session_state["make_item_list"][3]=True
    if st.session_state["make_item_list"][3]==True:
        conf_name=CONFIG_PATH
        to_degree=5
        progress_tab4 = st.empty()
        log_container_tab4 = st.empty()
        st.session_state["log network filter"] = []
        pse.filter_seealso(log_container_tab4,"log network filter",conf_name,to_degree)
        st.session_state["make_item_list"][3] = False
        st.success("작업 완료!")   
