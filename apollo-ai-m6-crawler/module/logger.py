### Streamlit 로그 처리
import streamlit as st

def log_writer(log_container, log_list_name, msg, reverse=False):
    if log_list_name not in st.session_state:
        st.session_state[log_list_name] = []

    st.session_state[log_list_name].append(msg)
    if reverse:
        log_container.text_area(
            f"실시간 로그: {log_list_name}",
            "\n".join(reversed(st.session_state[log_list_name])),
            height=300
        )
    else:
        log_container.text_area(
            f"실시간 로그: {log_list_name}",
            "\n".join(st.session_state[log_list_name]),
            height=300
        )
    
def progress_writer(progress_container, ratio, label):
    progress_container.progress(ratio, text=label)