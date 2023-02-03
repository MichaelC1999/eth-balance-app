from substreams import Substream
from streamlit.runtime.scriptrunner.script_run_context import add_script_run_ctx, get_script_run_ctx

import streamlit as st
from tempfile import NamedTemporaryFile
import pandas as pd
import substreams as sub
import os
import sys
import threading
import time
import requests
import math
import types

from dotenv import load_dotenv
load_dotenv()

sys.setrecursionlimit(15000)

st.set_page_config(layout='wide')
style_css = """
    <style>
        div.table-container {
            width: 100%;
            overflow: scroll;
        }
        table.dataframe {
        width: 100%;
        background-color: rgb(35,58,79);
        border-collapse: collapse;
        border-width: 2px;
        border-color: rgb(17,29,40);
        border-style: solid;
        color: white;
        font-size: 14px;
        }
        table.dataframe td, table.dataframe th {
        text-align: left;
        border-top: 2px rgb(17,29,40) solid;
        border-bottom: 2px rgb(17,29,40) solid;
        padding: 3px;
        white-space:nowrap;
        }
        table.dataframe thead {
            color: rgb(215,215,215);
        background-color: rgb(17,29,40);
        }
    </style>"""
sb = None
sb_keys = []

if bool(st.session_state) is False:
    st.session_state['streamed_data'] = []
    st.session_state['attempt_failures'] = 0
    st.session_state['error_message'] = ""
    st.session_state['has_started'] = False
    st.session_state["sftoken"] = None
    st.session_state["min_block"] = 10000000
    st.experimental_rerun()

block_start = st.number_input('START BLOCK:', min_value=1, max_value=20000001)
block_end = st.number_input('END BLOCK:', min_value=2, max_value=20000001, key="max_block")

st.text_input('FILTER ADDRESS:', placeholder="0x0", key="filter_address")

stop_message = st.empty()
if 'has_started' in st.session_state:
    if st.session_state['has_started'] is False:
        execute_button = st.button('Start Execution')
        if execute_button is True:
            st.session_state["min_block"] = block_start
            st.session_state['has_started'] = True
            st.experimental_rerun()
    if st.session_state['has_started'] is True:
        execute_button = st.button('Stop Execution')
        if execute_button is True:
            stop_message.write('Stopping...')
            st.session_state['has_started'] = False
            st.experimental_rerun()


if "SUBSTREAMS_API_TOKEN" in os.environ:
    st.session_state["sftoken"] = os.environ["SUBSTREAMS_API_TOKEN"]
elif "APIKEY" in os.environ and st.session_state["sftoken"] is None:
    APIKEY = os.environ["APIKEY"]
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = '{"api_key": "' + APIKEY + '"}'
    response = requests.post('https://auth.streamingfast.io/v1/auth/issue', headers=headers, data=data)
    resp_json = response.json()
    if "token" in resp_json:
        st.session_state["sftoken"] = resp_json["token"]

sb = Substream('./eth-balance-v0.1.0.spkg', token=st.session_state["sftoken"])

    
min_block = block_start
if 'min_block' in st.session_state:
    min_block = st.session_state['min_block']

# filter_address = "8b695d0d7160aa8d95dc6ccef6e7133f76a91de7"
    
max_block = block_end
if 'max_block' in st.session_state:
    max_block = st.session_state['max_block']


placeholder = st.empty()

def get_row_with_balance(row):
    try:
        new_balance = None
        for x in row["balanceChanges"]:
            if "0x" + x["address"] == st.session_state["filter_address"].lower():
                new_balance = x["newBalance"]
    except Exception as err:
        print("ERROR2 --- ", err)

    if new_balance is None:
        return None

    return_dict = {}
    return_dict["block"] = row["blockNumber"]
    return_dict["tx"] = row["txHash"]
    return_dict["new_balance"] = new_balance
    return_dict["address"] = st.session_state["filter_address"].lower()
    return return_dict

def get_valid_rows(data_rows):
    valid_tx = list(filter(lambda d: "0x" + d['to'] == st.session_state["filter_address"].lower() or "0x" + d["from"] == st.session_state["filter_address"].lower(), data_rows))
    return len(valid_tx) > 0

if 'streamed_data' in st.session_state:
    if len(st.session_state['streamed_data']) > 0:
        copy_df = pd.DataFrame(st.session_state['streamed_data'])
        if list(copy_df.columns):
            st.selectbox("Select Substream Table Sort Column", options=list(copy_df.columns), key="rank_col") 

        if st.session_state['rank_col'] is not None:
            copy_df = copy_df.sort_values(by=st.session_state['rank_col'],ascending=False)
            copy_df.index = range(1, len(copy_df) + 1)
            copy_df['tx'] = '0x' + copy_df['tx'].astype(str)
        html_table = '<div class="table-container">' + copy_df[:500].to_html() + '</div>'
        st.markdown(style_css + html_table, unsafe_allow_html=True)

error_message = st.empty()

if 'error_message' in st.session_state:
    if st.session_state['error_message'] != "" and st.session_state['error_message'] is not None: 
        error_message.text(st.session_state['error_message'])

if st.session_state['has_started'] is True and "filter_address" in st.session_state:
    st.session_state['error_message'] = ""
    if 'min_block' in st.session_state:
        min_block = st.session_state['min_block']
    if min_block > 0:
        if st.session_state["filter_address"] is None or st.session_state["filter_address"] == "":
            raise TypeError('No address provided for filter.')
        if max_block < min_block:
            raise TypeError('`min_block` is greater than `max_block`. This cannot be validly polled.')
        if max_block == min_block:
            st.session_state["min_block"] = 0
            st.session_state['has_started'] = False
            st.experimental_rerun()
        if max_block > min_block and sb is not None:
            poll_return = {}
            try:
                placeholder.text("Loading Substream Results...")
                poll_return = sb.poll("map_balances", start_block=min_block, end_block=max_block, return_first_result=True, return_type="dict", return_first_result_function=get_valid_rows)
                placeholder.empty()
                if "error" in poll_return:
                    if poll_return["error"] is not None:
                        if "debug_error_string" in dir(poll_return["error"]):
                            raise TypeError(poll_return["error"].debug_error_string() + " BLOCK: " + str(poll_return["data_block"]))
                        else:
                            raise TypeError(str(poll_return["error"]) + " BLOCK: " + str(poll_return["data_block"]))
                if "data" in poll_return:
                    if (len(poll_return["data"]) > 0):
                        valid_tx = list(filter(lambda d: "0x" + d['to'] == st.session_state["filter_address"].lower() or "0x" + d["from"] == st.session_state["filter_address"].lower(), poll_return["data"]))
                        if (len(valid_tx) > 0):
                            valid_tx_manipulated = map(get_row_with_balance, valid_tx)
                            st.session_state['streamed_data'].extend(valid_tx_manipulated)
                    st.session_state['min_block'] = int(poll_return["data_block"]) + 1
            except Exception as err:
                print("ERROR --- ", err)
                attempt_failures = st.session_state['attempt_failures']
                attempt_failures += 1
                if attempt_failures % 10 == 0:
                    st.session_state['error_message'] = "ERROR --- " + str(err)
                    st.session_state['has_started'] = False
                    st.session_state["min_block"] = max_block
                st.session_state['attempt_failures'] = attempt_failures
            st.experimental_rerun()
elif 'streamed_data' in st.session_state:
    if len(st.session_state['streamed_data']) > 0:
        st.write('Substream Polling Completed') 