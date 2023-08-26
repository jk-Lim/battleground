from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from google.cloud import storage
import numpy as np
import pandas as pd
from io import BytesIO
import io
import pyarrow
# slack_notifications.py
from slack_notifications import SlackAlert
from airflow.models import Variable
import pymysql
from config import DB_CONFIG

KEY_PATH = "./playdata-2-1e60a2f219de.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

slack_api_token = Variable.get("slack_api_token")
alert = SlackAlert('#message', slack_api_token) # 메세지를 보낼 슬랙 채널명을 파라미터로 넣어줍니다.


dag = DAG(
    dag_id="load_weapon_data",
    description="무기_분석",
    start_date=datetime(2023, 7, 1, 0, 0),
    schedule_interval='0 16 * * *',
    on_success_callback=alert.success_msg,
    on_failure_callback=alert.fail_msg
)

def _read_data_from_gcp_storage(**kwargs):
    bucket_name = "playdata2"
    file_path = "logs_weapon/"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    parquet_data = []
    blobs = bucket.list_blobs(prefix=file_path)

    for blob in blobs:

        # parquet 형식의 파일인지 확인
        if blob.name.endswith(".parquet"):
            # 객체를 바이트 스트림으로 다운로드
            byte_stream = io.BytesIO(blob.download_as_bytes())
            
            # parquet 데이터를 pandas DataFrame으로 읽기
            df = pd.read_parquet(byte_stream)
            parquet_data.append(df)
           

    # 개별 DataFrame들을 하나의 DataFrame으로 합치기
    concat_data = pd.concat(parquet_data, axis=0, ignore_index=True)
    kwargs['ti'].xcom_push(key='parquet_data', value=concat_data)


def _process_weapon_data(**kwargs):
    # GCP Storage에서 데이터 읽어오기
    parquet_data = kwargs['ti'].xcom_pull(key='parquet_data')
    kv3 = pd.concat(parquet_data, axis=0, ignore_index=True)
    result_df = kv3.groupby(['killer_weapon', 'victim_weapon']).size().reset_index(name='count')
    reverse_combinations = result_df.rename(columns={'killer_weapon': 'victim_weapon', 'victim_weapon': 'killer_weapon', 'count': 'reverse_count'})
    result_df = result_df.merge(reverse_combinations, on=['killer_weapon', 'victim_weapon'], how='outer')
    result_df.fillna(0, inplace=True)

    result_df['total_count'] = result_df['count'] + result_df['reverse_count']
    result_df['win_rate'] = result_df['count'] * 100 / result_df['total_count']
    result_df = result_df[(result_df['victim_weapon'].notnull()) & (result_df['killer_weapon'] != result_df['victim_weapon'])]

    target = result_df['killer_weapon'].unique()
    result = {}

    for weapon in target:
        filtered_df = result_df[(result_df['killer_weapon'] == weapon) & (result_df['total_count'] >= 20)].sort_values('win_rate', ascending=False)
        high_weapons = filtered_df.iloc[:3] if filtered_df.shape[0] >= 3 else filtered_df
        low_weapons = filtered_df.iloc[-3:] if filtered_df.shape[0] >= 3 else filtered_df

        result[weapon] = {
            "highs": high_weapons,
            "lows": low_weapons,
        }

    result_list = []

    for weapon in result.keys():
        result_list.append({
            "weapon_name": weapon,
            **{f"easy_weapon_{i + 1}": row["victim_weapon"] if row["victim_weapon"] else None for i, row in result[weapon]["highs"].reset_index(drop=True).iterrows()},
            **{f"easy_percent_{i + 1}": row["win_rate"] if row["win_rate"] else None for i, row in result[weapon]["highs"].reset_index(drop=True).iterrows()},
            **{f"hard_weapon_{i + 1}": row["victim_weapon"] if row["victim_weapon"] else None for i, row in result[weapon]["lows"].reset_index(drop=True).iterrows()},
            **{f"hard_percent_{i + 1}": row["win_rate"] if row["win_rate"] else None for i, row in result[weapon]["lows"].reset_index(drop=True).iterrows()},
        })

    final_df = pd.DataFrame(result_list)
    final_df = final_df.fillna(0)

    games_threshold = 30
    weapon_summary2 = result_df.groupby('killer_weapon').agg({'total_count': 'sum', 'count': 'sum'}).reset_index()
    weapon_summary2['win_rate'] = weapon_summary2['count'] * 100 / weapon_summary2['total_count']
    valid_weapons = weapon_summary2[weapon_summary2['total_count'] >= games_threshold]

    count_weight = 0.2
    win_rate_weight = 0.8
    weapon_summary2['score'] = (weapon_summary2['total_count'] * count_weight) + (weapon_summary2['win_rate'] * win_rate_weight)

    quantiles = weapon_summary2['score'].quantile([.9, .8, .5, .3, 0]).values
    tier_1 = weapon_summary2[weapon_summary2['score'] >= quantiles[0]]
    tier_2 = weapon_summary2[(weapon_summary2['score'] >= quantiles[1]) & (weapon_summary2['score'] < quantiles[0])]
    tier_3 = weapon_summary2[(weapon_summary2['score'] >= quantiles[2]) & (weapon_summary2['score'] < quantiles[1])]
    tier_4 = weapon_summary2[(weapon_summary2['score'] >= quantiles[3]) & (weapon_summary2['score'] < quantiles[2])]
    tier_5 = weapon_summary2[weapon_summary2['score'] < quantiles[3]]

    tier_1_sorted = tier_1.sort_values('score', ascending=True)['killer_weapon'].tolist()
    tier_2_sorted = tier_2.sort_values('score', ascending=True)['killer_weapon'].tolist()
    tier_3_sorted = tier_3.sort_values('score', ascending=True)['killer_weapon'].tolist()
    tier_4_sorted = tier_4.sort_values('score', ascending=True)['killer_weapon'].tolist()
    tier_5_sorted = tier_5.sort_values('score', ascending=True)['killer_weapon'].tolist()

    for idx, row in weapon_summary2.iterrows():
        if row['killer_weapon'] in tier_1_sorted:
            weapon_summary2.loc[idx, 'tier'] = 1
        elif row['killer_weapon'] in tier_2_sorted:
            weapon_summary2.loc[idx, 'tier'] = 2
        elif row['killer_weapon'] in tier_3_sorted:
            weapon_summary2.loc[idx, 'tier'] = 3
        elif row['killer_weapon'] in tier_4_sorted:
            weapon_summary2.loc[idx, 'tier'] = 4
        else:
            weapon_summary2.loc[idx, 'tier'] = 5

    weapon_summary2['tier'] = weapon_summary2['tier'].astype(int)

    final_df = final_df.merge(weapon_summary2[['killer_weapon', 'tier']], left_on='weapon_name', right_on='killer_weapon', how='left').drop(columns=['killer_weapon'])

    output_filename = "result.csv"
    final_df.to_csv(output_filename, index=False)
    
    return final_df

def _update_database(final_df):
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    for index, row in final_df.iterrows():
        weapon_name = row['weapon_name']
        easy_weapon_1 = row['easy_weapon_1']
        easy_weapon_2 = row['easy_weapon_2']
        easy_weapon_3 = row['easy_weapon_3']
        easy_percent_1 = row['easy_percent_1']
        easy_percent_2 = row['easy_percent_2']
        easy_percent_3 = row['easy_percent_3']
        hard_weapon_1 = row['hard_weapon_1']
        hard_weapon_2 = row['hard_weapon_2']
        hard_weapon_3 = row['hard_weapon_3']
        hard_percent_1 = row['hard_percent_1']
        hard_percent_2 = row['hard_percent_2']
        hard_percent_3 = row['hard_percent_3']

    update_query = """
        UPDATE services_weapons SET first_easy_weapon = %s,
                            first_easy_percent = %s,
                            second_easy_weapon = %s,
                            second_easy_percent = %s,
                            third_easy_weapon = %s,
                            third_easy_percent = %s,
                            first_hard_weapon = %s,
                            first_hard_percent = %s,
                            second_hard_weapon = %s,
                            second_hard_percent= %s,
                            third_hard_weapon = %s,
                            third_hard_percent = %s WHERE weapon_name = %s

        """
    cur.execute(update_query, (easy_weapon_1, easy_percent_1, easy_weapon_2, easy_percent_2, easy_weapon_3, easy_percent_3,
                            hard_weapon_1, hard_percent_1, hard_weapon_2, hard_percent_2, hard_weapon_3, hard_percent_3,
                            weapon_name))

    conn.commit()  
    
read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=_read_data_from_gcp_storage,
    provide_context=True
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=_process_weapon_data,
    provide_context=True
)

update_database_task = PythonOperator(
    task_id='update_database',
    python_callable=_update_database,
    provide_context=True
)

read_data_task >> process_data_task >> update_database_task