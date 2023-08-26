from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from google.cloud import storage
import re
import numpy as np
import pandas as pd
import json
from io import BytesIO
import io
import pyarrow
# slack_notifications.py
from slack_notifications import SlackAlert
from airflow.models import Variable

KEY_PATH = "./playdata-2-1e60a2f219de.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

slack_api_token = Variable.get("slack_api_token")
alert = SlackAlert('#message', slack_api_token) # 메세지를 보낼 슬랙 채널명을 파라미터로 넣어줍니다.


dag = DAG(
    dag_id="parsing_weapon_data",
    description="무기_분석",
    start_date=datetime(2023, 7, 2, 0, 0),
    schedule_interval='0 14 * * *',
    on_success_callback=alert.success_msg,
    on_failure_callback=alert.fail_msg
)

def _read_data_from_gcp_storage():
    bucket_name = "playdata2"
    file_path = ""
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
    return concat_data



def _process_logs_weapon(**kwargs):
    ti = kwargs['ti']
    concat_data = ti.xcom_pull(task_ids='read_data')

    tmp = [data for data2 in concat_data['logs'] for data in data2 if '_T' in data and data['_T'] == 'LogPlayerKillV2']

    kv2 = []

    for log in tmp:
        
        # 무기 관련 데이터 파싱
        if log['_T'] == 'LogPlayerKillV2':
            try :
                if 'ai' not in log['victim']['accountId'] and 'ai' not in log['killer']['accountId'] and 'ai' not in log['finisher']['accountId'] :
                    try :
                        v2row = {'victim_weapon' : None if len(log['victimWeapon']) == 0 else log['victimWeapon'],
                                'victim_account_id' :log['victim']['accountId'],
                                'victim_parts' : None if len(log["victimWeaponAdditionalInfo"]) == 0 else log["victimWeaponAdditionalInfo"],
                                'killer_weapon' : log['killerDamageInfo']['damageCauserName'],
                                'killer_account_id' : log['killer']['accountId'],
                                'killer_parts' : None if len(log['killerDamageInfo']['additionalInfo']) == 0 else log['killerDamageInfo']['additionalInfo'],
                                'killer_distance' :log['killerDamageInfo']['distance'],
                                'finisher_weapon' : log['finishDamageInfo']['damageCauserName'],
                                'finisher_account_id' : log['finisher']['accountId'],
                                'finisher_parts' : None if len(log['finishDamageInfo']['additionalInfo']) == 0 else log['finishDamageInfo']['additionalInfo'],
                                'finisher_distance' : log['finishDamageInfo']['distance'],
                                }
                    except :
                        v2row = {'victim_weapon' : None,
                                'victim_account_id' : None,
                                'victim_parts' : None,
                                'killer_weapon' : None,
                                'killer_account_id' : None,
                                'killer_parts' : None,
                                'killer_distance' : None,
                                'finisher_weapon' : None,
                                'finisher_account_id' : None,
                                'finisher_parts' : None,
                                'finisher_distance' : None,
                                }
                kv2.append(v2row)
            except :
                pass
            
    kv3 = pd.DataFrame(kv2)



    def remove_part(text):
        return re.sub(r'(?:^Weapon|^Weap)|_C.*$', '', text)

    kv3['victim_weapon'] = kv3['victim_weapon'].astype(str)
    kv3['victim_weapon'] = kv3['victim_weapon'].apply(remove_part)
    kv3['killer_weapon'] = kv3['killer_weapon'].astype(str)
    kv3['killer_weapon'] = kv3['killer_weapon'].apply(remove_part)
    kv3['finisher_weapon'] = kv3['finisher_weapon'].astype(str)
    kv3['finisher_weapon'] = kv3['finisher_weapon'].apply(remove_part)

    weapon_name_mapping = {
        'SKS': 'SKS', 'FNFal': 'SLR', 'Mini14': '미니14', 'Mk12': 'MK12', 'Mk14': 'MK14',
        'QBU88': 'QBU', 'VSS': 'VSS', 'AWM': 'AWM', 'Kar98k': 'KAR98K', 'L6': '링스 AMR',
        'M24': 'M24', 'Mosin': '모신 나강', 'Win1894': 'WIN94', 'Thompson': '토미 건',
        'BizonPP19': 'PP-19 비존', 'UZI': '마이크로 UZI', 'MP5K': 'MP5K', 'MP9': 'MP9',
        'P90': 'P90', 'UMP': 'UMP45', 'Vector': '벡터', 'DP28': 'DP-28', 'M249': 'M249',
        'MG3': 'MG3', 'Saiga12': 'S12K', 'DP12': 'DBS', 'OriginS12': 'O12',
        'Winchester': 'S1897', 'Berreta686': 'S686', 'Sawnoff': '소드 오프',
        'DesertEagle': 'Deagle', 'G18': 'P18C', 'M1911': 'P1911', 'M9': 'P92',
        'NagantM1895': 'R1895', 'Rhino': 'R45', 'vz61Skorpion': '스콜피온',
        'Crossbow_1': '석궁', 'HK416': 'M416', 'G36C': 'G36C', 'ACE32': 'ACE32',
        'AK47': 'AKM', 'AUG': 'AUG', 'Mk47Mutant': 'MK47 뮤턴트', 'FamasG2': 'FAMAS',
        'G36C': 'G36C', 'K2': 'K2', 'M16A4': 'M16A4', 'BerylM762': '베릴 M762',
        'QBZ95': 'QBZ', 'SCAR-L': 'SCAR-L', 'Groza': '그로자'
    }

    # 무기 이름 변환
    kv3['victim_weapon'] = kv3['victim_weapon'].map(weapon_name_mapping)
    kv3['killer_weapon'] = kv3['killer_weapon'].map(weapon_name_mapping)
    kv3['finisher_weapon'] = kv3['finisher_weapon'].map(weapon_name_mapping)

    # 무기 분류를 위한 조건과 값 설정
    conditions = [
        kv3['killer_weapon'].isin(['SKS', 'SLR', '미니14', 'MK12', 'MK14', 'QBU', 'VSS']),
        kv3['killer_weapon'].isin(['AWM', 'KAR98K', '링스 AMR', 'M24', '모신 나강', 'WIN94']),
        kv3['killer_weapon'].isin(['토미 건', 'PP-19 비존', '마이크로 UZI', 'MP5K', 'MP9', 'P90', 'UMP45', '벡터']),
        kv3['killer_weapon'].isin(['DP-28', 'M249', 'MG3']),
        kv3['killer_weapon'].isin(['S12K', 'DBS', 'O12', 'S1897', 'S686', '소드 오프']),
        kv3['killer_weapon'].isin(['Deagle', 'P18C', 'P1911', 'P92', 'R1895', 'R45', '스콜피온']),
        kv3['killer_weapon'].isin(['석궁']),
        kv3['killer_weapon'].isin(['M416', 'G36C', 'ACE32', 'AKM', 'AUG', 'FAMAS', '그로자', 'K2', 'M16A4', '베릴 M762', 'MK47 뮤턴트', 'QBZ', 'SCAR-L'])
    ]

    values = ['DMR', 'SR', 'SMG', 'LMG', 'SG', 'PISTOL', 'MISC', 'AR']

    kv3['weapon_classification'] = np.select(conditions, values)
    kv3.reset_index(drop=True, inplace=True)

    return kv3


def _upload_data_to_gcp_storage(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='process_logs_weapon')
    storage_client = storage.Client()

    bucket_name = "playdata2"
    folder_name = "logs_weapon"

    date = datetime.today().strftime("%Y%m%d")

    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{date}_logs_weapon.parquet")
    blob.upload_from_file(buffer)

    buffer.close()

read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=_read_data_from_gcp_storage,
    provide_context=True,
    dag=dag,
)

process_logs_weapon_task = PythonOperator(
    task_id='process_logs_weapon',
    python_callable=_process_logs_weapon,
    provide_context=True,
    dag=dag,
)

upload_data_task = PythonOperator(
    task_id='upload_data',
    python_callable=_upload_data_to_gcp_storage,
    provide_context=True,
    dag=dag,
)

read_data_task >> process_logs_weapon_task >> upload_data_task
