import pandas as pd
from datetime import datetime, timedelta
from pyzabbix.api import ZabbixAPI
from google.cloud import bigquery
from dotenv import load_dotenv
import os
load_dotenv()


unix_today = int((datetime.combine(datetime.today(), datetime.min.time()).timestamp()))
unix_yesterday = int((datetime.combine(datetime.today(), datetime.min.time()) - timedelta(days=1)).timestamp())


#Google BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\src\json\gglobo-pea-hdg-prd-564ea8a1e1f0.json'
client = bigquery.Client()  # Your Google Cloud Platform project ID
stage_table_id = "gglobo-plan-aloc-perf-hdg-prd.performance_gov.tb_zabbix_vfx_stage"
table_id = "gglobo-plan-aloc-perf-hdg-prd.performance_gov.tb_zabbix_vfx"

#Zabbix
user = os.getenv('ZABBIX_USER')
password = os.getenv('ZABBIX_PASSWORD')
server = os.getenv('ZABBIX_URL')
api = ZabbixAPI(url=server, user=user, password=password)


def get_host_group(group_name):
    group = api.hostgroup.get(filter={"name": group_name})
    group_id = group[0]['groupid']
    return group_id
   

def get_host_hist(group_id):
    hosts = api.host.get(output='extend',
                         groupids=group_id,
                         selectInterfaces=["interfaceid", "ip"],
                         selectItems=["itemid","name","key_"],)
    
    host_id = []
    data = []
    hora = []
    valor = []
    host_name = []
    
    for host in hosts:
        history = api.history.get(
            output =  "extend",
            history = 4,
            hostids = host['hostid'],
            sortfield = "clock",
            time_from = unix_yesterday,
            time_till = unix_today,
        )
        
        for item in history:
            data.append(datetime.fromtimestamp(int(item['clock'])).strftime('%Y-%m-%d'))
            hora.append(datetime.fromtimestamp(int(item['clock'])).strftime('%H:%M:%S'))
            valor.append(item['value'])
            host_id.append(host['hostid'])  
            host_name.append(host['name'])
            
    df = pd.DataFrame({'host_id': host_id, 'host_name': host_name, 'data': data, 'hora': hora, 'valor': valor})
        
    return df
    

def get_df():
    data = get_host_hist(get_host_group('CLOUD_VFX'))
    data = data.query('valor.str.contains("/")', engine='python')
    data['hora'] = pd.to_timedelta(data['hora'])
    
    data_group = data.groupby(['host_name', 'data', 'valor']).agg({'hora' : ['min', 'max']}).reset_index()
    data_group.columns = ['host_name', 'data', 'valor', 'hora_min', 'hora_max']
    data_group['tempo_logado'] = data_group['hora_max'] - data_group['hora_min']
    data_group['tempo_logado'] = data_group['tempo_logado'].astype(str).str.split(' ').str[2]
    data_group['tempo_logado'] = pd.to_timedelta(data_group['tempo_logado'])  
    data_group = data_group[['host_name', 'data', 'valor', 'tempo_logado']]
    data_group['user'] = data_group['valor'].str.split('-').str[0].str.strip()

    data = data_group.groupby(['host_name', 'user' , 'data']).agg({'tempo_logado': 'sum'}).reset_index()
    data['tempo_logado'] = data['tempo_logado'].astype(str).str.split(' ').str[2]
        
    return data


def insert_bq():
    data = get_df()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(data, stage_table_id, job_config=job_config)
    job.result()
    print("Loaded {} rows into {}.".format(job.output_rows, stage_table_id))
    
    merge_sql_file = open('merge.sql', 'r')
    
    merge_sql = merge_sql_file.read()
    merge_sql = merge_sql.replace('$stage_table_id', stage_table_id)
    merge_sql = merge_sql.replace('$table_id', table_id)

    query_job = client.query(merge_sql,
                             location="US", 
                             job_id_prefix="merge_")
    query_job.result()
    #print("Completed job: {}".format(query_job.job_id))
    
insert_bq()