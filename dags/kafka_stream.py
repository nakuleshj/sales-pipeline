from datetime import datetime
#from airflow import DAG
#from airflow.operators.python import PythonOperator
import json, random
from kafka import KafkaProducer
import pandas as pd
import time

default_args = {
    'owner' : 'nakulesh',
    'start_date' : datetime(2023, 9, 3, 10, 00)
}
def get_data():
    print('getting data')
    df=pd.read_excel('./data/retail_dataset.xlsx')
    
    return df

def format_msg(invoice_data: pd.DataFrame, invoice_id: int):
    
    customer_id=invoice_data["CustomerID"].unique()
    country=invoice_data["Country"].unique()
    product_data=invoice_data.drop(columns=['InvoiceNo','Country','CustomerID','InvoiceDate'])
    msg = {
        'timestamp':datetime.now().strftime('%m-%d-%y %H:%M:%S'),
        'invoice_id':invoice_id,
        'customer_id': int(customer_id[0]),
        'country': country[0],
        'products': product_data.to_dict('records')
    }
    return msg

def stream_data():
    df = get_data()
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        max_block_ms=5000
    )
    for invoice_id in df["InvoiceNo"].unique():

        msg=format_msg(df.loc[df['InvoiceNo']==invoice_id],invoice_id)
        #print(msg)
        
        producer.send(
            'sales',
            json.dumps(msg).encode("utf-8")
        )

        interval=random.randint(1,60)
        
        time.sleep(interval)

stream_data()

"""
with DAG(
    'user_automation',
    default_args= default_args,
    schedule_interval='@daily',
    catchup= False
) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    streaming_task
"""