from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

# exportar tabela Order do Northwind e salvar em um csv
def export_order_to_csv(**context):
    db_connection = sqlite3.connect('data/Northwind_small.sqlite')
    df = pd.read_sql_query('SELECT * FROM "Order";', db_connection)
    df.to_csv('data/output_orders_{}.csv'.format(context['ds_nodash']))
    db_connection.close()

# calcular 'quantity' + 'ShipCity' (apenas Rio de Janeiro)
# exportar resultado para um txt
def export_count(**context):
    db_connection= sqlite3.connect('data/Northwind_small.sqlite')
    order_detail = pd.read_sql_query("SELECT * FROM OrderDetail;", db_connection)
    orders = pd.read_csv('data/output_orders_{}.csv'.format(context['ds_nodash']))
    merged = pd.merge(order_detail, orders, how="inner", left_on="OrderId", right_on="Id")
    count = str(merged.query('ShipCity == "Rio de Janeiro"')['Quantity'].count())
    with open('count.txt', 'w') as t:
        t.write(count)
    db_connection.close()

# Definição do DAG
with DAG(
    'Desafio_5',
    default_args=default_args,
    description='Desafio Módulo 5 - Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    task1 = PythonOperator(
        task_id='extract_csv',
        python_callable=export_order_to_csv,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='export_fcount',
        python_callable=export_count,
        provide_context=True
    )

task1 >> task2 >> export_final_output
