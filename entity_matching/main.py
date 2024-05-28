

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from computation import combined, result
from helper import transform, create_soup


def load_file():

    abc_df = pd.read_csv("/opt/airflow/datasets/ABC_layout_1.csv")
    pqr_df = pd.read_csv("/opt/airflow/datasets/PQR_layout_2.csv")
    voters_df = pd.read_csv("/opt/airflow/datasets/layout_3_voters.csv")
    klm_df = pd.read_csv("/opt/airflow/datasets/KLM_layout_4.csv")
    license_df = pd.read_csv("/opt/airflow/datasets/layout_5_license_dropped.csv")

    return abc_df, pqr_df, voters_df, klm_df, license_df


def processing():

    abc_df, pqr_df, voters_df, klm_df, license_df = load_file()

    abc_df = abc_df.rename(
        columns={"First Name": "Name", "Father Name": "Father_Name", "Permanent_Adress": "Permanent_Address"})

    pqr_df = pqr_df.rename(columns={"Customer_ID": "Mobile Number"})

    voters_df = voters_df.rename(
        columns={"votersName": "Name", "votersFatherName": "Father_Name", "votersMotherName": "Mother Name",
                 " Gender": "Gender", "Permanent_Adress": "Permanent_Address"})

    klm_df = klm_df.rename(columns={"Father Name": "Father_Name"})

    layouts = [abc_df, pqr_df, voters_df, klm_df, license_df]
    layout_sources = ['bank', 'esewa', 'voter', 'electricity', 'license']

    for layout, source in zip(layouts, layout_sources):
        layout['source'] = source
        layout['modified_date'] = datetime.now()

    layout_copies = [layout.copy() for layout in layouts]
    soup = ['Name', 'Date of Birth', 'Father_Name']

    for layout, layout_copy in zip(layouts, layout_copies):
        layout_copy = transform(layout_copy)
        create_soup(layout, layout_copy, soup, "soup")

    return abc_df, pqr_df, voters_df, klm_df, license_df


def main():

    abc_df, pqr_df, voters_df, klm_df, license_df = processing()

    result1 = result(abc_df, pqr_df)
    result2 = result(result1, klm_df)
    result3 = result(result2, voters_df)
    result4 = result(result3, license_df)

    result4.to_csv("/opt/airflow/datasets/result4.csv", index=False, header=True)
    return "/opt/airflow/datasets/result4.csv"


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 5, 16),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=1)
}

# Instantiate a DAG
first_dag = DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='hadoop download file',
    schedule_interval='@once',
    catchup=False
)

load_file_task = PythonOperator(
    task_id='load_file_task',
    python_callable=load_file,
    dag=first_dag
)

preprocess_task = PythonOperator(
    task_id='preprocess_task',
    python_callable=processing,
    dag=first_dag
)

run_this = PythonOperator(
    task_id="run_this",
    python_callable=main,
    dag=first_dag
)


load_file_task >> preprocess_task >> run_this

