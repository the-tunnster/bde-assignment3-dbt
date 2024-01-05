import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data/"


########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'tarun_krishnan',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='data_import',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################


def import_raw_census_data_g01(**kwargs):
	file_path = AIRFLOW_DATA + "2016Census_G01_NSW_LGA.csv"
    
	ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
	conn_ps = ps_pg_hook.get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.census_data_g01
				VALUES %s
		"""

		values = dataFrame.to_dict('split')
		values = values['data']

		result = execute_values(conn_ps.cursor(), sql, values, page_size=len(dataFrame))
		conn_ps.commit()
	else:
		None

	return None

def import_raw_census_data_g02(**kwargs):
	file_path = AIRFLOW_DATA + "2016Census_G02_NSW_LGA.csv"
    
	ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
	conn_ps = ps_pg_hook.get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.census_data_g02
				VALUES %s
		"""

		values = dataFrame.to_dict('split')
		values = values['data']

		result = execute_values(conn_ps.cursor(), sql, values, page_size=len(dataFrame))
		conn_ps.commit()
	else:
		None

	return None

def import_raw_listings(**kwargs):
	file_path = AIRFLOW_DATA
	listings = ["01_2021.csv",
				"02_2021.csv",
				"03_2021.csv",
	 			"04_2021.csv",
				"05_2020.csv",
				"06_2020.csv",
				"07_2020.csv",
				"08_2020.csv",
				"09_2020.csv",
				"10_2020.csv",
				"11_2020.csv",
				"12_2020.csv"]

	ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
	conn_ps = ps_pg_hook.get_conn()

	for l in listings:
		dataFrame = pd.read_csv(file_path + l)

		if len(dataFrame) > 0:
			sql = """
				INSERT INTO RAW.listings
					VALUES %s
			"""

			values = dataFrame.to_dict('split')
			values = values['data']

			result = execute_values(conn_ps.cursor(), sql, values, page_size=len(dataFrame))
			conn_ps.commit()
		else:
			None

	return None

def import_raw_lga_code_name(**kwargs):
	file_path = AIRFLOW_DATA + "NSW_LGA_CODE.csv"
    
	ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
	conn_ps = ps_pg_hook.get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.lga_code_name
				VALUES %s
		"""

		values = dataFrame.to_dict('split')
		values = values['data']

		result = execute_values(conn_ps.cursor(), sql, values, page_size=len(dataFrame))
		conn_ps.commit()
	else:
		None

	return None

def import_raw_lga_name_suburb(**kwargs):
	file_path = AIRFLOW_DATA + "NSW_LGA_SUBURB.csv"
    
	ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
	conn_ps = ps_pg_hook.get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.lga_name_suburb
				VALUES %s
		"""

		values = dataFrame[["LGA_NAME", "SUBURB_NAME"]].to_dict('split')
		values = values['data']

		result = execute_values(conn_ps.cursor(), sql, values, page_size=len(dataFrame))
		conn_ps.commit()
	else:
		None

	return None

#########################################################
#
#   DAG Operator Setup
#
#########################################################


import_raw_census_data_g01_task = PythonOperator(
    task_id="import_raw_census_data_g01",
    python_callable=import_raw_census_data_g01,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_raw_census_data_g02_task = PythonOperator(
    task_id="import_raw_census_data_g02",
    python_callable=import_raw_census_data_g02,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_raw_listings_task = PythonOperator(
    task_id="import_raw_listings",
    python_callable=import_raw_listings,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_raw_lga_code_name_task = PythonOperator(
    task_id="import_raw_lga_code_name",
    python_callable=import_raw_lga_code_name,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_raw_lga_name_suburb_task = PythonOperator(
    task_id="import_raw_lga_name_suburb",
    python_callable=import_raw_lga_name_suburb,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_raw_census_data_g01_task >> import_raw_listings_task >> import_raw_census_data_g02_task >> import_raw_lga_code_name_task >> import_raw_lga_name_suburb_task
