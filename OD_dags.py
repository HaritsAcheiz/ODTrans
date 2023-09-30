import ODTrans
from airflow import DAG, Task
from airflow.operator.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from dotenv import load_dotenv
import os

load_dotenv()

@dag(schedule=None, start_date='2023-09-28', catchup=False)
def ODT():
	
	@task
	def connect(host, database, username, password):
		pass
	
	@task
	def extract_gate_in(conn, query):
		pass

	@task extract_gate_out(conn, query):
		pass

	@task extract_tob_in(conn, query):
		pass

	@task extract_tob_out(conn, query):
		pass
	
	@task
	def transform(df):
		pass

	conn = connect(host=os.getenv('HOST'), database=os.getenv('DATABASE'), username=('DBUSERNAME'), password=('DBPASSWORD'))
	extract_gate_in(conn=conn)

ODT()