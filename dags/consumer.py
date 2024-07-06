from airflow import DAG, Dataset

from airflow.decorators import task

from datetime import datetime

my_file = Dataset('/tmp/my_file.py')
my_file_2 = Dataset('/tmp/my_file_2.py')

with DAG(
	dag_id = "consumer",
	schedule = [my_file,my_file_2],
	start_date=datetime(2022,1,1),
	catchup = False

):
	@task
	def read_dateset():
		with open(my_file.uri,'r') as f:
			print(f.read())
	read_dateset()