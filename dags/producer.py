from airflow import DAG,Dataset

from airflow.decorators import task

from datetime import datetime

my_file = Dataset('/tmp/my_file.py')
my_file_2 = Dataset('/tmp/my_file_2.py')

with DAG(
	dag_id = "producer",
	schedule = "@daily",
	start_date = datetime(2022,1,1),
	catchup = False

):
	@task(outlets=[my_file])
	def update_dateset():
		with open(my_file.uri,"a+") as f:
			f.write("producter update")
	update_dateset()

	@task(outlets=[my_file_2])
	def update_dateset_2():
		with open(my_file_2.uri,"a+") as f:
			f.write("producter update")

	update_dateset_2()