# from datetime import timedelta
# import datetime
# from airflow.models import DAG
# from airflow.operators.papermill_operator import PapermillOperator
# from airflow.operators.bash_operator import BashOperator
# import sys

# default_args = {
#     'owner': 'teco',
#     'depends_on_past': False,
#     'start_date': datetime.datetime(2020,12,11,18,00,00),
#     'email': ['tremper@teco.edu'],
#     'retries': 1,
# }

# dag = DAG(
#     dag_id="Grimm",
#     default_args=default_args,
#     description="Download Data from ftp server and parse",
#     # max_active_runs=1, # configured as default in airflow.cfg
#     schedule_interval='5,35 * * * *'
# )

# parse_historic = PapermillOperator(
#     task_id="historic_parser",
#     dag=dag,
#     input_nb="/usr/local/airflow/live_parser/grimm_liveparser.ipynb",
#     output_nb="/usr/local/airflow/live_parser/result.ipynb",
#     parameters=""
# )

# # parameters="dict(parameter_thing='SN19006', parameter_mode='historic')"