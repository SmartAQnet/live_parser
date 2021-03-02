from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import pandas as pd
import json
import requests

from live_parser.basefunctions import parserfunctions as pf
from live_parser.basefunctions import ubafunctions as uba
from live_parser.basefunctions import requestfunction as requestfunc

# local file that is not checked into github, containing the target credentials where to parse data from
# import saqncredentials

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'teco', # required
    'depends_on_past': False, # success of the previous run of this task
    'start_date': seven_days_ago, # required, catchup is turned off in airflow.cfg
    'email': ['tremper@teco.edu'],
    'retries': 1
}

dag = DAG(
    dag_id='UBA_Live_Parser', 
    default_args=args,
    description="Download and parse data from the UBA API",
    schedule_interval='15 * * * *' # at :15 every hour
)

# FUNCTIONS

targeturl = "https://api.smartaq.net/v1.0"
operatordomain = "umweltbundesamt.de"



def parse_live(deby, **kwargs):

    
    # check how far back
    backtrace = "2 days"
    today = datetime.today()
    timeformat = '%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z'

    timestamp_from = datetime.strftime(today - pd.Timedelta(backtrace),timeformat)
    timestamp_to = datetime.strftime(today,timeformat)


    # get the file, parse it and format it for further progressing
    df = uba.requestUbaData(timestamp_from,timestamp_to,deby)
    df_formatted = uba.formatDataframe(df)

    # get the database info corresponding to the thing and its datastreams
    saqnthing = pf.getThingFromProperties(targeturl, **{"operator.domain": operatordomain}, **{"hardware.id": deby})

    sess = requestfunc.session(3, 2)
    saqndatastreams = json.loads(sess.get(saqnthing["Datastreams@iot.navigationLink"] + "?$expand=ObservedProperty").text)["value"]

    # for each datastream, check for missing observations and post if missing
    print("Posting results for " + deby + ": ")
    for targetdatastream in saqndatastreams:
        symmdiff = pf.getSymmDiff(targetdatastream,df_formatted)
        print("Datastream observing property: " + targetdatastream["ObservedProperty"]["@iot.id"])
            if(len(symmdiff) > 0):
                res = pf.postObservations(targetdatastream, symmdiff)
                print(res)
            else:
                print("no new observations to post")




# OPERATORS/TASKS

uba_stations = json.loads(requests.get("https://api.smartaq.net/v1.0/Things?$filter=properties/operator.domain eq 'umweltbundesamt.de'").text)["value"]
station_ids = [stat["properties"]["hardware.id"] for stat in uba_stations]

# Hourly Tasks

for stat_id in station_ids: 
    '''
    Create Tasks to parse all UBA Stations
    '''
    task = PythonOperator(
        task_id='UBA_parse_live_'+stat_id,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live,
        op_kwargs={"deby": stat_id},
        dag=dag
    )