from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
from pprint import pprint

import pandas as pd
import json
import requests
import re
from statistics import mode
import datetime as dt
from joblib import delayed, Parallel

from live_parser.basefunctions import ftpfunction as ftpfunc
from live_parser.basefunctions import parserfunctions as pf
from live_parser.basefunctions import grimmfunctions as grimm
from live_parser.basefunctions import requestfunction as requestfunc

# local file that is not checked into github, containing the target credentials where to parse data from
import saqncredentials

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'teco', # required
    'depends_on_past': False, # success of the previous run of this task
    'start_date': seven_days_ago, # required, catchup is turned off in airflow.cfg
    'email': ['tremper@teco.edu'],
    'retries': 1,
}

dagOPC = DAG(
    dag_id='EDM80OPC_Live_Parser', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='1,31 * * * *'
)

dagNEPH = DAG(
    dag_id='EDM80NEPH_Live_Parser', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='5,35 * * * *'
)

dag164 = DAG(
    dag_id='EDM164OPC_Live_Parser', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='9,39 * * * *'
)

# FUNCTIONS

targeturl = "https://api.smartaq.net/v1.0"
operatordomain = "grimm-aerosol.com"
folder = saqncredentials.grimm.folder_live # folder from where to parse live data as specified in saqncredentials.py
allthings = ftpfunc.getData(folder) # list of all things available in the folder

# recognize the device types EDM80OPC, EDM80NEPH and EDM164OPC
EDM80OPCpattern=re.compile("(SN19[0-9]{3})")
EDM80OPCs = list(filter(EDM80OPCpattern.match,allthings))

EDM80NEPHpattern=re.compile("(SN17[0-9]{3})")
EDM80NEPHs = list(filter(EDM80NEPHpattern.match,allthings))

EDM164OPCpattern=re.compile("(OPC-[0-9]{3})")
EDM164OPCs = list(filter(EDM164OPCpattern.match,allthings))




def parse_live(thing, **kwargs):
    filelist=ftpfunc.getData(folder + "/" + thing)

    # reduce filelist so it contains the last 5 days plus the day 2 weeks ago
    today = datetime.now()
    cropped_filelist = [file for file in filelist if datetime.strptime(file[:10],'%Y-%m-%d')+ timedelta(5) >= today]
    cropped_filelist += [file for file in filelist if (datetime.strptime(file[:10],'%Y-%m-%d') + timedelta(14)).date() == today.date()]

    for file in cropped_filelist:

        filepath = folder + "/" + thing + "/" + file
        
        try:
            # get the file, parse it and format it for further progressing
            df=grimm.parseGrimmFile(filepath)
            df_formatted=grimm.formatDataframe(df,filepath)

            # Check Serial Number Column whether they are all the same
            serialmode=mode(df_formatted["hardware.id"])

            # get the database info corresponding to the thing and its datastreams
            saqnthing = pf.getThingFromProperties(targeturl, **{"operator.domain": operatordomain}, **{"hardware.id": serialmode})

            sess = requestfunc.session(3, 2)
            saqndatastreams = json.loads(sess.get(saqnthing["Datastreams@iot.navigationLink"] + "?$expand=ObservedProperty").text)["value"]

            # for each datastream, check for missing observations
            res=Parallel(n_jobs=2)(delayed(pf.post_difference)(targetdatastream,df_formatted) for targetdatastream in saqndatastreams)
            print("success at file: " + filepath + " --- result: ")
            print(res)
        except: 
            print("failed at file: " + filepath)

def my_sleeping_function(random_base):
    '''This is a function that will run within the DAG execution'''
    time.sleep(random_base)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

# OPERATORS/TASKS

for edm80opc in ['SN19006']: # EDM80OPCs:
    '''
    Create Tasks to parse all EDM80OPC devices
    '''
    task = PythonOperator(
        task_id='EDM80OPC_parse_live_'+edm80opc,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live,
        op_kwargs={"thing": edm80opc},
        dag=dagOPC
    )

for edm80neph in EDM80NEPHs:
    '''
    Create Tasks to parse all EDM80NEPH devices
    '''
    task = PythonOperator(
        task_id='EDM80NEPH_parse_live_'+edm80neph,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live,
        op_kwargs={"thing": edm80neph},
        dag=dagNEPH
    )

for edm164opc in EDM164OPCs:
    '''
    Create Tasks to parse all EDM164OPC devices
    '''
    task = PythonOperator(
        task_id='EDM164OPC_parse_live_'+edm164opc,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live,
        op_kwargs={"thing": edm164opc},
        dag=dag164
    )
