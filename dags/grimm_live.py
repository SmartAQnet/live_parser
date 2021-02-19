from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import pandas as pd
import json
import requests
import re
from statistics import mode

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
    'retries': 1
}

dagOPC = DAG(
    dag_id='EDM80OPC_Live_Parser', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='1 * * * *' # at :01 every hour
)

dagNEPH = DAG(
    dag_id='EDM80NEPH_Live_Parser', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='5 * * * *' # at :05 every hour
)

dag164 = DAG(
    dag_id='EDM164OPC_Live_Parser', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='9 * * * *' # at :09 every hour
)


dagOPCdaily = DAG(
    dag_id='EDM80OPC_Live_Parser_Daily', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='45 2 * * *' # at 02:45 every day
)

dagNEPHdaily = DAG(
    dag_id='EDM80NEPH_Live_Parser_Daily', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='49 2 * * *' # at 02:49 every day 
)

dag164daily = DAG(
    dag_id='EDM164OPC_Live_Parser_Daily', 
    default_args=args,
    description="Download and parse data from the Grimm ftp-server",
    schedule_interval='53 2 * * *' # at 02:53 every day
)

# FUNCTIONS

targeturl = "https://api.smartaq.net/v1.0"
operatordomain = "grimm-aerosol.com"
folder = saqncredentials.grimm.folder_live # folder from where to parse live data as specified in saqncredentials.py
allthings = grimm.ftp_getData(folder) # list of all things available in the folder

# recognize the device types EDM80OPC, EDM80NEPH and EDM164OPC
EDM80OPCpattern=re.compile("(SN19[0-9]{3})")
EDM80OPCs = list(filter(EDM80OPCpattern.match,allthings))

EDM80NEPHpattern=re.compile("(SN17[0-9]{3})")
EDM80NEPHs = list(filter(EDM80NEPHpattern.match,allthings))

EDM164OPCpattern=re.compile("(OPC-[0-9]{3})")
EDM164OPCs = list(filter(EDM164OPCpattern.match,allthings))



def parse_live_daily(thing, **kwargs):
    filelist = grimm.ftp_getData(folder + "/" + thing)

    # reduce filelist so it contains the last 5 days (without today!) plus the day 2 weeks ago
    today = datetime.today()
    cropped_filelist = [file for file in filelist if (datetime.strptime(file[:10],'%Y-%m-%d') + timedelta(14)).date() == today.date()]
    cropped_filelist += [file for file in filelist if ((datetime.strptime(file[:10],'%Y-%m-%d') + timedelta(5) >= today) & (datetime.strptime(file[:10],'%Y-%m-%d') + timedelta(1) <= today))]
    cropped_filelist.sort()

    if(cropped_filelist == []):
        print("no files to parse")
    else:
        print("Parsing files:")
        print(cropped_filelist)

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
            # res=Parallel(n_jobs=2)(delayed(pf.post_difference)(targetdatastream,df_formatted) for targetdatastream in saqndatastreams)
            print("Posting from file: " + filepath + " --- results: ")
            for targetdatastream in saqndatastreams:
                symmdiff = pf.getSymmDiff(targetdatastream,df_formatted)
                print("Datastream observing property: " + targetdatastream["ObservedProperty"]["@iot.id"])
                if(len(symmdiff) > 0):
                    res = pf.postObservations(targetdatastream, symmdiff)
                    print(res)
                else:
                    print("no new observations to post")
        except: 
            print("failed at file: " + filepath)


def parse_live(thing, **kwargs):
    filelist = grimm.ftp_getData(folder + "/" + thing)

    # only get the current day
    cropped_filelist = list(filter(lambda x: x[:10] == datetime.today().isoformat()[:10], filelist))

    if(cropped_filelist == []):
        print("no file to parse available for today")

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
            # res=Parallel(n_jobs=2)(delayed(pf.post_difference)(targetdatastream,df_formatted) for targetdatastream in saqndatastreams)
            print("Posting from file: " + filepath + " --- results: ")
            for targetdatastream in saqndatastreams:
                symmdiff = pf.getSymmDiff(targetdatastream,df_formatted)
                print("Datastream observing property: " + targetdatastream["ObservedProperty"]["@iot.id"])
                if(len(symmdiff) > 0):
                    res = pf.postObservations(targetdatastream, symmdiff)
                    print(res)
                else:
                    print("no new observations to post")
        except: 
            print("failed at file: " + filepath)


# OPERATORS/TASKS

# Half-hourly Tasks

for edm80opc in EDM80OPCs: #['SN19001','SN19002','SN19003','SN19004','SN19005','SN19006','SN19007','SN19009','SN19010']:
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


# Daily Tasks

for edm80opc in EDM80OPCs: #['SN19001','SN19002','SN19003','SN19004','SN19005','SN19006','SN19007','SN19009','SN19010']:
    '''
    Create Tasks to parse all EDM80OPC devices
    '''
    task = PythonOperator(
        task_id='EDM80OPC_parse_live_daily_'+edm80opc,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live_daily,
        op_kwargs={"thing": edm80opc},
        dag=dagOPCdaily
    )

for edm80neph in EDM80NEPHs:
    '''
    Create Tasks to parse all EDM80NEPH devices
    '''
    task = PythonOperator(
        task_id='EDM80NEPH_parse_live_daily_'+edm80neph,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live_daily,
        op_kwargs={"thing": edm80neph},
        dag=dagNEPHdaily
    )

for edm164opc in EDM164OPCs:
    '''
    Create Tasks to parse all EDM164OPC devices
    '''
    task = PythonOperator(
        task_id='EDM164OPC_parse_live_daily_'+edm164opc,
        provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
        python_callable=parse_live_daily,
        op_kwargs={"thing": edm164opc},
        dag=dag164daily
    )