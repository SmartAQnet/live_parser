from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import pandas as pd
import json
import requests
import re
from statistics import mode
from joblib import delayed, Parallel

from live_parser.basefunctions import ftpfunction as ftpfunc
from live_parser.basefunctions import parserfunctions as pf
from live_parser.basefunctions import grimmfunctions as grimm
from live_parser.basefunctions import requestfunction as requestfunc

# local file that is not checked into github, containing the target credentials where to parse data from
import saqncredentials

# set to midnight this day. Causes no problems with historic parsers since they are set to @once and not periodic. 
this_day = datetime.combine(datetime.today(), datetime.min.time())

# default args
args = {
    'owner': 'teco', # required
    'depends_on_past': False, # success of the previous run of this task
    'start_date': this_day, # required, catchup is turned off in airflow.cfg
    'email': ['tremper@teco.edu'],
    'retries': 3 # set higher for historic parser since they are not set periodically. 
}




# GLOBALS

targeturl = "https://api.smartaq.net/v1.0"
operatordomain = "grimm-aerosol.com"
folder_h = saqncredentials.grimm.folder_historic # folder from where to parse historic data as specified in saqncredentials.py
folder_l = saqncredentials.grimm.folder_live # folder from where to parse historic data as specified in saqncredentials.py

allthings_h = ftpfunc.getData(folder_h) # list of all things available in the folder
allthings_l = ftpfunc.getData(folder_l) # list of all things available in the folder

# recognize the device types EDM80OPC, EDM80NEPH and EDM164OPC
EDM80OPCpattern=re.compile("(SN19[0-9]{3})")
EDM80OPCs_h = list(filter(EDM80OPCpattern.match,allthings_h))
EDM80OPCs_l = list(filter(EDM80OPCpattern.match,allthings_l))

EDM80NEPHpattern=re.compile("(SN17[0-9]{3})")
EDM80NEPHs_h = list(filter(EDM80NEPHpattern.match,allthings_h))
EDM80NEPHs_l = list(filter(EDM80NEPHpattern.match,allthings_l))

EDM164OPCpattern=re.compile("(OPC-[0-9]{3})")
EDM164OPCs_h = list(filter(EDM164OPCpattern.match,allthings_h))
EDM164OPCs_l = list(filter(EDM164OPCpattern.match,allthings_l))



# # FUNCTIONS
# Parse function
def parse_file(folder, thing, datfile, **kwargs):

    filepath = folder + "/" + thing + "/" + datfile

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


# function that creates a dag with its tasks
def create_dag(dag_id, folder, edm80opc):

    dagOPC = DAG(
        dag_id=dag_id, 
        default_args=args,
        description="Download and parse data from the Grimm ftp-server",
        schedule_interval='@once'
    )

    with dagOPC:
        dagOPC.doc_md = dag_hist_doc(edm80opc) # Markdown Documentation for the DAG
        files = ftpfunc.getData(folder + "/" + edm80opc) 
        files.sort()

        for datfile in files: 
            '''
            Create Tasks to parse all files
            '''
            task = PythonOperator(
                task_id='EDM80OPC_parse_history_'+edm80opc+"_"+datfile,
                provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
                python_callable=parse_file,
                op_kwargs={"folder": folder, "thing": edm80opc, "datfile": datfile},
                dag=dagOPC
    )

    return dagOPC

# function that creates a markdown documentation for a dag
def dag_hist_doc(device):
    return('''
    
    ### Purpose

    This DAG parses all Data that has been collected for device ''' + str(device) + ''' within the SmartAQnet Project (2017 - 2020).
    
    Each Task parses Data taken on a single day, which corresponds to one file. The Data is sent to the SmartAQnet Database ''' + targeturl + '''
    
    ''')



# # DAGS AND TASKS

# loop that creates a dag with its tasks for each device
# for some reason, looping over too much at once produces a timeout. Possibly impatient with the ftp requests. 
for edm80opc in EDM80OPCs_h[:10]:
    dag_id = 'EDM80OPC_'+edm80opc+'_historic_Parser'
    globals()[dag_id] = create_dag(dag_id,folder_h,edm80opc)

    if(edm80opc in EDM80OPCs_l): # if there is any data after the official project end, parse that too
        dag_id = 'EDM80OPC_'+edm80opc+'_historic_to_live_Parser'
        globals()[dag_id] = create_dag(dag_id,folder_l,edm80opc)


# for edm80opc in EDM80OPCs[10:20]:
#     dag_id = 'EDM80OPC_'+edm80opc+'_historic_Parser'
#     globals()[dag_id] = create_dag(dag_id,edm80opc)

# for edm80opc in EDM80OPCs[20:30]:
#     dag_id = 'EDM80OPC_'+edm80opc+'_historic_Parser'
#     globals()[dag_id] = create_dag(dag_id,edm80opc)

# for edm80opc in EDM80OPCs[30:]:
#     dag_id = 'EDM80OPC_'+edm80opc+'_historic_Parser'
#     globals()[dag_id] = create_dag(dag_id,edm80opc)



# dagNEPH = DAG(
#     dag_id='EDM80NEPH_Live_Parser', 
#     default_args=args,
#     description="Download and parse data from the Grimm ftp-server",
#     schedule_interval='5,35 * * * *'
# )

# dag164 = DAG(
#     dag_id='EDM164OPC_Live_Parser', 
#     default_args=args,
#     description="Download and parse data from the Grimm ftp-server",
#     schedule_interval='9,39 * * * *'
# )


# for edm80neph in EDM80NEPHs:
#     '''
#     Create Tasks to parse all EDM80NEPH devices
#     '''
#     task = PythonOperator(
#         task_id='EDM80NEPH_parse_live_'+edm80neph,
#         provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
#         python_callable=parse_live,
#         op_kwargs={"thing": edm80neph},
#         dag=dagNEPH
#     )

# for edm164opc in EDM164OPCs:
#     '''
#     Create Tasks to parse all EDM164OPC devices
#     '''
#     task = PythonOperator(
#         task_id='EDM164OPC_parse_live_'+edm164opc,
#         provide_context=True, # passes execution date to task, see https://godatadriven.com/blog/the-zen-of-python-and-apache-airflow/#3-passing-context-to-tasks
#         python_callable=parse_live,
#         op_kwargs={"thing": edm164opc},
#         dag=dag164
#     )
