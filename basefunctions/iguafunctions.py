import json
import requests
import pandas as pd
import basefunctions.ftpfunction as gf
import datetime
import numpy as np

# works with Alphasense/... .txt files

# function that gets a file from the igua server when given a filepath
# returns the pandas dataframe from the parsed file

def parseIguaFile(filepath):

    try:
        content = requests.get(filepath).text

        headerlist = content.split("\n")[0].split("\t")
        bodylist = list(map(lambda x: x.split("\t"), content.split("\n")[1:]))

        if(bodylist[0] == ''):
            bodylist.pop(0)  # dropping the first line resulting from the the double linebreak delimiter after header

        if(bodylist[-1] == ''):
            bodylist.pop()  # dropping the empty line resulting from the last linebreak delimiter

        df = pd.DataFrame(data=bodylist, columns=headerlist)

    except:
        raise SystemExit("header or body format did not match the expected format. Aborting.")

    return df


# function that formats a Dataframe to SAQN FROST Variables for standardized procession

def formatDataframe(df):

    # function to use for sanity checks
    def throwColumnError(col):
        print("Error: " + col + " column not where expected to be")

    headerlist = df.keys()

    # time stuff
    # time format used in strftime conversion
    timeformat = '%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z'

    utckey = headerlist[2]

    # sanity check
    if("UTC".lower() not in utckey.lower()):
        throwColumnError("utc datetime")

    # Check and convert UTC Time Column
    try:
        # remove rows with falsy/empty entries in the UTC column
        dfred = df[df[utckey].astype(bool)]
    except:  # using except only to see where stuff went wrong. exit anyway
        raise SystemExit("UTC Timestamp Column Error. Aborting.")

    else:
        # convert utc to timestamp
        dfred[utckey] = dfred[utckey].map(lambda x: pd.to_datetime(x, utc=True))
        # set as index
        dfred = dfred.set_index(utckey, drop=False).sort_index()
        # convert utc time column into same format as saqn server
        dfred[utckey] = dfred[utckey].map(lambda x: x.strftime(timeformat))

        # rename columns
        res = dfred.rename(columns={
            utckey: "resultTime",
            headerlist[0]: "lat",
            headerlist[1]: "lon",
            headerlist[3]: "alt",
            headerlist[23]: "saqn:op:mcpm10",
            headerlist[14]: "saqn:op:mcpm2p5",
            headerlist[19]: "saqn:op:mcpm1",
            headerlist[7]: "saqn:op:ta",
            headerlist[16]: "saqn:op:plev"
        })

    return res


# check if a datetime of type YYYY-MM-DD is in a timespan-list of [DD.MM.YYYY, DD.MM.YYYY] where the last one can also be named 'today'

def is_in_timespan(to_check,timespan):
    
    check = datetime.datetime.strptime(to_check,'%Y-%m-%d')
    
    start_raw = timespan[0]
    end_raw = timespan[1]
    
    start = datetime.datetime.strptime(start_raw,'%d.%m.%Y')
    
    if(end_raw == 'today'):
        end = datetime.datetime.now()
    else:
        end = datetime.datetime.strptime(end_raw,'%d.%m.%Y')
    
    
    return((start <= check) and (check <= end))


# check which thing the sensor was mounted on at a datetime of type YYYY-MM-DD. The thing-sensor dict is http://srv.geo.uni-augsburg.de/data/aux/alphasense/AS_fahrrad_stat_uav.json
# returns shortname:hardware.id

def get_thing_id(to_check,records):
    
    mask = np.array([is_in_timespan(to_check,x[0]) for x in records])

    active_record = np.array(records)[mask]
    
    if(active_record.size == 0):    
        return(False)
    else:
        return(active_record[0][1])
    