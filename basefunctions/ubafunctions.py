import json
import requests
import pandas as pd


# UBA has phenomenonTimes end with hour 24 instead of 0 the next day. datetime cant deal with that, have to replace
def todatetimeUTCstring(string):
    if(string[-8:-6] == '24'):
        res = pd.to_datetime(string.replace(" 24:"," 23:")) + pd.Timedelta('1 hour')
    else: 
        res = pd.to_datetime(string)
    return (res - pd.Timedelta('1 hour')).strftime("%Y-%m-%d" + "T" + "%H" + ":00:00.000Z")


# Function that requests data from UBA API and returns a pandas dataframe
def requestUbaData(timestamp_from,timestamp_to,deby):


    operatordomain = "umweltbundesamt.de"
    current_component = "1" #PM10
    current_scope = "2" #hourly averages

    req = json.loads(requests.get("https://api.smartaq.net/v1.0/Things?$filter=properties/hardware.id eq '" + deby + "' and properties/operator.domain eq '" + operatordomain + "'&$expand=Datastreams").text)["value"]
    
    if(len(req)>=1):
        thing = req[0]
    else:
        print("no devices found with id " + deby)
        return ""
    
    station_no = thing["properties"]["station_no"]

    for stream in thing["Datastreams"]:

        start_date=timestamp_from.split("T")[0]
        start_result_time=str(int(timestamp_from.split("T")[1].split(":")[0]) + 1)
        end_date=timestamp_to.split("T")[0]
        end_result_time=str(int(timestamp_to.split("T")[1].split(":")[0]) + 1)

        link = "https://www.umweltbundesamt.de/api/air_data/v2/measures/json?date_from=" + start_date + "&time_from=" + start_result_time + "&date_to=" + end_date + "&time_to=" + end_result_time + "&station=" + str(station_no) + "&component=" + current_component + "&scope=" + current_scope
        
        print("requesting data from: ")
        print(link)
        
        data = json.loads(requests.get(link).text)

        val_index = data["indices"]["data"]["station id"]["date start"].index("value")
        end_index = data["indices"]["data"]["station id"]["date start"].index("date end")

        #function todatetimeUTCstring converts to pandas datetime, converts CET to UTC, then outputs ISO string
        list_of_results = [
            {
                "result":data['data'][str(station_no)][x][val_index],
                "resultTime":todatetimeUTCstring(data['data'][str(station_no)][x][end_index]),
                "phenomenonTime": todatetimeUTCstring(x) + "/" + todatetimeUTCstring(data['data'][str(station_no)][x][end_index])
            } 
            for x in data['data'][str(station_no)].keys()
            if str(station_no) in data['data'].keys()
        ]

        return pd.DataFrame(list_of_results)

# Function that formats the dataframe output of requestUbaData to a standardized saqn format
def formatDataframe(df):

    # set resultTime as index
    df["time"] = pd.to_datetime(df["resultTime"])
    df = df.set_index("time", drop=True).sort_index()

    # rename result column to match ObservedProperty
    res = df.rename(columns={
        "result": "saqn:op:mcpm10"
    })
    return res