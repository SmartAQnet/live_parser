import json
import requests
import pandas as pd
import basefunctions.ftpfunction as gf


# function that gets a file from the grimm ftp server when given a filepath
# returns the pandas dataframe from the parsed file

def parseGrimmFile(filepath):

    try:
        content = gf.getData(filepath)
        [header, body] = content.split("***begin of header***\n")[1].split("\n***end of header***\n")

        headerlist = header.split(";")
        bodylist = body.split("\n")[:-1]

        if(bodylist[0] == ''):
            bodylist.pop(0)  # dropping the first line resulting from the the double linebreak delimiter after header

        if(bodylist[-1] == ''):
            bodylist.pop()  # dropping the empty line resulting from the last linebreak delimiter

        df = pd.DataFrame(data=list(
            map(lambda x: x.split(";"), bodylist)),
            columns=headerlist
            )

    except:
        raise SystemExit("header or body format did not match the expected format. Aborting.")

    return df


# function that formats a Dataframe to SAQN FROST Variables for standardized procession

def formatDataframe(df, filepath):

    # function to use for sanity checks
    def throwColumnError(col):
        print("Error: " + col + " column not where expected to be: " + filepath)

    headerlist = df.keys()

    # identify device type
    if "OPC-" in filepath:
        devicetype = "EDM164OPC"
    elif "SN17" in filepath:
        devicetype = "EDM80NEPH"
    elif "SN19" in filepath:
        devicetype = "EDM80OPC"
    else:
        devicetype = None
        raise SystemExit("could not identify device type")

    # time stuff
    # time format used in strftime conversion
    timeformat = '%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z'

    # where is the UTC time column
    if(devicetype == "EDM164OPC"):
        utckey = headerlist[1]
    elif(devicetype == "EDM80NEPH"):
        utckey = headerlist[15]
    elif(devicetype == "EDM80OPC"):
        utckey = headerlist[34]

    # sanity check
    if("[UTC]".lower() not in utckey.lower()):
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

    # get keys from positions where they are supposed to be
    if(devicetype == "EDM164OPC"):

        # sanity check if headers seem to be where expected
        # soft checks to compensate for typos
        if("[local time]".lower() not in headerlist[0].lower()):
            throwColumnError("local time")
        if("software" not in headerlist[44].lower()):
            throwColumnError("Software Version")
        if("serial" not in headerlist[43].lower()):
            throwColumnError("Serial Number")
        if('lon' not in headerlist[2].lower()):
            throwColumnError("Longitude")
        if('lat' not in headerlist[3].lower()):
            throwColumnError("Latitude")
        if('height' not in headerlist[4].lower()):
            throwColumnError("Altitude")
        if('pm10' not in headerlist[5].lower()):
            throwColumnError("PM10")
        if('pm4' not in headerlist[6].lower()):
            throwColumnError("PM4")
        if(
            ('pm2.5' not in headerlist[7].lower())
            and
            ('pm2,5' not in headerlist[7].lower())
        ):
            throwColumnError("PM2.5")
        if('pm1' not in headerlist[8].lower()):
            throwColumnError("PM1")
        if('temp' not in headerlist[40].lower()):
            throwColumnError("Temperature")
        if('humid' not in headerlist[41].lower()):
            throwColumnError("Humidity")
        if('pres' not in headerlist[42].lower()):
            throwColumnError("Air Pressure")

        # rename columns
        res = dfred.rename(columns={
            utckey: "resultTime",
            headerlist[0]: "localTime",
            headerlist[2]: "lon",
            headerlist[3]: "lat",
            headerlist[4]: "alt",
            headerlist[5]: "saqn:op:mcpm10",
            headerlist[6]: "saqn:op:mcpm4",
            headerlist[7]: "saqn:op:mcpm2p5",
            headerlist[8]: "saqn:op:mcpm1",
            headerlist[40]: "saqn:op:ta",
            headerlist[41]: "saqn:op:hur",
            headerlist[42]: "saqn:op:plev",
            headerlist[43]: "hardware.id",
            headerlist[44]: "software.version"
        })

    elif(devicetype == "EDM80NEPH"):

        # sanity check if headers seem to be where expected
        # soft checks to compensate for typos

        # rename columns
        res = dfred.rename(columns={
            headerlist[0]: "localTime",
            headerlist[2]: "saqn:op:mcpm10",
            headerlist[3]: "saqn:op:mcpm4",
            headerlist[4]: "saqn:op:mcpm2p5",
            headerlist[5]: "saqn:op:mcpm1",
            headerlist[11]: "saqn:op:ta",
            headerlist[13]: "saqn:op:hur",
            utckey: "resultTime",
            headerlist[16]: "lon",
            headerlist[17]: "lat",
            headerlist[18]: "alt",
            headerlist[19]: "hardware.id",
            headerlist[20]: "timeResolution",
            headerlist[21]: "software.version",
            headerlist[23]: "lastCalibrationLocalTime"
        })

    elif(devicetype == "EDM80OPC"):

        # sanity check if headers seem to be where expected
        # soft checks to compensate for typos

        # rename columns
        res = dfred.rename(columns={
            utckey: "resultTime",
            headerlist[0]: "localTime",
            headerlist[1]: "hardware.id",
            headerlist[3]: "saqn:op:mcpm10",
            headerlist[4]: "saqn:op:mcpm2p5",
            headerlist[5]: "saqn:op:mcpm1",
            headerlist[35]: "lon",
            headerlist[36]: "lat",
            headerlist[37]: "alt",
            headerlist[38]: "timeResolution",
            headerlist[39]: "software.version",
            headerlist[40]: "lastCalibrationLocalTime"
        })

    return res





# takes a dataframe, checks it for a lastCalibrationLocalTime column
# adds a lastCalibrationUTC column
# and returns the whole dataframe with the appended column
# def addCalibrationUTC:
# something like
# try:
#     localutcdiff = pd.to_datetime(row["localTime"] + "+00:00") - index).round("h")
# except ValueError:  # catch 24 hour notation, replacing 24 with 00 and adding a day
#     if(" 24:" in row["localTime"]):
#         localutcdiff = pd.to_datetime(row["localTime"].replace(" 24:", " 00:") + "+00:00") + pd.Timedelta('1d') - index).round("h")
#         calibration_utc = (row["lastCalibrationLocalTime"] - localutcdiff).strftime(timeformat)
# but without the index row stuff, work with the whole dataframe somehow



# updates (PATCHES) the software number of a datastream if an observation has a newer one
# takes as arguments:
#  the line of the current observation as a one line series like dfred[softwareversionkey][[4]]
#  and the properties of the datastream as dictionary like datastream["properties"]
# NO deepcopy used here, the inputvariable of dsprops is overwritten since it is a shallow copy only. But thats fine as the
# intended use is anyway ds=manageSoftwareNo(...,ds)

def updateSoftwareNo(inputline, datastream):

    # time format used in strftime conversion
    timeformat = '%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z'

    patch = False

    if("software.version" not in list(datastream["properties"].keys())):
        datastream["properties"]["software.version"] = {}

    line = pd.Series({inputline.index[0].strftime(timeformat): inputline[0]})
    skeyseries = pd.Series(datastream["properties"]["software.version"])

    # if timestamp already exist, nothing happens
    if(line.index[0] not in skeyseries.index):
        resseries = skeyseries.append(line).sort_index()
        pos = resseries.index.get_loc(line.index[0])

        # if the series with new key appended has more than one entry
        if(len(resseries) > 1):
            # if there is one date before the new entry...
            if(pos > 0):
                # ...and they are equal drop it again and dont patch
                if(resseries[pos-1] == resseries[pos]):
                    resseries = resseries.drop(resseries.index[pos])
                # ...and they are not equal, PATCH
                elif(resseries[pos-1] != resseries[pos]):
                    patch = True

            # if there is one date after the new entry...
            if(pos < (len(resseries))-1):
                # ...and they are equal: delete the one after and PATCH
                if(resseries[pos+1] == resseries[pos]):
                    resseries = resseries.drop(resseries.index[pos+1])
                    patch = True

        datastream["properties"]["software.version"] = resseries.to_dict()

        if(patch is True):
            requests.patch(
                datastream["@iot.selfLink"],
                json={"properties": datastream["properties"]}
            )
            # write to log that the version number has changed
            print("patching datastream properties: new software version number")
            print(datastream["properties"])
            print("added: " + inputline)

    return datastream
