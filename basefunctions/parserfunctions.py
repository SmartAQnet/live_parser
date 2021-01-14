import hashlib
import json
import basefunctions.requestfunction as requestfunc
import pandas as pd


# returns the first 7 digits of the sha1 hash of the input string
def hashfunc7(inputstring):
    return hashlib.sha1(bytes(str(inputstring), 'utf-8')).hexdigest()[0:7]


# returns the full 40 digits of the sha1 hash of the input string
def hashfunc40(inputstring):
    return hashlib.sha1(bytes(str(inputstring), 'utf-8')).hexdigest()


# function to try float conversion in lambda function
def tryfloat(expr):
    try:
        return float(expr)
    except:
        return None


# Get Database Info
# Get Thing entry via any properties passed to kwargs as **{key: value}
def getThingFromProperties(url, **kwargs):

    # initialize a request session (retries 3, backoff factor 2)
    sess = requestfunc.session(3, 2)

    # remove trailing slash if necessary
    if(url[-1] == "/"):
        url = url[:-1]

    getlink = url + "/Things?$filter="
    
    for arg in kwargs:
        getlink += "properties/" + arg + " eq " + "'" + kwargs.get(arg) + "'" + " and "
    
    getlink = getlink[:-5]

    # try three times, sometimes the server loses a request
    matchingsaqnthings = json.loads(sess.get(getlink).text)["value"]

    assert (len(matchingsaqnthings) <= 1), "error: multiple devices in db matching specifications"
    assert (len(matchingsaqnthings) != 0), "error: no device in db matching specifications"
    res = matchingsaqnthings[0]

    return res


# returns the symmetric difference between the parsed file and the SAQN Server observations for a given datastream
# takes as arguments:
# 1. a json of a datastream (especially the return of a json.loads(requests.get(...datastream...).text)) request) which contains a
#  navigation link to the things datastreams under the key ["Datastreams@iot.navigationLink"]
#  and a link to the things ObservedProperty iot.id under the key ["ObservedProperty"]["@iot.id"]
# 2. the dataframe of observations from the parsed file
def getSymmDiff(targetdatastream, dfred):

    # initialize a request session (retries 3, backoff factor 2)
    sess = requestfunc.session(3, 2)

    # time format used in strftime conversion
    timeformat = '%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z'

    # retrieve obsprop from the saqn observedproperty iot.id
    obsprop = targetdatastream["ObservedProperty"]["@iot.id"]

    # check whether the datastreams ObservedProperty exists in the file
    try:
        assert (obsprop in dfred.keys()), "Datastream " + targetdatastream["name"] + " not in Dataframe"
    except AssertionError as e:
        print(e)
        return pd.DataFrame()

    # convert the result column of obsprop in the ftp dataframe from strings with commas into real floats
    dfred[obsprop] = dfred[obsprop].map(lambda x: tryfloat(str(x).replace(",", ".")))

    startiso = min(dfred.index).strftime(timeformat)
    endiso = max(dfred.index).strftime(timeformat)

    # get information from database and construct a dataframe from the existing data
    targetquery = targetdatastream["Observations@iot.navigationLink"] + "?$filter=resultTime ge " + startiso + " and resultTime le " + endiso
    obslist = json.loads(sess.get(targetquery).text)["value"]
    obsdf = pd.DataFrame(
        data=list(
            map(lambda x: [pd.to_datetime(x["resultTime"]), x["result"]], obslist)
            ),
        columns=["resultTime", obsprop]
        ).set_index("resultTime").sort_index()

    # get the symmetric difference of the saqn database slice and the grimm ftp slice
    symmdiff = pd.concat([dfred, obsdf])
    symmdiff = symmdiff[~symmdiff.index.duplicated(keep=False)]

    return symmdiff


# posts Observations to the SAQN Server
# (beginning needs to be redundant with getSymmDiff() because getSymmDiff() is optional)
# takes as arguments:
# 1. a json of a datastream (especially the return of a json.loads(requests.get(...datastream...).text)) request) which contains a
#  navigation link to the things datastreams under the key "Datastreams@iot.navigationLink"
# 2. the dataframe of observations from the parsed file
# 3. the key dictionary that translates between the database keys of the thing and the dataframe of observations
def postObservations(targetdatastream, dfred):

    # initialize a request session (retries 3, backoff factor 2)
    sess = requestfunc.session(3, 2)

    # time format used in strftime conversion
    timeformat = '%Y-%m-%d' + 'T' + '%H:%M:%S' + '.000Z'

    # retrieve obsprop from the saqn observedproperty iot.id
    obsprop = targetdatastream["ObservedProperty"]["@iot.id"]

    # check whether the datastreams ObservedProperty exists in the file
    try:
        assert (obsprop in dfred.keys()), "Datastream " + targetdatastream["name"] + " not in Dataframe"
    except AssertionError as e:
        print(e)
        return pd.DataFrame()

    # convert the result column of obsprop in the ftp dataframe from strings with commas into real floats
    dfred[obsprop] = dfred[obsprop].map(lambda x: tryfloat(str(x).replace(",", ".")))

    # observation counter for checking
    oc = {"success": 0, "failed": 0}

    if(len(dfred) > 0):
        for index, row in dfred.iterrows():

            observation = {
                "resultTime": index.strftime(timeformat),
                "result": row[obsprop]
                }

            # check if a time resolution is given and set phenomenonTime as interval
            try:
                assert ("timeResolution" in dfred.keys()), "no time resolution given"
            except AssertionError:
                observation["phenomenonTime"] = index.strftime(timeformat)
            else:
                timeresunit = "s"  # assuming seconds as units
                try:
                    observation["phenomenonTime"] = (index - pd.Timedelta(row["timeResolution"] + timeresunit)).strftime(timeformat) + "/" + index.strftime(timeformat)
                except TypeError:
                    observation["phenomenonTime"] = index.strftime(timeformat)

            # check if parameters are given (as json converted into a string)
            if("parameters" in dfred.keys()):
                try:
                    observation["parameters"] = json.loads(row["parameters"])
                except ValueError:
                    print("couldn't read Observation parameters JSON")

            # add FeatureOfInterest
            gps = []

            try:
                gps.append(float(row["lon"].replace("E", "").replace(",", ".")))
                gps.append(float(row["lat"].replace("N", "").replace(",", ".")))
            except KeyError as ke:
                print("Key " + ke + " not found")
                continue
            except ValueError as ve:
                print("Could not convert " + ve + " to Float")
                continue
            try:
                gps.append(float(row["alt"].replace("H", "").replace(",", ".")))
            except KeyError:
                pass  # altitude is optional
            except ValueError:
                pass

            observation["FeatureOfInterest"] = {
                "name": "n/a",
                "description": "n/a",
                "encodingType": "application/vnd.geo+json",
                "feature": {
                    "type": "Point",
                    "coordinates": gps
                    }
                }

            # generate iot.id
            try:
                observation["@iot.id"] = "saqn:o:" + hashfunc40(targetdatastream["properties"]["@iot.id"].split("saqn:ds:")[1] + ":" + observation["phenomenonTime"])
            except:  # bare except is fine: if it doesnt work for WHATEVER reason, it gets autogenerated
                pass

            # POST REQUEST HERE
            # print(observation)

            req = sess.post(targetdatastream["@iot.selfLink"] + "/Observations", json.dumps(observation))

            if((req.status_code == 200) or (req.status_code == 201)):
                oc["success"] += 1
            elif(req.status_code >= 400):
                oc["failed"] += 1
            else:
                raise SystemExit("Observation Post Status " + str(req.status_code))

        else:
            pass

    return oc

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


# Wrapper Function that combines getSymmDiff and postObservations for parallelization
def post_difference(targetdatastream, df_formatted):

    symmdiff = getSymmDiff(targetdatastream, df_formatted)
    if(len(symmdiff) > 0):
        posted = postObservations(targetdatastream, symmdiff)
        return(posted)
    else:
        return("None")

