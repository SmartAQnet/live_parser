import json, requests
import datetime as dt
from pandas import to_datetime  # because pandas to_datetime > python datetime

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import numpy as np

def session(con, back):

    s = requests.Session()
    retry = Retry(connect=con, backoff_factor=back)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    return s

def get_data(url):
    data = []

    while url:
        req = json.loads(sess.get(url).text)
        if "@iot.nextLink" in req.keys():
            url = req["@iot.nextLink"]
        else:
            url = None
        data += req["value"]
    return data

sess = session(5,3)

yesterday = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"


qualifying_devices = json.loads(requests.get("https://api.smartaq.net/v1.0/Things?$filter=properties/shortname%20eq%20%27EDM80OPC%27&$expand=Datastreams").text)["value"]

qualifying_devices_ds_urls = []

for device in qualifying_devices:
    qualifying_ds = [device["@iot.selfLink"] + "/Datastreams('" + ds["@iot.id"] + "')" for ds in device["Datastreams"] if "resultTime" in ds.keys() and ds["resultTime"] and to_datetime(ds["resultTime"].split("/")[-1]) > to_datetime(yesterday)]
    if qualifying_ds:
        qualifying_devices_ds_urls += [np.random.choice(qualifying_ds)]
    del qualifying_ds


for ds_url in qualifying_devices_ds_urls:

    obs_filter = "$filter=resultTime%20ge%20" + yesterday
    expand_foi = "$expand=FeatureOfInterest"

    url = ds_url + "/Observations?" + obs_filter + "&" + expand_foi
    print(url)
    test_data = get_data(url)

    foi_gps = np.array([g["FeatureOfInterest"]["feature"]["coordinates"] for g in test_data if len(g["FeatureOfInterest"]["feature"]["coordinates"])==3]) # all scouts have an altitude value. this skips error entries (which do happen when the file breaks for some reason)

    # retrive thing url by removing the "/Datastream('...')" from the full ds url
    loc_url = "/".join(ds_url.split("/")[:-1]) + "/Locations"

    loc_data = get_data(loc_url)
    loc_gps = np.array(loc_data[0]["location"]["coordinates"])

    if len(foi_gps)>3:
        print(f"{len(foi_gps)} datapoints")

        # allowing 11m uncertainty
        # at 48.3: 10.9 -> 10.8 --> 0.1 lat is 11km --> 0.0002 is 0.011 km
        # at 10.9: 48.3 -> 48.4 --> 0.1 lon is 11km --> 0.0002 is 0.011 km

        if ((np.mean(foi_gps[:,:2], axis=0) - loc_gps[:2]) < np.array([0.0001, 0.0001]) + 3*np.std(foi_gps[:,:2], axis=0)).any():  # ignore altitude values because optional
            print("foi and loc fit together")
        else:
            print(f"foi and loc not fitting! Check Location:")
            print(f"{loc_data[0]['name']} at {loc_gps[:2]}")
            print(f"Thing: {json.loads(requests.get('/'.join(ds_url.split('/')[:-1])).text)['name']}")
            print(f"Observations mean, std at: {np.mean(foi_gps[:,:2], axis=0)} +/- {np.std(foi_gps[:,:2], axis=0)}")
    else:
        print("less than 3 observations")


