# live-parser
This repository contains the live running code of the scripts that parse data for the SmartAQnet database


## Basefunctions Library:

### parserfunctions as pf:

#### pf.getThingFromProperties(url, domain, serialno)

function that uses url, operator domain and serialno as string inputs and returns the saqn database entry of the thing    

#### pf.post_difference(targetdatastream, dataframe)

wrapper function that executes getSymmDiff and postObservations in succession

#### pf.getSymmDiff(targetdatastream, dataframe)

function that checks a list of observations against existing observations in the database by timestamp input is targetdatastream, dataframe. returns an equally formatted, reduced dataframe of missing observations

#### pf.postObservations(targetdatastream, dataframe)

function that posts observations to the server. returns a dictionary of counts of successfull and failed posts

### grimmfunctions as grimm:

#### grimm.parseGrimmFile(filepath)

function that grabs a file from the ftp server when given a path and parses it, returning a dataframe

#### grimm.formatDataframe(df,filepath)

function that formats the dataframe such that column heads are identical to saqn observedproperty iot.ids input is dataframe, filepath (to identify device type)

---

---

    
### OLD, not included

#### grimm.updateSoftwareNo(inputline,datastream)

function that checks the ["properties"]["software_version"] field of a datastream with a new input of the form pd.Series({pandas timestamp, value}). mutates the datastream object (shallow copy!) and performs a patch request if necessary. returns the datastream so datastream=grimm.updateSoftwareNo(inputline,datastream) makes sense although the shallow copy should mutate it anyway

--> probably belongs to pf library as it is not grimm specific

---

---

### TODO

- last calibration time to utc so that it is captured by the post function
- statistical algorithm to extract the real FoIs from the raw, noisy FoIs if necessary
- script to generate historical locations from FoI and vice versa if necessary