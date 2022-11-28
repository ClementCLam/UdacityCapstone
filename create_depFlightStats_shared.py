######################
# import all dependent libraries
######################
from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import explode
import time
from pyspark.sql.functions import to_date, from_utc_timestamp, unix_timestamp
from pyspark.sql.functions import month, year, date_format

#####################
# set up a SparkSession in loca lmode to ETL datasets in a data lake
#####################
spark = SparkSession \
    .builder \
    .appName("FlightStats from Cirium by airport") \
    .config('spark.ui.port',3000) \
    .getOrCreate()
#spark.sparkContext.getConf().getAll()  # check sparksession full info

#################################################################################
# Fetch datasets from API access via https protocol and save it to a json file
# FlightStatus&Track-> Historical-> HistoricalFlightStatus->Departures 2010-2020
#################################################################################
# declare RestAPI  parameters
params = {
    'appId': '*****',
    'appKey': '47*******************68381',
    'utc': 'false',
    'numHours': '6',  # max 
    'maxFlights': '500', #69
}

# RestAPI requests encapsulate in a function
# with HTTPS API error handling
def executeRestApi(y,m,d,h):
    response = None
    api='{}/{}/{}/{}'.format(y,m,d,h)
    rest_api = 'https://api.flightstats.com/flex/flightstatus/historical/rest/v3/json/airport/status/MEL/dep/'+api
    try:
        response = requests.get(rest_api, params=params)
    except Exception as e:
        return e
    if response != None and response.status_code == 200:
        return response.text
    else: 
        return None

#
# main codes - ETL
# Extract data via RestAPI requests
# Transform data using Spark to re-arrange data items
# Load data into parquet-format file ready for data analytics
#  
t0=time.time()
totalcount=0
DOM=[31,28,31,30,31,30,31,31,30,31,30,31]
for yr in range(10): #10
    yrcount=0
    for mth in range(12): #12
        mthcount=0
        for day in range(DOM[mth]): # as per DOM
            daycount=0
            for hour in range(4): #4
                jsondata = executeRestApi(2010+yr,mth+1,day+1,6*hour)
                open('data.json', 'w').write(jsondata)
                Df = spark.read.format("json")\
                    .option("inferSchema","true")\
                    .option("multiLine","true")\
                    .option("header","true")\
                    .load("data.json")
                try:
                    Df = Df.withColumn("flights",explode("flightStatuses")) # open an array
                    Df2 =Df.withColumn("flightId", Df["flights"].getItem("flightId"))\
                    .withColumn("carrierFsCode",Df["flights"].getItem("carrierFsCode"))\
                    .withColumn("flightNumber", Df["flights"].getItem("flightNumber"))\
                    .withColumn("departureAirportFsCode", Df["flights"].getItem("departureAirportFsCode"))\
                    .withColumn("arrivalAirportFsCode", Df["flights"].getItem("arrivalAirportFsCode"))\
                    .withColumn("departureDate", Df["flights"].getItem("departureDate"))\
                    .withColumn("arrivalDate", Df["flights"].getItem("arrivalDate"))\
                    .withColumn("operationalTimes", Df["flights"].getItem("operationalTimes"))
                    Df3 = Df2.withColumn("departureDateLocal", Df2["departureDate"].getItem("dateLocal"))\
                    .withColumn("arrivalDateLocal", Df2["arrivalDate"].getItem("dateLocal"))\
                    .withColumn("publishedDeparture", Df2["operationalTimes"].getItem("publishedDeparture"))\
                    .withColumn("publishedArrival", Df2["operationalTimes"].getItem("publishedArrival"))\
                    .withColumn("actualRunwayDeparture", Df2["operationalTimes"].getItem("actualRunwayDeparture"))\
                    .withColumn("actualRunwayArrival", Df2["operationalTimes"].getItem("actualRunwayArrival"))
                    Df4 = Df3.withColumn("publishedDepartureUtc", Df3["publishedDeparture"].getItem("dateUtc"))\
                    .withColumn("publishedArrivalUtc", Df3["publishedArrival"].getItem("dateUtc"))\
                    .withColumn("actualRunwayDepartureUtc", Df3["actualRunwayDeparture"].getItem("dateUtc"))\
                    .withColumn("actualRunwayArrivalUtc", Df3["actualRunwayArrival"].getItem("dateUtc"))
                    Df5 = Df4.withColumn("departureDateLocalYear", year("departureDateLocal"))\
                    .withColumn("departureDateLocalMonth", month("departureDateLocal"))\
                    .withColumn("arrivalDateLocalYear", year("arrivalDateLocal"))\
                    .withColumn("arrivalDateLocalMonth", month("arrivalDateLocal"))\
                    .withColumn("publishedDepartureEpochSec",unix_timestamp(from_utc_timestamp(("publishedDepartureUtc"),'UTC+10')))\
                    .withColumn("publishedArrivalEpochSec",unix_timestamp(from_utc_timestamp(("publishedArrivalUtc"),'UTC+10')))\
                    .withColumn("actualRunwayDepEpochSec",unix_timestamp(from_utc_timestamp(("actualRunwayDepartureUtc"),'UTC+10')))\
                    .withColumn("actualRunwayArrEpochSec",unix_timestamp(from_utc_timestamp(("actualRunwayArrivalUtc"),'UTC+10'))) 
                except Exception as err:
                    print("AnalysisException on {}/{}/{}: actualRunway time is not available"\
                          .format(2010+yr,mth+1,day+1))
                else:
                    # select data columns for a new Spark dataframe
                    Df6 = Df5.select("flightId"\
                        ,"carrierFsCode"\
                        ,"flightNumber"\
                        ,"departureAirportFsCode"\
                        ,"arrivalAirportFsCode"\
                        ,"departureDateLocalMonth"\
                        ,"departureDateLocalYear"\
                        ,"arrivalDateLocalMonth"\
                        ,"arrivalDateLocalYear"\
                        ,"publishedDepartureEpochSec"\
                        ,"publishedArrivalEpochSec"\
                        ,"actualRunwayDepEpochSec"\
                        ,"actualRunwayArrEpochSec")
                    parseList = Df6.dropDuplicates(["flightId"])
                    parseList.coalesce(2).write \
                        .mode("append") \
                        .parquet("departureFlights.parquet") 
                    daycount += parseList.count()               
            mthcount += daycount
            print("Day: {}/{}/{} completed downloading {} records".format(2010+yr,mth+1,day+1,daycount))
            elapsed = time.time()-t0
            print("Time elapsed in seconds = {:2f}".format(elapsed))
        yrcount += mthcount
        print("Month: {}/{} completed downloading {} records".format(2010+yr,mth+1,mthcount))
        elapsed = time.time()-t0
        print("Time elapsed in seconds = {:2f}".format(elapsed)) 
    totalcount += yrcount
    print("Year: {} completed downloading {} records".format(2010+yr+1,yrcount))
    elapsed = time.time()-t0
    print("Time elapsed in seconds = {:2f}".format(elapsed)) 
print("Total completed downloading {} records".format(totalcount))
elapsed = time.time()-t0
print("Total time elapsed in seconds = {:2f}".format(elapsed))    

# finish SparkSession
spark.stop()