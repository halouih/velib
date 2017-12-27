from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

### Initialisation ###
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 5)
ssc.checkpoint("./checkpoint")



### Question 1 ###

# data recueille les données
data = ssc.socketTextStream("velib.behmo.com", 9999)

# utils recueille les données utiles que nous utiliserons pour le reste de l'application
utils = data.map(lambda theta: json.loads(theta))\
    .map(lambda theta:(theta['contract_name'] + ' ' + theta['name'], theta['available_bikes']))\




### Question 2 ###

# Utilisation de la fonction filter pour selectionner toutes les 5 secondes les stations qui n'ont plus de vélos
utils_vides = utils.filter(lambda theta: theta[1]==0)\
    .pprint() 




### Question 3 ###

# Every 5s print the Velib stations that have became empty


def updateNewEmptyStation(newValues, tracker):
    if not newValues:
        tracker = 1
    else:
        if tracker is None:
            tracker = 1
        elif newValues[0] == 0 and tracker ==1:
            tracker = 0
        elif newValues[0] == 0 and tracker == 0:
            tracker = 1
        else:
            pass
    return tracker

tracker_empty_stations = utils.reduceByKey(lambda x, y: x).updateStateByKey(updateNewEmptyStation)
tracker_empty_stations.filter(lambda new_emp_station: new_emp_station[1] == 0).pprint()




### Question 4 ###

# Every 1 min: print the stations that were most active during the last 5 min (activity = number of bikes borrowed and returned)
# on fait la somme des valeurs absolues de la diff de bike available entre deux fenetre

    utils
    .reduceByKeyAndWindow(lambda c1, c2: abs(c2 - c1),None , 350, 60)\
    .transform(lambda rdd: rdd.sortBy(lambda wc: -wc[1]))\
    .foreachRDD(lambda rdd: print(rdd.take(10)))\




ssc.start()
ssc.awaitTermination()

