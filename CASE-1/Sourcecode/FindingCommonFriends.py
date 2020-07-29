import os

os.environ["SPARK_HOME"] = "D:/UMKC/PB/Project/spark-2.4.5-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "D:/UMKC/PB/Project/spark-2.4.5-bin-hadoop2.7"

from pyspark import SparkContext

def map(value):
    user = value[0]
    friends = value[1]
    keys = []

    for friend in friends:
        friendlist = friends[:]
        friendlist.remove(friend)
        key = sorted(user + friend)
        keylist = list(key)
        keylist.insert(len(user), '-')
        keys.append((''.join(keylist), friendlist))
    return keys


def reduce(key, value):
    reducer = []
    for friend in key:
        if friend in value:
            reducer.append(friend)
    return reducer


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    #reads the file
    lines = sc.textFile("input", 1)

    #creates a (key, value) paris
    pairs = lines.map(lambda x: (x.split(" ")[0], x.split(" ")[1]))

    #groups by key to produce key and list of values
    pair = pairs.groupByKey().map(lambda x : (x[0], list(x[1])))

    #runs mapper
    line = pair.flatMap(map)

    #reduced by key
    commonFriends = line.reduceByKey(reduce)
    commonFriends.coalesce(1).saveAsTextFile("commonFriendsOutput")
