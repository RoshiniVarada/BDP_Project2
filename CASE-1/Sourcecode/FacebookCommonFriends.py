import os

os.environ["SPARK_HOME"] = "D:/UMKC/PB/Project/spark-2.4.5-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "D:/UMKC/PB/Project/spark-2.4.5-bin-hadoop2.7"

from pyspark import SparkContext

def map(value):
    print(value)
    #the person in the first array part will be the user
    user = value[0]
    #the list of all the friends will be saved in the friends array
    friends = value[1]
    print(1)
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
    lines = sc.textFile("facebook_combined.txt", 1)

    #creates a (key, value) pairs
    pairs = lines.map(lambda x: (x.split(" ")[0], x.split(" ")[1]))

    #groups by key to produce key and list of values
    pair = pairs.groupByKey().map(lambda x : (x[0], list(x[1])))

    #runs mapper
    line = pair.flatMap(map)

    #reduced by key
    commonFriends = line.reduceByKey(reduce)
    pair.coalesce(1).saveAsTextFile("pairoutofmap")
    line.coalesce(1).saveAsTextFile("pairoutofflatmap")
    commonFriends.coalesce(1).saveAsTextFile("commonFriendsOutput")
