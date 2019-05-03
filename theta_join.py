import pyspark

from pyspark.sql import SparkSession, Row, functions as F
from pyspark import SparkContext 
from pyspark.sql.types import IntegerType
from random import sample
from math import floor

sc = SparkContext("local", "q3 app")

spark = SparkSession.builder.master("local").appName("test1").config("spark.some.config.option", "some-value").getOrCreate()

df_QuantilePoints = spark.read.csv("/mnt/c/users/taiseii/Documents/data intensive systems/ms2/tables/small/QuantilePoints.table", header=True)

df_CourseRegistrations = spark.read.csv("/mnt/c/users/taiseii/Documents/data intensive systems/ms2/tables/small/CourseRegistrations.table", header=True, nullValue='null')
df_CourseRegistrations = df_CourseRegistrations.withColumnRenamed("CourseRegistrations 80000000:INT CourseOfferId", "CourseOfferId").withColumnRenamed("INT StudentRegistrationId","StudentRegistrationId").withColumnRenamed("INT StudentRegistrationId", "StudentRegistrationId").withColumnRenamed("GRADE Grade", "Grade")
df_CourseRegistrations = df_CourseRegistrations.withColumn('Grade', df_CourseRegistrations['Grade'].cast(IntegerType()))

#loading rdd
QuantilePoints_rdd = df_QuantilePoints.rdd
CourseRegistrations_rdd = df_CourseRegistrations.rdd

#map 
QuantilePoints_rdd = QuantilePoints_rdd.map(lambda x: (x,'q'))
CourseRegistrations_rdd = CourseRegistrations_rdd.map(lambda x: (x,'c'))
#filtering
CourseRegistrations_rdd = CourseRegistrations_rdd.filter(lambda x: isinstance(x[0][2], int)  )


range_Q = QuantilePoints_rdd.count()
sequence_Q = [i for i in range(range_Q)]

range_C = CourseRegistrations_rdd.count()
sequence_C = [i for i in range(range_C)]

rdd = QuantilePoints_rdd.union(CourseRegistrations_rdd)

def getRegionsR(m):
    key = []
    if m <= floor(range_Q/2):
        key.append(1)
        key.append(3)
        key.append(5)
    else:
        key.append(2)
        key.append(4)
        key.append(6)
    return key

def getRegionsC(m):
    key = []
    if m <= floor(range_C/3):
        key.append(1)
        key.append(2)
    elif m >= floor(range_C/3) and m <= 2*floor(range_C/3):
        key.append(3)
        key.append(4)
    else:
        key.append(5)
        key.append(6)
    return key


# random assignment to ensure the each pair is uniformly distributed across joint matrix
# map location(x,y) and (1,0) for (student.grade <= quantile)
# partition arbitraly since it is uniformly assigned

def mapper(x):
    mapped = []
    keys = []
    try:
        what = x[len(x)-1][0]
        if what == 'q' :
            matrixR = sample(sequence_Q, 1)
            keys = getRegionsR(matrixR[0])
            # print(matrixR)
            # print(keys)
        else :
            matrixC = sample(sequence_C, 1)
            keys = getRegionsC(matrixC[0])
            # print(matrixC)
            # print(keys)
    except:
        print('error')
    for i in keys:
        mapped.append(x + (i,))
    return mapped

rdd = rdd.map(mapper)
rdd = rdd.flatMap(lambda a:a)

#Assigned region as key and reduce by key
rdd = rdd.keyBy(lambda x: x[len(x)-1])
rdd = rdd.reduceByKey(lambda a,b : a + b)

def reducing(x):
    result = []
    q = []
    c = []
    rangeT = len(x)-1
    counter = 0
    for i in x[rangeT]:
        counter = counter + 1
        if i == 'q':
            q.append(x[rangeT][counter-2])
        elif i == 'c':
            c.append(x[rangeT][counter-2])
    #join on condition( student.grade <= quantile)
    for qq in q:
        for cc in c:
            if int(cc[2]) <= int(qq[0]):
                result.append(cc + qq)
    # print(result)
    return result

rdd = rdd.flatMap(reducing)

                                
#           -- TESTING AREA HERE --
                                  
print( rdd.take(200) )




