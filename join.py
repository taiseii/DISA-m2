import pyspark

from pyspark.sql import SparkSession, Row, functions as F
from pyspark import SparkContext 
from pyspark.sql.types import IntegerType

sc = SparkContext("local", "q2 app")

spark = SparkSession.builder.master("local").appName("test1").config("spark.some.config.option", "some-value").getOrCreate()

df_CourseOffers = spark.read.csv("/mnt/c/users/taiseii/Documents/data intensive systems/ms2/tables/small/CourseOffers.table", header=True)
df_CourseOffers = df_CourseOffers.withColumnRenamed("CourseOffers 400000:AUTOINCREASE CourseOfferId", "CourseOfferId").withColumnRenamed("INT CourseId", "CourseId").withColumnRenamed("INT Year", "Year").withColumnRenamed("INT Quartile", "Quartile")

df_CourseRegistrations = spark.read.csv("/mnt/c/users/taiseii/Documents/data intensive systems/ms2/tables/small/CourseRegistrations.table", header=True, nullValue='null')
df_CourseRegistrations = df_CourseRegistrations.withColumnRenamed("CourseRegistrations 80000000:INT CourseOfferId", "CourseOfferId").withColumnRenamed("INT StudentRegistrationId","StudentRegistrationId").withColumnRenamed("INT StudentRegistrationId", "StudentRegistrationId").withColumnRenamed("GRADE Grade", "Grade")
df_CourseRegistrations = df_CourseRegistrations.withColumn('Grade', df_CourseRegistrations['Grade'].cast(IntegerType()))


#rdd-laod

CourseOffers_rdd = df_CourseOffers.rdd
CourseRegistrations_rdd = df_CourseRegistrations.rdd

#q2
CourseOffers_rdd = CourseOffers_rdd.map(lambda x: (x, 1))
CourseRegistrations_rdd = CourseRegistrations_rdd.map(lambda x: (x,0))

CourseOffers_rdd = CourseOffers_rdd.keyBy(lambda x: int(x[0][0]))
CourseRegistrations_rdd = CourseRegistrations_rdd.keyBy(lambda x: int(x[0][0]))

rdd = CourseOffers_rdd.union(CourseRegistrations_rdd)

rdd3 = rdd.reduceByKey(lambda a,b: a+b)

def merge(x):
    unit = 2
    co_cr = []
    co = []
    cr = []
    try:
        # print(x)
        # print(len(x[1]))
        contents = len(x[1])/unit
        if contents > 1:
            for i in range(contents):
                pos = 2*i+1
                what = x[1][pos]
                if what == 1:
                    co.append(x[1][pos-1])
                    # print(x[1][pos-1])
                else :
                    cr.append(x[1][pos-1])
                    # print(x[1][pos-1])
            for o in co:
                for r in cr:
                    co_cr.append(o+r)
            # print(co_cr)        
    except:
        print('error')

    return co_cr

rdd4 = rdd3.flatMap(merge)

print(rdd4.take(100))   




