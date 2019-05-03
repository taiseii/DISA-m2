'''/
Created on Mar 15, 2019

@author: student
'''


import pyspark
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.master("local").appName("test1").config("spark.some.config.option", "some-value").getOrCreate()

df_Degrees = spark.read.csv("/home/student/tables/Degrees.table", header=True)
df_Degrees = df_Degrees.withColumnRenamed("Degrees 8000:AUTOINCREASE DegreeId", "DegreeId").withColumnRenamed("VARCHAR Dept", "Dept").withColumnRenamed("VARCHARLONG Description", "Description").withColumnRenamed("INT TotalECTS", "TotalECTS")

df_Students = spark.read.csv("/home/student/tables/Students.table", header=True)
df_Students = df_Students.withColumnRenamed("Students 4000000:AUTOINCREASE StudentId", "StudentId").withColumnRenamed("VARCHAR StudentName", "StudentName").withColumnRenamed("VARCHARLONG Address", "Address").withColumnRenamed("INT BirthYearStudent", "BirthYearStudent").withColumnRenamed("GENDER Gender", "Gender")

df_StudentRegistrationsToDegrees = spark.read.csv("/home/student/tables/Degrees.table", header=True)
df_StudentRegistrationsToDegrees = df_StudentRegistrationsToDegrees.withColumnRenamed("Degrees 8000:AUTOINCREASE DegreeId","DegreeId").withColumnRenamed("VARCHAR Dept", "Dept").withColumnRenamed("VARCHARLONG Description", "Description").withColumnRenamed("INT TotalECTS", "TotalECTS")

df_Teachers = spark.read.csv("/home/student/tables/Teachers.table", header=True)
df_Teachers = df_Teachers.withColumnRenamed("Teachers 40000:AUTOINCREASE TeacherId", "TeacherId").withColumnRenamed("VARCHAR TeacherName", "TeacherName").withColumnRenamed("VARCHARLONG Address", "Address").withColumnRenamed("INT BirthyearTeacher", "BirthyearTeacher").withColumnRenamed("GENDER Gender","Gender")


df_Courses = spark.read.csv("/home/student/tables/Courses.table",header=True)
df_Courses = df_Courses.withColumnRenamed("Courses 40000:AUTOINCREASE CourseId", "CourseId").withColumnRenamed("VARCHAR CourseName", "CourseName").withColumnRenamed("VARCHARLONG CourseDescription", "CourseDescription").withColumnRenamed("INT DegreeId", "DegreeId").withColumnRenamed("INT ECTS", "ECTS")


df_CourseOffers = spark.read.csv("/home/student/tables/CourseOffers.table", header=True)
df_CourseOffers = df_CourseOffers.withColumnRenamed("CourseOffers 400000:AUTOINCREASE CourseOfferId", "CourseOfferId").withColumnRenamed("INT CourseId", "CourseId").withColumnRenamed("INT Year", "Year").withColumnRenamed("INT Quartile", "Quartile")

df_CourseRegistrations = spark.read.csv("/home/student/tables/CourseRegistrations.table", header=True, nullValue='null')
df_CourseRegistrations = df_CourseRegistrations.withColumnRenamed("CourseRegistrations 80000000:INT CourseOfferId", "CourseOfferId").withColumnRenamed("INT StudentRegistrationId","StudentRegistrationId").withColumnRenamed("INT StudentRegistrationId", "StudentRegistrationId").withColumnRenamed("GRADE Grade", "Grade")
df_CourseRegistrations = df_CourseRegistrations.withColumn('Grade', df_CourseRegistrations['Grade'].cast(IntegerType()))

df_QuantilePoints = spark.read.csv("/home/student/tables/QuantilePoints.table", header=True)

q11 = df_CourseOffers.filter('(quartile = 2) and (year = 2016)').join(df_CourseRegistrations.filter('Grade >= 5'), df_CourseRegistrations.CourseOfferId  == df_CourseOffers.CourseOfferId).drop(df_CourseRegistrations.CourseOfferId).select('CourseOfferId', 'Grade').groupBy('CourseOfferId').avg('Grade')
q11.show()
q12 = df_CourseRegistrations.filter('Grade >= 5 and StudentRegistrationId = 3').groupby().avg('Grade')
q12.show()


## SECOND EXERCISE:
# 1 load rdds - Courses and CourseOffers 
# 2 determine k --> (k, v)
# 3 new rdd u = Courses.union(CourseOffers)
# 4 reduceByKey on new rdd and execute the join locally at each reducer

# THIRD EXERCISE:
# CHALLENGE: WHAT IS THE KEY, but generally the same thing as 2
# Need to create join matrix and use theta join