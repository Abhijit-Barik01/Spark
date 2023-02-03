from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType,StringType,StructType
spark=SparkSession.builder.appName("demoApp").getOrCreate()
person_list=[("Berry","","Allen",1,"M"),
              ("Robert","Queen","",2,"M"),
             ("Tony","","stark",4,"F")
             ]
schema=StructType([\
        StructField("firstname",StringType(),True),\
        StructField("middlename",StringType(),True),\
        StructField("lastname",StringType(),True),\
        StructField("Age",IntegerType(),True),\
        StructField("Gender",StringType(),True)
        ])
df=spark.createDataFrame(data=person_list,schema=schema)
df.printSchema()
df.show(truncate=False)
df.write.csv("record.csv")




#sprak job submit command
#spark-submit --master yarn --deploy-mode cluster pysparkjob.py
