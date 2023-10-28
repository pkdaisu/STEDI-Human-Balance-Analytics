import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelemeter Trusted
AccelemeterTrusted_node1698474673451 = glueContext.create_dynamic_frame.from_catalog(
    database="haipm6",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelemeterTrusted_node1698474673451",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1698474672920 = glueContext.create_dynamic_frame.from_catalog(
    database="haipm6",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1698474672920",
)

# Script generated for node Join
Join_node1698474769691 = Join.apply(
    frame1=CustomerTrusted_node1698474672920,
    frame2=AccelemeterTrusted_node1698474673451,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1698474769691",
)

# Script generated for node Drop Fields
DropFields_node1698474798133 = DropFields.apply(
    frame=Join_node1698474769691,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1698474798133",
)

# Script generated for node SQL Query
SqlQuery0 = """
select distinct * from myDataSource

"""
SQLQuery_node1698476704176 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": DropFields_node1698474798133},
    transformation_ctx="SQLQuery_node1698476704176",
)

# Script generated for node Amazon S3
AmazonS3_node1698474810615 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1698476704176,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://haipm6-test1/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698474810615",
)

job.commit()
