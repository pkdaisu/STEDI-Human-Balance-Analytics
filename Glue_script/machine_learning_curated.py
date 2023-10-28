import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1698479579476 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://haipm6-test1/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1698479579476",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1698479581805 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://haipm6-test1/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1698479581805",
)

# Script generated for node Join
Join_node1698479631918 = Join.apply(
    frame1=step_trainer_trusted_node1698479579476,
    frame2=accelerometer_trusted_node1698479581805,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1698479631918",
)

# Script generated for node Drop Fields
DropFields_node1698485006089 = DropFields.apply(
    frame=Join_node1698479631918,
    paths=[
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "customerName",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "`.serialNumber`",
    ],
    transformation_ctx="DropFields_node1698485006089",
)

# Script generated for node Amazon S3
AmazonS3_node1698479751634 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698485006089,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://haipm6-test1/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698479751634",
)

job.commit()
