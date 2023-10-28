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

# Script generated for node customer curated
customercurated_node1698481470976 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://haipm6-test1/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1698481470976",
)

# Script generated for node Step trainer
Steptrainer_node1698481470421 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://haipm6-test1/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainer_node1698481470421",
)

# Script generated for node Join
Join_node1698481517433 = Join.apply(
    frame1=Steptrainer_node1698481470421,
    frame2=customercurated_node1698481470976,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1698481517433",
)

# Script generated for node Drop Fields
DropFields_node1698483763744 = DropFields.apply(
    frame=Join_node1698481517433,
    paths=[
        "`.serialNumber`",
        "`.customerName`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "`.shareWithPublicAsOfDate`",
        "`.birthDay`",
        "`.lastUpdateDate`",
        "`.registrationDate`",
        "email",
        "lastUpdateDate",
        "`.shareWithResearchAsOfDate`",
        "phone",
        "shareWithFriendsAsOfDate",
        "`.shareWithFriendsAsOfDate`",
    ],
    transformation_ctx="DropFields_node1698483763744",
)

# Script generated for node Amazon S3
AmazonS3_node1698481549728 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698483763744,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://haipm6-test1/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698481549728",
)

job.commit()
