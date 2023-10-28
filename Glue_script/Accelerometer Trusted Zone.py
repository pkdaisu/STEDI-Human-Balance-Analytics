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

# Script generated for node Amazon S3
AmazonS3_node1698471360630 = glueContext.create_dynamic_frame.from_catalog(
    database="haipm6",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1698471360630",
)

# Script generated for node Amazon S3
AmazonS3_node1698471358608 = glueContext.create_dynamic_frame.from_catalog(
    database="haipm6",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1698471358608",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1698471391884 = Join.apply(
    frame1=AmazonS3_node1698471358608,
    frame2=AmazonS3_node1698471360630,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyJoin_node1698471391884",
)

# Script generated for node Drop Fields
DropFields_node1698471491057 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1698471391884,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "serialnumber",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1698471491057",
)

# Script generated for node Amazon S3
AmazonS3_node1698471470303 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698471491057,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://haipm6-test1/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698471470303",
)

job.commit()
