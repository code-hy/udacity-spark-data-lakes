import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3 Customer Trusted
AmazonS3CustomerTrusted_node1698191406512 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="customer_trusted",
        transformation_ctx="AmazonS3CustomerTrusted_node1698191406512",
    )
)

# Script generated for node Amazon S3 Accelerometer Landing
AmazonS3AccelerometerLanding_node1698191392429 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AmazonS3AccelerometerLanding_node1698191392429",
    )
)

# Script generated for node Customer Privacy Filter
AmazonS3CustomerTrusted_node1698191406512DF = (
    AmazonS3CustomerTrusted_node1698191406512.toDF()
)
AmazonS3AccelerometerLanding_node1698191392429DF = (
    AmazonS3AccelerometerLanding_node1698191392429.toDF()
)
CustomerPrivacyFilter_node1698191442802 = DynamicFrame.fromDF(
    AmazonS3CustomerTrusted_node1698191406512DF.join(
        AmazonS3AccelerometerLanding_node1698191392429DF,
        (
            AmazonS3CustomerTrusted_node1698191406512DF["email"]
            == AmazonS3AccelerometerLanding_node1698191392429DF["user"]
        ),
        "left",
    ),
    glueContext,
    "CustomerPrivacyFilter_node1698191442802",
)

# Script generated for node Drop Fields
DropFields_node1698191658782 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1698191442802,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1698191658782",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1698299282816 = DynamicFrame.fromDF(
    DropFields_node1698191658782.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1698299282816",
)

# Script generated for node Amazon s3 Customer Curated
Amazons3CustomerCurated_node1698191694332 = glueContext.getSink(
    path="s3://hy-stedi-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Amazons3CustomerCurated_node1698191694332",
)
Amazons3CustomerCurated_node1698191694332.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
Amazons3CustomerCurated_node1698191694332.setFormat("json")
Amazons3CustomerCurated_node1698191694332.writeFrame(DropDuplicates_node1698299282816)
job.commit()
