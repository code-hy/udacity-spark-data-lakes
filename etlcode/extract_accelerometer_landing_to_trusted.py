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
CustomerPrivacyFilter_node1698191442802 = Join.apply(
    frame1=AmazonS3CustomerTrusted_node1698191406512,
    frame2=AmazonS3AccelerometerLanding_node1698191392429,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyFilter_node1698191442802",
)

# Script generated for node Drop Fields
DropFields_node1698191658782 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1698191442802,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1698191658782",
)

# Script generated for node Amazon S3 Accelerometer Trusted
AmazonS3AccelerometerTrusted_node1698191694332 = glueContext.getSink(
    path="s3://hy-stedi-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3AccelerometerTrusted_node1698191694332",
)
AmazonS3AccelerometerTrusted_node1698191694332.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3AccelerometerTrusted_node1698191694332.setFormat("json")
AmazonS3AccelerometerTrusted_node1698191694332.writeFrame(DropFields_node1698191658782)
job.commit()
