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

# Script generated for node Amazon S3 Bucket
AmazonS3Bucket_node1698115266737 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://hy-stedi-lake-house/customer/landing/customer-1691348231425.json"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3Bucket_node1698115266737",
)

# Script generated for node SQL Query
SqlQuery267 = """
select * from myDataSource
where sharewithresearchasofdate != 0
"""
SQLQuery_node1698290388469 = sparkSqlQuery(
    glueContext,
    query=SqlQuery267,
    mapping={"myDataSource": AmazonS3Bucket_node1698115266737},
    transformation_ctx="SQLQuery_node1698290388469",
)

# Script generated for node Amazon S3 Trusted Customer Zone
AmazonS3TrustedCustomerZone_node1698115482645 = glueContext.getSink(
    path="s3://hy-stedi-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3TrustedCustomerZone_node1698115482645",
)
AmazonS3TrustedCustomerZone_node1698115482645.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3TrustedCustomerZone_node1698115482645.setFormat("json")
AmazonS3TrustedCustomerZone_node1698115482645.writeFrame(SQLQuery_node1698290388469)
job.commit()
