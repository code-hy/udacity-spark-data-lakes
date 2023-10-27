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

# Script generated for node Amazon S3 step trainer landing
AmazonS3steptrainerlanding_node1698301556422 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://hy-stedi-lake-house/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="AmazonS3steptrainerlanding_node1698301556422",
    )
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1698301575779 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1698301575779",
)

# Script generated for node SQL Query
SqlQuery350 = """
select * from step_trainer_landing stl , customer_curated_new ccn 
    where stl.serialnumber = ccn.serialnumber;
"""
SQLQuery_node1698304193076 = sparkSqlQuery(
    glueContext,
    query=SqlQuery350,
    mapping={
        "step_trainer_landing": AmazonS3steptrainerlanding_node1698301556422,
        "customer_curated_new": AWSGlueDataCatalog_node1698301575779,
    },
    transformation_ctx="SQLQuery_node1698304193076",
)

# Script generated for node Amazon S3
AmazonS3_node1698304336514 = glueContext.getSink(
    path="s3://hy-stedi-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698304336514",
)
AmazonS3_node1698304336514.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1698304336514.setFormat("json")
AmazonS3_node1698304336514.writeFrame(SQLQuery_node1698304193076)
job.commit()
