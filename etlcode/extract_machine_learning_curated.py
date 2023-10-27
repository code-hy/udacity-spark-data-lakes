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

# Script generated for node AWS Glue Data Catalog accelerometer trusted
AWSGlueDataCatalogaccelerometertrusted_node1698304879702 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AWSGlueDataCatalogaccelerometertrusted_node1698304879702",
    )
)

# Script generated for node AWS Glue Data Catalog step trainer trusted
AWSGlueDataCatalogsteptrainertrusted_node1698376423281 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_trusted",
        transformation_ctx="AWSGlueDataCatalogsteptrainertrusted_node1698376423281",
    )
)

# Script generated for node SQL Query
SqlQuery304 = """
select * from accelerometer_trusted at, step_trainer_trusted stt 
     where at.timestamp = stt.sensorreadingtime;
"""
SQLQuery_node1698304909350 = sparkSqlQuery(
    glueContext,
    query=SqlQuery304,
    mapping={
        "accelerometer_trusted": AWSGlueDataCatalogaccelerometertrusted_node1698304879702,
        "step_trainer_trusted": AWSGlueDataCatalogsteptrainertrusted_node1698376423281,
    },
    transformation_ctx="SQLQuery_node1698304909350",
)

# Script generated for node AWS Glue Data Catalog Machine Learning Curated
AWSGlueDataCatalogMachineLearningCurated_node1698305711213 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=SQLQuery_node1698304909350,
        database="stedi",
        table_name="machine_learning_curated",
        additional_options={
            "enableUpdateCatalog": True,
            "updateBehavior": "UPDATE_IN_DATABASE",
        },
        transformation_ctx="AWSGlueDataCatalogMachineLearningCurated_node1698305711213",
    )
)

job.commit()
