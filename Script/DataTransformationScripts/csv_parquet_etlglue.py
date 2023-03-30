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

# Script generated for node userCompanies
userCompanies_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://org-sample-data/user_companies_csv/"],
        "recurse": True,
    },
    transformation_ctx="userCompanies_node1",
)

# Script generated for node userLoginLocations
userLoginLocations_node1680082335422 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://org-sample-data/user_login_locations_csv/"],
        "recurse": True,
    },
    transformation_ctx="userLoginLocations_node1680082335422",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1680084834281 = ApplyMapping.apply(
    frame=userLoginLocations_node1680082335422,
    mappings=[
        ("ID", "string", "`(right) ID`", "string"),
        ("USER_ID", "string", "`(right) USER_ID`", "string"),
        ("COUNTRY", "string", "`(right) COUNTRY`", "string"),
        ("CITY", "string", "`(right) CITY`", "string"),
        ("ZIP_CODE", "string", "`(right) ZIP_CODE`", "string"),
        ("LONGITUDE", "string", "`(right) LONGITUDE`", "string"),
        ("LATITUDE", "string", "`(right) LATITUDE`", "string"),
        ("IP", "string", "`(right) IP`", "string"),
        ("CREATED_AT", "string", "`(right) CREATED_AT`", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1680084834281",
)

# Script generated for node Join
Join_node1680082797766 = Join.apply(
    frame1=userCompanies_node1,
    frame2=RenamedkeysforJoin_node1680084834281,
    keys1=["USER_ID"],
    keys2=["`(right) ID`"],
    transformation_ctx="Join_node1680082797766",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1680082797766,
    mappings=[
        ("ID", "string", "ID", "string"),
        ("USER_ID", "string", "USER_ID", "string"),
        ("COMPANY_ID", "string", "COMPANY_ID", "string"),
        ("ROLE", "string", "ROLE", "string"),
        ("IS_PRIMARY", "string", "IS_PRIMARY", "string"),
        ("HAS_AGREED_WITH_TERMS", "string", "HAS_AGREED_WITH_TERMS", "string"),
        ("HAS_AGREED_WITH_TERMS_AT", "string", "HAS_AGREED_WITH_TERMS_AT", "string"),
        (
            "USER_ACTIVATION_LAST_NOTIFIED_AT",
            "string",
            "USER_ACTIVATION_LAST_NOTIFIED_AT",
            "string",
        ),
        ("`(right) ID`", "string", "`(right) ID`", "string"),
        ("`(right) USER_ID`", "string", "`(right) USER_ID`", "string"),
        ("`(right) COUNTRY`", "string", "`(right) COUNTRY`", "string"),
        ("`(right) CITY`", "string", "`(right) CITY`", "string"),
        ("`(right) ZIP_CODE`", "string", "`(right) ZIP_CODE`", "string"),
        ("`(right) LONGITUDE`", "string", "`(right) LONGITUDE`", "string"),
        ("`(right) LATITUDE`", "string", "`(right) LATITUDE`", "string"),
        ("`(right) IP`", "string", "`(right) IP`", "string"),
        ("`(right) CREATED_AT`", "string", "`(right) CREATED_AT`", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://org-output-data/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
