import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import logging
from pyspark.sql.functions import isnull, count, when

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def log_errors(inner):
    def wrapper(*args, **kwargs):
        result, test = inner(*args, **kwargs)
        if result:
            logging.error("test failed: {}".format(test))
    return wrapper

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "gf-weather", table_name = "gf_test", transformation_ctx = "datasource0")


@log_errors
def test_null(frame, to_check):
    frame = frame.toDF()
    test_result = [x for x in to_check if frame.select(count(when(isnull(x), x))).collect()[0][0] > 0]
    return len(test_result) > 0, ", ".join(test_result) + "contain null values"

test_null_columms = ['region', 'screentemperature', 'observationdate']
#test if specifed columns contain any null values
test_null(datasource0, test_null_columms)

@log_errors
def check_above(frame, test_column, test_value):
	frame = frame.toDF()
	test_result = frame.agg({test_column: "max"}).collect()[0][0] > test_value
	return test_result, "{0} contains value(s) above {1}".format(test_column, test_value)

#check time is in hours range	
check_above(datasource0, "observationtime", 23)
#check direction doesn't exceed 20	
check_above(datasource0, "winddirection", 20)
#check weather codes in met off code range	
check_above(datasource0, "significantweathercode", 30)

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [
	("forecastsitecode", "long", "forecastsitecode", "long"),
	("observationtime", "long", "observationtime", "byte"), 
	("observationdate", "string", "observationdate", "date"),
	("winddirection", "long", "winddirection", "byte"), 
	("windspeed", "long", "windspeed", "short"), 
	("windgust", "long", "windgust", "short"), 
	("visibility", "long", "visibility", "short"), 
	("screentemperature", "double", "screentemperature", "double"), 
	("pressure", "long", "pressure", "short"), 
	("significantweathercode", "long", "significantweathercode", "byte"), 
	("sitename", "string", "sitename", "string"), 
	("latitude", "double", "latitude", "float"), 
	("longitude", "double", "longitude", "float"), 
	("region", "string", "region", "string"), 
	("country", "string", "country", "string")
	], transformation_ctx = "applymapping1")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://gf-parquet"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
