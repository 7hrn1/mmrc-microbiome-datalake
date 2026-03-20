from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import sys
import logging
import boto3
import re
import json
from pyspark.context import SparkContext
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyspark.sql.functions as F 
from datetime import datetime

# Initialize Glue and Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job_params = getResolvedOptions(sys.argv,["file_name", "full_file_path"])

# Define source bucket and get params passed from lambda
bucket_name = "mmrc-participant-metadata"
input_path = job_params['full_file_path']
file_name = job_params["file_name"]

#Configure table details
catalog_name = "glue_catalog"
database_name = "mmrc_microbiome"
table_name = "mmrc_participant_metadata"
full_table_name = f"""{catalog_name}.{database_name}.{table_name}"""

#Define logger 
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

def main():
    """
    Main function that executes for processing and transforming csv file to be strored as Iceberg Table
    """
    
    try:
        logger.info(f'********JOB START********')
       
        #Specify an input s3 filepath
        input_filepath = f"s3://{bucket_name}/{input_path}"
        #Read csv file into Glue DynamicFrame
        par_meta_dyf = glueContext.create_dynamic_frame.from_options(
            format_options={
                "withHeader": True
            },
            connection_type="s3",
            format="csv",
            connection_options={"paths": [input_filepath]}
        )
        # Convert DynamicFrame to Spark DataFrame
        par_meta_df = par_meta_dyf.toDF()
        
        #Extract 4-digit sample number and standardise format to be SampleXXXX
        sample_id_col_header = par_meta_df.schema[0].name
        cleaned_sample_id_df = par_meta_df.withColumn(
            "Sample_ID_Extracted", 
            F.concat(F.lit("sample"),F.regexp_extract(f"`{sample_id_col_header}`", r"(\d+\w?)", 1))  
        )
        #Remove old sample number column
        cleaned_sample_id_df = cleaned_sample_id_df.drop(sample_id_col_header)
        #Rename new sample number column
        cleaned_sample_id_df = cleaned_sample_id_df.withColumnRenamed("Sample_ID_Extracted","Sample_ID")

        #2. Change all column names to lowercase
        lowercased_cols = [c.lower() for c in cleaned_sample_id_df.columns]
        cleaned_cols_df = cleaned_sample_id_df.toDF(*lowercased_cols)
        
        #Rearrange such that sample id is in front
        cleaned_cols_df =  cleaned_cols_df.select("sample_id", *[f"`{c}`" for c in cleaned_cols_df.columns if c != "sample_id"])

        #Add source and batch date,month, year columns for partitioning
        cleaned_cols_df = cleaned_cols_df.withColumn("source_file", F.lit(f"{file_name}")) \
            .withColumn("batch_day", F.lit(datetime.now().strftime('%d'))) \
            .withColumn("batch_month", F.lit(datetime.now().strftime('%m'))) \
            .withColumn("batch_year", F.lit(datetime.now().strftime('%Y')))
        
        #Parallelising processing of sam and tax dataframes by calling iceberg_table_transform() func
        with ThreadPoolExecutor(max_workers=4) as executor:
            #Submit procesing of sam and tax tables to different threads for parallel processing 
            future_to_name = {
                executor.submit(iceberg_table_transform, full_table_name, cleaned_cols_df): "sam",
            }
            #Collect results after thread execution completed
            results = {}
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    result = future.result()
                    results[name] = result
                except Exception as e:
                    raise Exception(f"iceberg_table_transform failed for {name}: {str(e)}")
        
        #Check if any of the Iceberg transformations have failed and raise Exception if yes
        if not results.get("sam"):
            raise Exception("The Iceberg table transforms failed.")
        
        return
        
    except Exception as e:
        logger.error(e)
        raise e
    
def iceberg_table_transform(iceberg_table_name, df):
    """
    Helper function that aligns columns between any existing Iceberg Table schema and input dataframe's schema
    """
    try:
        #1. Read Iceberg Table from Glue Catalog and extract schema
        logger.info("Getting table schema")
        table_schema = spark.sql(f"DESCRIBE TABLE {iceberg_table_name}").select("col_name", "data_type").collect()
        
        # Build a map of existing columns and their types
        iceberg_columns = {row["col_name"].lower(): row["data_type"] for row in table_schema 
                        if not row["col_name"].startswith("#")}  # Filter out metadata rows
        
        logger.info(f"Iceberg table has {len(iceberg_columns)} columns")
        
        # Get columns in the new dataframe
        dataframe_columns = {col_name.lower(): type(df.schema[col_name].dataType).__name__
                        for col_name in df.columns}

        #Get list of columns in existing Iceberg table but not in input dataframe  
        missing_cols = set([c for c in iceberg_columns.keys()]) - set([c for c in dataframe_columns.keys()])
        #Get list of columns in input dataframe but not in existing Iceberg table
        new_cols = set([c for c in dataframe_columns.keys()]) - set([c for c in iceberg_columns.keys()])

        # Find columns and their data types that need to be added to the Iceberg table
        columns_to_add = {}
        for c in new_cols:
                # Convert Spark SQL type to Iceberg type
                if "double" in dataframe_columns[c].lower():
                    columns_to_add[c] = "DOUBLE"
                elif "int" in dataframe_columns[c].lower():
                    columns_to_add[c] = "INT"
                elif "string" in dataframe_columns[c].lower():
                    columns_to_add[c] = "STRING"
                elif "timestamp" in dataframe_columns[c].lower():
                    columns_to_add[c] = "TIMESTAMP"
                else:
                    columns_to_add[c] = "STRING"  # Default fallback
        
        logger.info(f"Found {len(columns_to_add)} new columns to add")
        
        # Add new columns to the table if needed
        if columns_to_add:
            # Add columns in batches to avoid excessive ALTER statements
            batch_size = 100
            column_batches = []
            current_batch = []
            
            for c in columns_to_add.keys():
                current_batch.append(f"`{c}` {columns_to_add[c]}")
                if len(current_batch) >= batch_size:
                    column_batches.append(current_batch)
                    current_batch = []
            
            if current_batch:  # Add any remaining columns
                column_batches.append(current_batch)
            
            # Execute ALTER TABLE to evolve Iceberg Table schema for each batch
            for batch in column_batches:
                alter_sql = f"ALTER TABLE {iceberg_table_name} ADD COLUMNS ({', '.join(batch)})"
                logger.info(f"Executing: {alter_sql}")
                spark.sql(alter_sql)
                spark.catalog.refreshTable(iceberg_table_name)
    
        #Find columns in Iceberg Table not in input dataframe and add them
        #Guard all existing columns inside input dataframe to allow for column names with special characters
        columns_expr = [wrap_expr(c) for c in df.columns]

        # Append the missing columns with defaults using lit() and proper casting.
        for c in missing_cols:
            col_type = iceberg_columns[c].lower()
            if "double" in col_type or "decimal" in col_type:
                new_expr = F.lit(0.0).cast("double")
            elif "int" in col_type:
                new_expr = F.lit(0).cast("int")
            elif "timestamp" in col_type:
                new_expr = F.lit(None).cast("timestamp")
            else:
                new_expr = F.lit(None).cast("string")
            
            # Convert the Column expression to its SQL representation.
            default_sql = new_expr._jc.toString()  
            
            # Build an SQL expression string for selectExpr with a safe alias.
            sql_expr = f"{default_sql} AS {wrap_expr(c)}"
            columns_expr.append(sql_expr)
        
        # Reassemble the DataFrame using selectExpr with all (existing + missing) columns.
        df = df.selectExpr(*columns_expr)
        
        #Refresh table once again before writing to S3
        spark.catalog.refreshTable(iceberg_table_name)
        
        logger.info("Attempting to write dataframe to S3")
        
        # Write aligned dataframe to Iceberg Table location in S3
        df.writeTo(iceberg_table_name).append()
        
        return True
    
    except Exception as e:
        logger.error(f"FATAL ERROR CAUSED BY {str(e)}")
        return False
        
def wrap_expr(col_name):
    """
    Aux function that wraps a string val in backticks to escape special characters to be used in SQL statements
    """
    return f"`{col_name}`"
    
if __name__ == "__main__":
    # Run the main function
    main()
