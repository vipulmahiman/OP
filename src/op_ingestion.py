import logging
import sys
import datetime
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, array, when, array_remove
from pyspark.sql.utils import AnalysisException
from cerberus import Validator

# Create Logger
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
    level="INFO"
)
logger = logging.getLogger(__name__)
# Create Spark session
spark = SparkSession.builder.master("local").appName("OP-Test").getOrCreate()
sc = spark.sparkContext
# Column list
col_list = ["id", "activation_date", "active", "owner_id", "billing_date", "billing_tax_number", "company_name",
            "company_email", "contact_name", "is_billing"]

def calculate_delta(df_prev, df_curr, ingestion_date):
    '''
    :param df_prev: Dataframe of previous date file
    :param df_curr: Dataframe of current date file
    :param ingestion_date: Ingestion Date to identify the date of delta
    :return: dataframe of delta records
    '''

    if ingestion_date is None:
        ingestion_date = datetime.datetime.now()
    # get data which is in the current dataset but not in previous
    df_insert = df_curr.join(df_prev, "id", "leftanti")
    # get data which is in the previous dataset but not in current
    df_delete = df_prev.join(df_curr, "id", "leftanti")
    # get conditions for all columns except id
    conditions = [when(col("d1." + c) != col("d2." + c), F.lit(c)).otherwise("col_matched") for c in col_list if
                  c != 'id']
    # get select column list and column_names for modified column list
    select_expr = [col("d2.id"), *[col("d2." + c) for c in col_list if c != 'id'],
                   array_remove(array(*conditions), "col_matched").alias("column_names")
                   ]
    df_modified = df_prev.alias("d1").join(df_curr.alias("d2"), "id").select(*select_expr)
    df_modified = df_modified.filter(F.size(col("column_names")) > 0)
    df_insert = df_insert.withColumn("column_names", F.array(F.lit(""))).withColumn("row_status", F.lit("I")).withColumn("ingestion_date", F.lit(ingestion_date))
    df_delete = df_delete.withColumn("column_names", F.array(F.lit(""))).withColumn("row_status", F.lit("D")).withColumn("ingestion_date",F.lit(ingestion_date))
    df_modified = df_modified.withColumn("row_status", F.lit("M")).withColumn("ingestion_date",F.lit(ingestion_date))

    logger.info("Printing Inserted Record")
    df_insert.show(truncate=False)
    logger.info("Printing Deleted Record")
    df_delete.show(truncate=False)
    logger.info("Printing Modified Record")
    df_modified.show(truncate=False)
    #combine all the datasets to get the delta
    dfs = [df_insert, df_modified, df_delete]
    df = reduce(DataFrame.unionAll, dfs)
    return df


def apply_delta(df_prev, df_delta):
    '''
    :param df_prev: Dataframe of previous date file
    :param df_delta: Dataframe of delta between current date and previous date files
    :return: df_current: Current date data to be reconciled with current date file.
    '''

    #get data from delta which is either modified or is new in delta
    df_modified = df_delta.select(col_list).where(df_delta.row_status.isin("I", "M")).select(col_list)
    # get data from previous day which was not modified and hence not captured in delta.
    df_unmodified = df_prev.join(df_delta, "id", "leftanti").select(col_list)
    dfs = [df_modified, df_unmodified]
    df_current = reduce(DataFrame.unionAll, dfs)
    return df_current


def validate_file(df):
    '''
    :param df: dataframe of file data
    :return: error_list to report as validation errors.

    Schema rules:
    1. Active and is_billing are boolean format and have allowable integer values of 0 and 1
    2. Billing_tax_number follows a set format Indonesian Tax Number (TFN) Format XX.XXX.XXX.X-XXX.XXX
    3. Company_email should be validated for email id format.
    '''

    schema = {'id': {'type': 'integer'},
              'activation_date': {'type': ['datetime', 'date']},  # Flexibility of datetime field in the file.
              'active': {'type': 'integer', 'min': 0, 'max': 1},
              'owner_id': {'type': 'integer'},
              'billing_date': {'type': 'date'},
              'billing_tax_number': {'type': 'string',
                                     "regex": "^[0-9]{2}.[0-9]{3}.[0-9]{3}.[0-9]{1}-[0-9]{3}.[0-9]{3}$"},
              'company_name': {'type': 'string'},
              'company_email': {'type': 'string', "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"},
              'contact_name': {'type': 'string'},
              'is_billing': {'type': 'integer', 'min': 0, 'max': 1}}

    v = Validator(schema)
    # Convert the dataframe into dictionary
    dict_df = df.toPandas().to_dict(orient='records')
    # Collect and report validation errors:
    error_list = []
    for doc in dict_df:
        if not (v.validate(doc)):
            error_desc = f"{doc['id']}: {v.errors}"
            error_list.append(error_desc)
    return error_list


def file_loader(file_path):
    # Validate if file has valid schema and contain all the columns
    logger.info(f"Loading File from {file_path}")
    try:
        df = spark.read \
            .parquet(file_path) \
            .select(col_list)
    except AnalysisException:
        logger.info("Input file does not contain required columns, Aborting Process")
        raise
    except:
        logger.info(f"Unexpected error: {sys.exc_info()[0]}")
        raise
    logger.info(f"File {file_path} record count: {df.count()}")
    # Convert activation date column to date (input can be timestamp or date)
    df = df.withColumn("activation_date",  df.activation_date.cast('date'))
    return df


if __name__ == '__main__':
    # prev_day_file_path = "C:\\Users\\vipul\\Downloads\\2021-10-25-dump.parquet"
    # curr_day_file_path = "C:\\Users\\vipul\\Downloads\\2021-10-26-dump.parquet"

    prev_day_file_path = "..\\OP-TEST\\files\\2021-10-25-dump.parquet"
    curr_day_file_path = "..\\OP-TEST\\files\\\\2021-10-26-dump.parquet"

    df_prev = file_loader(prev_day_file_path)
    # Validate only if a flag passed as this is already validated in previous run.
    prev_err_list = validate_file(df_prev)

    df_curr = file_loader(curr_day_file_path)
    curr_err_list = validate_file(df_curr)

    df_delta = calculate_delta(df_prev, df_curr, datetime.datetime.now())
    logger.info(f"delta count: {df_delta.count()}")
    df_delta.show(truncate=False)

    df_new = apply_delta(df_prev, df_delta)

    df_new_sorted = df_new.orderBy('id')
    df_curr_sorted = df_curr.orderBy('id')

    logger.info(f"df_new sorted count: {df_new_sorted.count()}")
    df_new_sorted.printSchema()
    df_new_sorted.show(100, truncate=False)

    logger.info(f"df_curr sorted count: {df_curr_sorted.count()}")
    df_curr_sorted.printSchema()
    df_curr_sorted.show(100, truncate=False)

    df3 = df_new_sorted.union(df_curr_sorted).orderBy('id')
    logger.info(f"df3 count: {df3.count()}")
    df3.show(100, truncate=False)

    if( df_new_sorted.union(df_curr_sorted).count() == df_new_sorted.count()) :
        # df_new_sorted == df_curr_sorted:
        logger.info("Reconciled post applying the delta to previous day file")
    else:
        logger.info("Delta did not reconcile.")
