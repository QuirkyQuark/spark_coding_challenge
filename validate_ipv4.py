import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

parser = argparse.ArgumentParser(description="Validate given ip addresses in a spark DataFrame and return columns that have valid ipv4 addresses")
parser.add_argument('--input_file', help="name of csv file that contains the ipv4 data to validate.", default="input.txt")
parser.add_argument("--input_file_has_header", type=bool, help="Whether the input file has a header", default=True)
parser.add_argument("--output_file", help="name of csv file that contains the validated DataFrame", default="output.txt")

args = parser.parse_args()

spark = SparkSession.builder.appName("Validate_ipv4").getOrCreate()

df = spark.read.csv(args.input_file, header=args.input_file_has_header)

regex='^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\.(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$|^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\:(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$|^(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\-(0*([0-9]|[1-8][0-9]|9[0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))$'
total_row_count = df.count()
columns_to_drop = []

for column in df.columns:
    row_count = df.agg(F.sum(F.col(column).rlike(regex).cast('integer')).alias(column)).collect()[0][column]
    if row_count != total_row_count:
        columns_to_drop.append(column)

df = df.drop(*columns_to_drop)
df.toPandas().to_csv(args.output_file, index=False)
df.show()
