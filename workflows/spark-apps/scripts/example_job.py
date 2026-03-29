from argparse import ArgumentParser

from pyspark.sql import SparkSession


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--message", default="hello")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("example-job").getOrCreate()
    df = spark.createDataFrame([(args.message,)], ["message"])
    df.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
