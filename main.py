from pyspark.sql import SparkSession
from pyspark.sql.functions import *

FOLDER_PATH_ARGUMENT_INDEX = 1
NUMBER_OF_PAIRS_ARGUMENT_INDEX = 2
NUMBER_OF_COMMAND_LINE_ARGUMENTS = 3


def main() -> int:
    if len(sys.argv) < NUMBER_OF_COMMAND_LINE_ARGUMENTS:
        print("usage: python3 main.py [folder_path] [number_of_pairs]")
        return 1

    folder_path = sys.argv[FOLDER_PATH_ARGUMENT_INDEX]
    try:
        number_of_pairs = int(sys.argv[NUMBER_OF_PAIRS_ARGUMENT_INDEX])
    except ValueError:
        print("usage: python3 main.py [folder_path] [number_of_pairs]")
        return 1

    spark = SparkSession.builder.getOrCreate()

    raw_df = spark.read.format("csv").option("header", "true").load(folder_path)
    max_neighbours_df = get_max_common_neighbors_pairs(raw_df, number_of_pairs)
    print_result_df(max_neighbours_df)
    return 0


def print_result_df(df: DataFrame):
    for row in df.collect():
        print(f"node1 = {row['pair'][0]}, node2 = {row['pair'][1]}, common = {row['count']}")


def get_max_common_neighbors_pairs(df: DataFrame, number_of_pairs: int) -> DataFrame:
    df_a = df.select(col('src').alias("node_a"), col('dst').alias('dst_a'))
    df_b = df.select(col('src').alias("node_b"), col('dst').alias('dst_b'))

    df = df_a.join(df_b, (col("dst_a") == col("dst_b")) & (col("node_a") != col("node_b"))).\
        groupBy('node_a', 'node_b').count()
    df = df.withColumn("pair", array_sort(array(col("node_a"), col("node_b")))).\
        drop("node_a", "node_b").dropDuplicates().orderBy(col('count').desc())

    result = df.limit(number_of_pairs)
    return result


def get_max_common_neighbors_pairs_undirected(df: DataFrame, number_of_pairs: int) -> DataFrame:
    df_rev = df.select(col('dst').alias("src"), col('src').alias('dst'))
    df = df.union(df_rev).dropDuplicates()
    return get_max_common_neighbors_pairs(df, number_of_pairs)


if __name__ == '__main__':
    exit_code = main()
