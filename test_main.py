from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from main import get_max_common_neighbors_pairs


def are_dfs_equal(df1: DataFrame, df2: DataFrame):
    df1_as_list = df1.collect()
    df2_as_list = df2.collect()
    if len(df1_as_list) != len(df2_as_list):
        return False
    for i in range(len(df1_as_list)):
        if df1_as_list[i]['count'] != df2_as_list[i]['count']:
            return False
        elif df1_as_list[i]['pair'][0] != df2_as_list[i]['pair'][0] or df1_as_list[i]['pair'][1] != df2_as_list[i]['pair'][1]:
            return False
    return True


def test_basic_success():
    dataset_path = "datasets/dataset02"
    spark = SparkSession.builder.getOrCreate()
    raw_df = spark.read.format("csv").option("header", "true").load(dataset_path)

    expected_df = spark.createDataFrame([{"count": 2, "pair": ["101", "105"]}])
    result_df = get_max_common_neighbors_pairs(raw_df, 1)
    assert are_dfs_equal(expected_df, result_df)

    expected_df = spark.createDataFrame([{"count": 2, "pair": ["101", "105"]}, {"count": 1, "pair": ["105", "107"]}])
    result_df = get_max_common_neighbors_pairs(raw_df, 2)
    assert are_dfs_equal(expected_df, result_df)


def test_zero_pairs():
    dataset_path = "datasets/dataset01"
    spark = SparkSession.builder.getOrCreate()
    raw_df = spark.read.format("csv").option("header", "true").load(dataset_path)
    result_df = get_max_common_neighbors_pairs(raw_df, 0)
    assert result_df.count() == 0


def test_folder_success():
    dataset_path = "datasets/dataset01"
    spark = SparkSession.builder.getOrCreate()
    raw_df = spark.read.format("csv").option("header", "true").load(dataset_path)

    expected_df = spark.createDataFrame([{"count": 4, "pair": ["101", "107"]}])
    result_df = get_max_common_neighbors_pairs(raw_df, 1)
    assert are_dfs_equal(expected_df, result_df)

