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
    number_of_pairs = sys.argv[NUMBER_OF_PAIRS_ARGUMENT_INDEX]

    spark = SparkSession.builder.getOrCreate()

    raw_df = spark.read.format("csv").option("header", "true").load(folder_path)
    max_neighbours_df = get_max_common_neighbors_pairs(raw_df, number_of_pairs)

    print(max_neighbours_df)
    return 0


def get_max_common_neighbors_pairs(df, number_of_pairs) -> DataFrame:
    pass


if __name__ == '__main__':
    exit_code = main()
