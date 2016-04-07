import pyspark as ps
from settings import *
import numpy as np


def greatest_lower_bound_idx(arr, x):
    """
    :param arr: Sorted
    :param x: a number greater than or equal to arr[0]
    :return: the index of the glb of x in arr
    """
    for i in range(len(arr)-1, -1, -1):
        if arr[i] <= x:
            return i

def clean_data(spark_context, data_file_name):
    """
    Cleans the data (not the labels) into a nice RDD that is ready to use for machine learning
    :param spark_context:
    :param data_file_name:
    :return:
    """

    # Loading from text file but we're assuming it actually exists on hadoop
    raw_data = spark_context.textFile('{0}/{1}/{2}'.format(HOME, DATA_DIR, data_file_name))

    zipped = raw_data.zipWithIndex()

    # Collect the indices of the breaks.  This is massively smaller than the full dataset and we can
    # use it as a broadcast variable.
    breaks = zipped.filter(lambda x: x[0] == '').collect()
    breaks_list = [0] + sorted([v for (k, v) in breaks])
    breaks_list_broadcast = spark_context.broadcast(np.array(breaks_list))

    def mapper1(x):
        """
        Find the smallest value greater than x[1] in breaks_list_broadcast.values()
        Output is: block_id, (row_id, row)
        """

        # Index of the first double line break after the given line
        i = greatest_lower_bound_idx(breaks_list_broadcast.value, x[1])
        return i, (x[1], x[0])

    # Now we remove the whitespace lines and merge the values into a single list
    combined = zipped.filter(lambda x: len(x[0]) > 0).map(mapper1).combineByKey(lambda value: [value],
                           lambda x, value: x + [value],
                           lambda x, y: x + y
                          )

    # The data format is now
    # key - index of the double line break indicating the end of the block
    # value - (index of the line, line)

    # We want this to eventually be:
    # key - index of the *block*
    # value - numpy array representing the block

    def mapper2(x):
        # Construct the numpy array of data
        key = x[0]
        # This sorts the list by line id, ensuring that the data are in order
        value = sorted(x[1])

        cols = len(value[0][1].strip().split(' '))
        rows = len(value)

        arr = np.empty((rows, cols))

        for i in range(len(value)):
            arr[i] = np.array(map(float, value[i][1].strip().split(' ')))

        # find the index of the block by looking it up in the broadcast array
        block_index = greatest_lower_bound_idx(breaks_list_broadcast.value, key)
        return block_index, arr

    cleanTrainData = combined.map(mapper2).sortByKey()
    return cleanTrainData