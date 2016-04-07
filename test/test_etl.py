from __future__ import division, absolute_import
import unittest as unittest
import os
import pyspark as ps
import numpy as np

# This allows us to import test settings.
os.environ["TRUMOTION_TEST"] = "1"
from src.ETL_Pipeline import clean_data


class TestProblems(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        os.environ["TRUMOTION_TEST"] = "0"

    def test_env_setup(self):
        self.assertEqual(os.environ["TRUMOTION_TEST"], "1")

    def test_clean_data(self):
        sc = ps.SparkContext('local[4]')
        actual = clean_data(sc, 'train.txt').take(1)[0]

        # Matches test/data/train.txt
        desired = (0, np.array([[1.635533, 0.024848, 0.432087],
                                [1.547694, 0.008754, 0.319101]]))

        self.assertEqual(actual, desired)
