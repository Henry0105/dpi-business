# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.portrait import CrowdCalculation


class CrowdCalculationTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("crowd_portrait", "crowd_portrait_calculation1.json")
        cc = CrowdCalculation(param)
        job_status = cc.submit()

        self.assertEqual(job_status.status, 0)
        self.mock_dfs.assert_called_once()
        self.hive_submit.assert_called()
        self.spark.submit.assert_called_once()

        print(json.dumps(cc.job, indent=1))
        input = cc.job['params'][0]['inputs'][0]

        self.assertEqual(input['tagList'], ["D001", "D002", "D003", "D004", "D011"])


if __name__ == "__main__":
    unittest.main()