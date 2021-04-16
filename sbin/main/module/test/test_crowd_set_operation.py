# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.filter import CrowdSetOperation


class CrowdSetOperationTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("crowd_set_operation", "crowd_set_operation1.json")
        cf = CrowdSetOperation(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "cf_output_uuid", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 1)
        self.mock_dfs.assert_called_once()
        self.hive_submit.assert_called()
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['uuid'], 'jimas_5b0fcb0c777ffff1fbeb995d')
        self.assertEqual(cf.job['params'][0]['output']['uuid'], '542_5d6e1ae675fabf9d5093d3a0')
        self.assertEqual(cf.job['params'][0]['output']['value'], '20180601_jimas_5b10bc397fbff7f73bd7befe')


if __name__ == "__main__":
    unittest.main()
