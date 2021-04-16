# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.lookalike import Normal


class CrowdFilterTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("lookalike", "lookalike_normal_1.json")
        cf = Normal(param)
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

        self.assertEqual(input['uuid'], '1052c9e8b956e41d42e94469732580b2')


if __name__ == "__main__":
    unittest.main()
