# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.filter import CrowdFilter


class CrowdFilterTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("crowd_filter", "crowd_filter.json")
        cf = CrowdFilter(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "cf_output_uuid", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 0)
        self.mock_dfs.assert_called_once()
        self.hive_submit.assert_called()
        self.mq_send2.assert_called_once()
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['include']['agebin'], '5,6')
        self.assertEqual(input['include']['applist'], 'm.taobao.com')
        self.assertEqual(input['include']['cell_factory'], 'MEIZU,VIVO')
        self.assertEqual(input['exclude']['cell_factory'], '360,other')


if __name__ == "__main__":
    unittest.main()
