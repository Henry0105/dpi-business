# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.profile_cal import ProfileCalBatchMonomer


class ProfileCalBatchMonomerTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_cal", "profile_cal_batch_monomer_origin.json")
        cf = ProfileCalBatchMonomer(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "cf_output_uuid", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 1)
        self.mq_send2.assert_called_once()
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['profileIds'], ["2_1001","3_1000","4466_1000", "1034_1000", "7_1000"])


if __name__ == "__main__":
    unittest.main()
