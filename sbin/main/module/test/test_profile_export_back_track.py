# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.profile_export import ProfileBatchBackTracker


class ProfileCalBatchMonomerTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_export/back_track", "test1_origin.json")
        cf = ProfileBatchBackTracker(param)
        msg = {"applicationId": "aid", "data": {"match_info": u'code\u0001uuid\u0002'
                                                                u'{"uuid": "cf_output_uuid",'
                                                                u'"match_cnt": 10}'}, "code":2}
        cf.receive(json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 1)
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['profileIds'], ["1070_1000","1071_1000","5344_1000","5345_1000","5351_1000","3124_1000",
                                               "3125_1000","3126_1000","3129_1000"])


if __name__ == "__main__":
    unittest.main()
