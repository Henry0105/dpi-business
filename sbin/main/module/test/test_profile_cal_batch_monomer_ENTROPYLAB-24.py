# coding=utf-8

import unittest
from module.test.utils import TestJob


class ProfileCalBatchMonomerTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param_inputs = [{'profileIds': [1,2,3]}, {"profileIds": [2, 3, 4]}]
        inputs_len = len(param_inputs)
        profile_ids = []
        for input_idx, input in enumerate(param_inputs):
            profile_ids += list(set(input['profileIds']) - set(profile_ids))
            input['profileIds'] = [] if input_idx < inputs_len-1 else profile_ids
            print "input_idx is ", input_idx, "input['profileIds'] is ", input['profileIds']

        self.assertEqual(param_inputs[inputs_len-1]['profileIds'], [1, 2, 3, 4])

    def test_submit_2(self):
        param_inputs = [{'profileIds': []}, {"profileIds": [2, 3, 4]}]
        inputs_len = len(param_inputs)
        profile_ids = []
        for input_idx, input in enumerate(param_inputs):
            profile_ids += list(set(input['profileIds']) - set(profile_ids))
            input['profileIds'] = [] if input_idx < inputs_len-1 else profile_ids
            print "input_idx is ", input_idx, "input['profileIds'] is ", input['profileIds']

        self.assertEqual(param_inputs[inputs_len-1]['profileIds'], [2, 3, 4])


    def test_submit_3(self):
        param_inputs = [{'profileIds': [2,3,4]}, {"profileIds": []}]
        inputs_len = len(param_inputs)
        profile_ids = []
        for input_idx, input in enumerate(param_inputs):
            profile_ids += list(set(input['profileIds']) - set(profile_ids))
            input['profileIds'] = [] if input_idx < inputs_len-1 else profile_ids
            print "input_idx is ", input_idx, "input['profileIds'] is ", input['profileIds']

        self.assertEqual(param_inputs[inputs_len-1]['profileIds'], [2, 3, 4])


if __name__ == "__main__":
    unittest.main()
