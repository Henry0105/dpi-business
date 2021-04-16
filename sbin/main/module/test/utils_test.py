import os

os.environ['MID_ENGINE_ENV'] = 'dev'
os.environ['DATAENGINE_HOME'] = '../../../../'

from module.tools.utils import unique, get_limit, get_limit_clause

__author__ = 'zhangjt'
import unittest


class TestToolMethods(unittest.TestCase):
    def test_unique(self):
        list1 = [40, 40, 10, 30, 20, 10]
        list2 = [40, 10, 30, 20]
        list_unique = unique(list1)
        self.assert_(list_unique == list2)

    def test_get_limit(self):
        output = {"limit": 100}
        limit_value = get_limit(output)
        self.assert_(limit_value == 100)

        output_none = {}
        limit_value_none = get_limit(output_none)
        self.assert_(limit_value_none == 20000000)

        output_nolimit = {"limit": -1}
        limit_value_nolimit = get_limit(output_nolimit)
        self.assert_(limit_value_nolimit is None)

    def test_get_limit_clause(self):
        limit_clause = get_limit_clause(100)
        self.assert_(limit_clause == " limit 100")
        limit_clause_none = get_limit_clause(None)
        self.assert_(limit_clause_none == "")
        limit_clause_no = get_limit_clause(-1)
        self.assert_(limit_clause_no == "")