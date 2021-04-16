# coding=utf-8
import os

__author__ = 'zhangjt'

_boolean_states = {'1': True, 'yes': True, 'true': True, 'on': True,
                   '0': False, 'no': False, 'false': False, 'off': False}


def str2bool(v):
    if v.lower() not in _boolean_states:
        raise ValueError, 'Not a boolean: %s' % v
    return _boolean_states[v.lower()]


mock_flag = str2bool(os.environ.get('dataengine_shell_mock')) \
    if os.environ.has_key('dataengine_shell_mock') \
    else False

id_dict = {'1': 'imei', '2': 'mac', '3': 'phone', '4': 'device', '5': 'imei_14'}