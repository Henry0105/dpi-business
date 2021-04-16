# coding=utf-8
import ConfigParser
import logging
import os

from module.const.common import str2bool

__author__ = 'zhangjt'


class PropUtils:
    def __init__(self):
        _path = "%s/conf/hive_database_table.properties" % os.environ.get('DATAENGINE_HOME')
        with open(_path) as f:
            def my_filter(line):
                new_line = str.strip(line)
                if new_line is "" or str.strip(line).startswith("#"):
                    return False
                return True

            filter_lines = filter(lambda line: my_filter(line), f.readlines())
            self.props = {key.strip(): value.strip() for key, value in [kv.split("=") for kv in filter_lines]}

        logging.debug(self.props)

        self.HIVE_TABLE_DATA_OPT_CACHE = self.get_property("table_data_opt_cache")
        self.HIVE_TABLE_DATA_OPT_CACHE_NEW = self.get_property("table_data_opt_cache_new")
        self.HIVE_TABLE_DATA_HUB = self.get_property("table_data_hub")
        self.HIVE_TABLE_DEVICE_PROFILE_INFO_SEC = self.get_property("table_dm_device_profile_info_sec")
        self.HIVE_TABLE_DEVICE_SINGLE_PROFILE_INFO = self.get_property("table_single_profile_info")
        self.HIVE_TABLE_SINGLE_PROFILE_TRACK_INFO = self.get_property("table_single_profile_track_info")
        self.HIVE_TABLE_IDFA_PROFILE_INFO = self.get_property("table_dm_idfa_profile_info")
        self.HIVE_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE = self.get_property("table_crowd_portrait_calculation_score")
        self.HIVE_TABLE_CROWD_PORTRAIT_ADJUST_CALCULATION_SCORE = self.get_property("table_crowd_portrait_calculation_adjust_score")
        self.HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE = self.get_property("table_crowd_portrait_estimation_score")
        self.HIVE_TABLE_DM_DEVICE_MAPPING = self.get_property("table_dm_device_mapping")
        self.HIVE_TABLE_DM_DEVICE_MAPPING_V2 = self.get_property("table_dm_device_mapping_v2")
        self.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL = self.get_property("table_id_mapping_external_full")
        self.HIVE_TALBE_MOBEYE_O2O_BASE_SCORE_DAILY = self.get_property("table_mobeye_o2o_base_score_daily")
        self.HIVE_TABLE_APPINFO_DAILY = self.get_property("table_appinfo_daily")
        self.HIVE_TABLE_APPINFO_DAILY_V2 = self.get_property("table_appinfo_daily_v2")
        self.HIVE_TABLE_MOBEYE_O2O_LBS_FREQUENCY_DAILY = self.get_property("table_mobeye_o2o_lbs_frequency_daily")
        self.HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY = self.get_property("table_mobeye_o2o_lbs_homeandwork_daily")
        self.HIVE_TABLE_MOBEYE_O2O_LBS_SOUREANDFLOW_DAILY = self.get_property("table_mobeye_o2o_lbs_soureandflow_daily")
        self.HIVE_TABLE_APPPKG_VECTOR_DATA_OPT = self.get_property("table_apppkg_vector_data_opt")
        self.HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI = self.get_property("table_apppkg_icon2vec_par_wi")
        self.HIVE_TABLE_RP_DPI_MKT_URL_INPUT = self.get_property("hive_table_rp_dpi_mkt_url_input")

    def get_property(self, key):
        if key in self.props:
            return self.props[key]
        else:
            return None


class AppPropUtils:
    def __init__(self):
        _path = "%s/conf/application.properties" % os.environ.get('DATAENGINE_HOME')
        with open(_path) as f:
            def my_filter(line):
                new_line = str.strip(line)
                if new_line is "" or str.strip(line).startswith("#"):
                    return False
                return True

            filter_lines = filter(lambda line: my_filter(line), f.readlines())
            self.props = {key.strip(): value.strip() for key, value in [kv.split("=", 1) for kv in filter_lines]}

        logging.debug(self.props)

    def get_property(self, key):
        if key in self.props:
            return self.props[key]
        else:
            return None


class CfgParser:
    _boolean_states = {'1': True, 'yes': True, 'true': True, 'on': True,
                       '0': False, 'no': False, 'false': False, 'off': False}

    def __init__(self):
        _cfg = "%s/conf/app.cfg" % os.environ.get('DATAENGINE_HOME')
        # _cfg = "/Users/juntao/src/Yoozoo/dataengine/resources/app.cfg"
        logging.debug("cfg path is %s" % _cfg)
        self.scope = os.environ.get('MID_ENGINE_ENV')
        # self.scope = "pre-85"
        logging.debug("MID_ENGINE_ENV=%s" % self.scope)
        self.config = ConfigParser.ConfigParser()
        self.config.read(_cfg)
        if self.scope.find("-") != -1:
            self.scope_parent = self.scope.split("-")[0]
        else:
            self.scope_parent = None

    def getint(self, name):
        return int(self.get(name))

    def getboolean(self, name):
        v = self.get(name)
        if v is None:
            return False
        return str2bool(v)

    def getfloat(self, name):
        return float(self.get(name))

    def get(self, name):
        if self.config.has_option(self.scope, name):
            return self.config.get(self.scope, name)
        elif self.scope_parent is not None and self.config.has_option(self.scope_parent, name):
            return self.config.get(self.scope_parent, name)
        elif self.config.has_option('default', name):
            return self.config.get('default', name)
        else:
            return None


cfg_parser = CfgParser()
prop_utils = PropUtils()
app_utils = AppPropUtils()

if __name__ == '__main__':
    # print(cfg_parser.get("master_hbase_zk"))
    # print(cfg_parser.get("dataengine_data_home"))
    print(prop_utils.get_property("view.original_android_id_mapping"))
    print(prop_utils.get_property("view.original_android_id_mapping_1"))
