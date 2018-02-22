from __future__ import print_function, division

from WMCore.ReqMgr.DataStructs.DefaultConfig.EDITABLE_SPLITTING_PARAM_CONFIG import EDITABLE_SPLITTING_PARAM_CONFIG
from WMCore.ReqMgr.DataStructs.DefaultConfig.DAS_RESULT_FILTER import DAS_RESULT_FILTER
from WMCore.ReqMgr.DataStructs.DefaultConfig.PERMISSION_BY_REQUEST_TYPE import PERMISSION_BY_REQUEST_TYPE

#TODO need to find the better way to import all the config from DefaultConfig area
DEFAULT_CONFIG = {'EDITABLE_SPLITTING_PARAM_CONFIG': EDITABLE_SPLITTING_PARAM_CONFIG,
                  'DAS_RESULT_FILTER': DAS_RESULT_FILTER,
                  'PERMISSION_BY_REQUEST_TYPE': PERMISSION_BY_REQUEST_TYPE}

class ReqMgrConfigDataCache(object):
    # this cache shouldn't be accessed directly
    # Only through the reqmgr2 api
    _req_aux_db = None
    _req_config_data_cache = {}

    @staticmethod
    def set_aux_db(couchdb):
        ReqMgrConfigDataCache._req_aux_db = couchdb
        return

    @staticmethod
    def register_default(default_config):
        ReqMgrConfigDataCache._req_config_data_cache.update(default_config)
        return

    @staticmethod
    def getConfig(doc_name):

        try:
            config = ReqMgrConfigDataCache._req_aux_db.document(doc_name)
            del config["_id"]
            del config["_rev"]
        # TODO only get the exception when server is not available.
        except Exception as ex:
            config = ReqMgrConfigDataCache._req_config_data_cache.get(doc_name, None)
            print("Getting from Cache due to: %s" % str(ex))
        return config

    @staticmethod
    def replaceConfig(doc_name, content):
        if doc_name not in ReqMgrConfigDataCache._req_config_data_cache:
            raise Exception("No config exists")

        if ReqMgrConfigDataCache._req_aux_db.documentExists(doc_name):
            ReqMgrConfigDataCache._req_aux_db.delete_doc(doc_name)
        response = ReqMgrConfigDataCache._req_aux_db.putDocument(doc_name, content)
        ReqMgrConfigDataCache._req_config_data_cache.update(doc_name = content)
        return response

    @staticmethod
    def putDefaultConfig():
        error = ""
        for doc_name, content in DEFAULT_CONFIG.items():
            try:
                ReqMgrConfigDataCache.replaceConfig(doc_name, content)
            except Exception as ex:
                error += str(ex)

        return error

# register all default config
ReqMgrConfigDataCache.register_default(DEFAULT_CONFIG)
