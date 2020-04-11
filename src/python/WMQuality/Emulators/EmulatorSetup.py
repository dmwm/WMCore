"""
Use this for only unit test
"""
import os
import tempfile

from WMCore.Configuration import Configuration, saveConfigurationFile


def deleteConfig(configFile):
    if os.path.exists(configFile):
        os.remove(configFile)
    else:
        pass


def setupWMAgentConfig():
    _, configFile = tempfile.mkstemp(".py", "TESTAGENTConfig", )
    os.environ["WMAGENT_CONFIG"] = configFile
    _wmAgentConfig(configFile)
    return configFile


def _wmAgentConfig(configFile):
    config = Configuration()
    config.section_("General")
    config.General.logdb_name = "unittest_logdb"
    config.General.central_logdb_url = "http://localhost/central_logdb"
    config.General.ReqMgr2ServiceURL = "http://localhost/reqmgr2"

    config.section_("JobStateMachine")
    # Waring setting couchDB to None will cause the ERROR:
    # but that should be ignored, if you want to test couchDB
    # set the real couchDB information here
    config.JobStateMachine.couchurl = os.getenv("COUCHURL")
    config.JobStateMachine.couchDBName = os.getenv("COUCHDB")
    config.JobStateMachine.jobSummaryDBName = "wmagent_summary_test"
    config.JobStateMachine.summaryStatsDBName = "stat_summary_test"

    config.section_("Agent")
    # User specific parameter
    config.Agent.hostName = "cmssrv52.fnal.gov"
    # User specific parameter
    config.Agent.contact = "sfoulkes@fnal.gov"
    # User specific parameter
    config.Agent.teamName = "DMWM"
    # User specific parameter
    config.Agent.agentName = "WMAgent"
    config.Agent.useMsgService = False
    config.Agent.useTrigger = False
    config.Agent.isDocker = False

    # BossAir setup
    config.section_("BossAir")
    config.BossAir.pluginNames = ['TestPlugin', 'SimpleCondorPlugin']
    config.BossAir.pluginDir = 'WMCore.BossAir.Plugins'

    saveConfigurationFile(config, configFile)
