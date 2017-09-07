from WMCore.Configuration import Configuration
config = Configuration()
config.section_('Agent')
# Agent:
config.Agent.hostName = None
config.Agent.contact = None
config.Agent.teamName = "team_usa"
config.Agent.agentName = None
config.section_('General')
# General: General Settings Section
config.General.workDir = '/home/test/application/WMAgentEmulator'
config.section_('CoreDatabase')
# CoreDatabase:
# dialect: Choose between oracle or mysql
# socket:  Set the socket file location for mysql (optional)
#
config.CoreDatabase.connectUrl='mysql://username:password@cmssrv18.fnal.gov:3306/TestDB'
config.component_('WMAgentEmulator')
# WMAgents:
config.WMAgentEmulator.componentDir = config.General.workDir + '/WMAgentEmulator'
config.WMAgentEmulator.namespace = "WMQuality.Emulators.WMAgents.WMAgentEmulator"
config.WMAgentEmulator.pollInterval = 10
