WMStats.namespace("AgentModel")
WMStats.AgentModel = new WMStats._ModelBase('agentInfo', {}, 
                                          WMStats.Agents, WMStats.AgentTable);
WMStats.AgentModel.setTrigger("agentReady");

