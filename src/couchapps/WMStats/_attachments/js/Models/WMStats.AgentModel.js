WMStats.namespace("AgentModel");
WMStats.AgentModel = new WMStats._ModelBase('agentInfo', {}, WMStats.Agents);
WMStats.AgentModel.setTrigger(WMStats.CustomEvents.AGENTS_LOADED);
