WMStats.namespace("CampaignModel")

WMStats.CampaignModel = new WMStats._ModelBase('campaignStatus',
                          {'reduce': true, 'group_level':1, 'descending':true}, 
                          WMStats.Campaigns, WMStats.CampaignTable);
WMStats.CampaignModel.setTrigger("campaignReady");
