WMStats.namespace("CampaignView")

WMStats.CampaignView = new WMStats._ViewBase('campaignStatus',
                          {'reduce': true, 'group_level':1, 'descending':true}, 
                          WMStats.Campaigns, WMStats.CampaignTable);