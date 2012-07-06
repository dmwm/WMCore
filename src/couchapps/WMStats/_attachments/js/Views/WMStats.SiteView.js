WMStats.namespace("SiteView")

WMStats.SiteView = new WMStats._ViewBase('latestAgentSite', 
                                         {"reduce": true, "group_level": 2}, 
                                         WMStats.Sites, WMStats.SiteTable);

WMStats.SiteView.constructSiteKey = function(data) {
    /*
     * assemple keys from data for lasted site summary.
     * key format is [timestamp, site, agent_url]
     */
    var keys = [];
    for (var i in data.rows){
        keys.push([data.rows[i].value.max, data.rows[i].key[0], 
                   data.rows[i].key[1]]);
    }
    return keys;
}

WMStats.SiteView.callback = function(siteKeys) {
    var options = {"keys": WMStats.SiteView.constructSiteKey(siteKeys), "reduce": true, 
                       "group": true};

    WMStats.Couch.view('timeWithAgentSite', options, jQuery.proxy(this.visualize, this), 'json')
}

