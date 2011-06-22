function(doc, site) {
	var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];

	// !code lib/workqueue_utils.js

	if (ele && ele["Status"] === "Available") {
		var input_sites = workqueue_utils.commonInputDataSites(ele);
		// check for at least one site hosting input data
		if (ele.Inputs && !input_sites.length) {
			emit('No site with data');
			return;
		}
		// check for at least one site with data in the whitelist
		if (ele.SiteWhitelist.length && input_sites) {
			var inputs_in_whitelist = false;
			for (var i = 0; i < ele.SiteWhitelist.length; i++) {
				if (input_sites.indexOf(ele.SiteWhitelist[i]) !== -1) {
					inputs_in_whitelist = true;
					break;
				}
			}
			if (!inputs_in_whitelist) {
				emit('No hosting site in whitelist');
				return;
			}
		}
		// check for at least one site with data not in the blacklist
		if (ele.SiteBlacklist.length && input_sites) {
			var inputs_in_blacklist = true
			for (var i = 0; i < input_sites.length; i++) {
				if (ele.SiteBlacklist.indexOf(input_sites[i]) === -1) {
					inputs_in_blacklist = false;
					break;
				}
			}
			if (inputs_in_blacklist) {
				emit('Hosting site in blacklist');
				return;
			}
		}
	}
}
