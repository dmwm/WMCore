/** abstract environment of the web page */
WMStats.namespace("Env");
// page view ("#activeRequestPage", "#workloadSummaryPage")
WMStats.Env.Page = "#activeRequestPage";
// summary view ( contains #category_view, #request_view, #job_view)
WMStats.Env.View = "#category_view";
// category view selection (different variation has different values, i.e. site, campaing)
WMStats.Env.CategorySelection = null;
// Request selection: all request or categorize request
WMStats.Env.RequestSelection = "all_requests";
// s
WMStats.Env.ViewSwitchSelection = null;

// place holder for current data in category_view
WMStats.Env.CategoryData = null;
// place holder for current data in request_view
WMStats.Env.CurrentRequestData = null;

// env variable for request detail box status
WMStats.Env.RequestDetailOpen = false;
