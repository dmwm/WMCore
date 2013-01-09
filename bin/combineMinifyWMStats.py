from glob import iglob
import shutil
import os

WMSTATS_SCRIPT_DIR = "../src/couchapps/WMStats/_attachments/js"
MINIFIED_DIR = "../src/couchapps/WMStats/_attachments/js/minified"
    
globalLib = ["WMStats.Globals.js",
             "WMStats.Utils.js",
             "WMStats.Couch.js"]

dataStrunct = ["DataStruct/WMStats._StructBase.js",
               "DataStruct/WMStats.GenericRequests.js",
               "DataStruct/WMStats.Agents.js",
               "DataStruct/WMStats.Sites.js",
               "DataStruct/WMStats.JobSummary.js",
               "DataStruct/WMStats.Campaigns.js",
               "DataStruct/WMStats.Alerts.js",
               "DataStruct/WMStats.SiteSummary.js",
               "DataStruct/WMStats.JobDetails.js",
               "DataStruct/WMStats.WorkloadSummary.js",
               "DataStruct/WMStats.History.js"]

# tier1 specific library
t1_dataStruct = ["DataStruct/T1/WMStats.RequestSummary.js",
                "DataStruct/T1/WMStats.CampaignSummary.js"]

# tier0 specific library
t0_dataStruct = ["DataStruct/T0/WMStats.RequestSummary.js",
                "DataStruct/T0/WMStats.RunSummary.js"]

# analysis specific library
an_dataStruct = ["DataStruct/T1/WMStats.RequestSummary.js",
                 "DataStruct/T1/WMStats.CampaignSummary.js",
                 "DataStruct/Analysis/WMStats.UserSummary.js"]

views = ["Views/Controls/WMStats.CommonControls.js",
         "Views/Tables/WMStats.Table.js",
         "Views/Tables/WMStats.JobSummaryTable.js",
         "Views/Tables/WMStats.SiteSummaryTable.js",
         "Views/Tables/WMStats.WorkloadSummaryTable.js",
         "Views/HTMLList/WMStats.JobDetailList.js",
         "Views/HTMLList/WMStats.AgentStatusGUI.js",
         "Views/Graphs/WMStats.SiteHistoryGraph.js"]

# tier1 specific library
t1_views = ["Views/Controls/T1/WMStats.Controls.js",
            "Views/Tables/T1/WMStats.ActiveRequestTable.js",
            "Views/Tables/T1/WMStats.CampaignSummaryTable.js",
            "Views/HTMLList/T1/WMStats.RequestDetailList.js",
            "Views/HTMLList/T1/WMStats.RequestAlertGUI.js",
            "Views/HTMLList/T1/WMStats.CategoryDetailList.js",
            "Views/HTMLList/T1/WMStats.RequestSummaryList.js",
            "Views/HTMLList/T1/WMStats.RequestDataList.js"]

# tier1 specific library
t0_views = ["Views/Controls/T0/WMStats.Controls.js",
            "Views/Tables/T0/WMStats.ActiveRequestTable.js",
            "Views/Tables/T0/WMStats.RunSummaryTable.js",
            "Views/HTMLList/T0/WMStats.RequestDetailList.js",
            "Views/HTMLList/T0/WMStats.RequestAlertGUI.js",
            "Views/HTMLList/T0/WMStats.CategoryDetailList.js",
            "Views/HTMLList/T0/WMStats.RequestSummaryList.js",
            "Views/HTMLList/T0/WMStats.RequestDataList.js"]

# tier1 specific library
an_views = ["Views/Controls/Analysis/WMStats.Controls.js",
            "Views/Tables/T1/WMStats.ActiveRequestTable.js",
            "Views/Tables/T1/WMStats.CampaignSummaryTable.js",
            "Views/Tables/Analysis/WMStats.UserSummaryTable.js",
            "Views/HTMLList/Analysis/WMStats.RequestDetailList.js",
            "Views/HTMLList/Analysis/WMStats.CategoryDetailList.js",
            "Views/HTMLList/T1/WMStats.RequestAlertGUI.js",
            "Views/HTMLList/T1/WMStats.RequestSummaryList.js",
            "Views/HTMLList/T1/WMStats.RequestDataList.js"]

models =["Models/WMStats._ModelBase.js",
         "Models/WMStats._RequestModelBase.js",
         "Models/WMStats.JobSummaryModel.js",
         "Models/WMStats.JobDetailModel.js",
         "Models/WMStats.AgentModel.js",
         "Models/WMStats.WorkloadSummaryModel.js",
         "Models/WMStats.HistoryModel.js"]

t1_models =["Models/T1/WMStats.ActiveRequestModel.js"]

t0_models =["Models/T0/WMStats.ActiveRequestModel.js"]

an_models =["Models/T1/WMStats.ActiveRequestModel.js"]

controller = ["Controller/WMStats.Env.js",
             "Controller/WMStats.GenericController.js",
             "Controller/WMStats.ActiveRequestController.js",
             "Controller/WMStats.CategoryMap.js",
             "Controller/WMStats.TableController.js",
             "Controller/WMStats.WorkloadSummaryController.js"]

t1_controller = ["Controller/T1/addCategoryMap.js"]
t0_controller = ["Controller/T0/addCategoryMap.js"]
an_controller = ["Controller/Analysis/addCategoryMap.js"]

def concatenateFiles(filelist, fileName):
    destPath = os.path.join(MINIFIED_DIR, fileName)
    destination = open(destPath, 'wb')
    for filename in filelist:
        filePath = os.path.join(WMSTATS_SCRIPT_DIR, filename)
        shutil.copyfileobj(open(filePath, 'rb'), destination)
    destination.close()
    try:
        from subprocess import call
        call(["uglifyjs", destPath, "-o", destPath])
    except Exception, ex:
        print "%s" % str(ex)
        pass
    
if __name__ == "__main__":
    
    initLoadDest = 'global.min.js'
    t1Dest = 'import-all-t1.min.js'
    t0Dest = 'import-all-t0.min.js'
    anDest = 'import-all-analysis.min.js'
    
    concatenateFiles(globalLib, initLoadDest)
    
    t1_lib = []
    t1_lib.extend(dataStrunct)
    t1_lib.extend(t1_dataStruct)
    t1_lib.extend(views)
    t1_lib.extend(t1_views)
    t1_lib.extend(models)
    t1_lib.extend(t1_models)
    t1_lib.extend(controller)
    t1_lib.extend(t1_controller)
    concatenateFiles(t1_lib, t1Dest)
    
    t0_lib = []
    t0_lib.extend(dataStrunct)
    t0_lib.extend(t0_dataStruct)
    t0_lib.extend(views)
    t0_lib.extend(t0_views)
    t0_lib.extend(models)
    t0_lib.extend(t0_models)
    t0_lib.extend(controller)
    t0_lib.extend(t0_controller)
    concatenateFiles(t0_lib, t0Dest)
    
    an_lib = []
    an_lib.extend(dataStrunct)
    an_lib.extend(an_dataStruct)
    an_lib.extend(views)
    an_lib.extend(an_views)
    an_lib.extend(models)
    an_lib.extend(an_models)
    an_lib.extend(controller)
    an_lib.extend(an_controller)
    concatenateFiles(an_lib, anDest)
    