
from glob import iglob
import shutil
import os

WMSTATS_SCRIPT_DIR = "../src/couchapps/WMStats/_attachments/js"
MINIFIED_DIR = "../src/couchapps/WMStats/_attachments/js/minified"

globalLib = ["WMStats.Globals.js",
             "WMStats.Utils.js",
             "WMStats.Couch.js",
             "WMStats.Ajax.js"]

dataStrunct = ["DataStruct/WMStats._StructBase.js",
               "DataStruct/WMStats.GenericRequests.js",
               "DataStruct/WMStats.Tasks.js",
               "DataStruct/WMStats.Agents.js",
               "DataStruct/WMStats.Sites.js",
               "DataStruct/WMStats.JobSummary.js",
               "DataStruct/WMStats.Campaigns.js",
               "DataStruct/WMStats.Alerts.js",
               "DataStruct/WMStats.SiteSummary.js",
               "DataStruct/WMStats.JobDetails.js",
               "DataStruct/WMStats.WorkloadSummary.js",
               "DataStruct/WMStats.History.js",
               "DataStruct/WMStats.LogDBData.js",
               "DataStruct/WMStats.LogMessage.js"]

# tier1 specific library
t1_dataStruct = ["DataStruct/T1/WMStats.RequestSummary.js",
                "DataStruct/T1/WMStats.CampaignSummary.js",
                "DataStruct/T1/WMStats.CMSSWSummary.js",
                "DataStruct/T1/WMStats.AgentRequestSummary.js",
                "DataStruct/T1/WMStats.ReqMgrRequest.js"]


# tier0 specific library
t0_dataStruct = ["DataStruct/T0/WMStats.RequestSummary.js",
                "DataStruct/T0/WMStats.RunSummary.js"]

viewModel = ["ViewModels/WMStats.ViewModel.js"]

views = ["Views/WMStats.CategoryMap.js",
         "Views/WMStats.View.IndexHTML.js",
         "Views/Controls/WMStats.CommonControls.js",
         "Views/Tables/WMStats.Table.js",
         "Views/Tables/WMStats.JobSummaryTable.js",
         "Views/Tables/WMStats.WorkloadSummaryTable.js",
         "Views/Tables/WMStats.TableController.js",
         "Views/HTMLList/WMStats.JobDetailList.js",
         "Views/HTMLList/WMStats.AgentDetailList.js",
         "Views/HTMLList/WMStats.CategoryTitle.js",
         "Views/HTMLList/WMStats.RequestTitle.js",
         "Views/HTMLList/WMStats.RequestLogList.js",
         "Views/Graphs/WMStats.SiteHistoryGraph.js"
         ]

# tier1 specific library
t1_views = ["Views/Controls/T1/WMStats.Controls.js",
            "Views/Tables/T1/WMStats.ActiveRequestTable.js",
            "Views/Tables/T1/WMStats.ActiveRequestTableWithJob.js",
            "Views/Tables/T1/WMStats.TaskSummaryTable.js",
            "Views/Tables/T1/WMStats.CampaignSummaryTable.js",
            "Views/Tables/T1/WMStats.CMSSWSummaryTable.js",
            "Views/Tables/T1/WMStats.SiteSummaryTable.js",
            "Views/Tables/T1/WMStats.AgentRequestSummaryTable.js",
            "Views/Tables/T1/addCategoryMap.js",
            "Views/HTMLList/T1/WMStats.RequestDetailList.js",
            "Views/HTMLList/T1/WMStats.RequestAlertGUI.js",
            "Views/HTMLList/T1/WMStats.CategoryDetailList.js",
            "Views/HTMLList/T1/WMStats.RequestSummaryList.js",
            "Views/HTMLList/T1/WMStats.RequestDataList.js",
            "Views/HTMLList/T1/WMStats.ResubmissionList.js"]

# tier1 specific library
t0_views = ["Views/Controls/T0/WMStats.Controls.js",
            "Views/Tables/T0/WMStats.ActiveRequestTable.js",
            "Views/Tables/T0/WMStats.TaskSummaryTable.js",
            "Views/Tables/T0/WMStats.RunSummaryTable.js",
            "Views/Tables/T0/addCategoryMap.js",
            "Views/HTMLList/T0/WMStats.RequestDetailList.js",
            "Views/HTMLList/T0/WMStats.RequestAlertGUI.js",
            "Views/HTMLList/T0/WMStats.CategoryDetailList.js",
            "Views/HTMLList/T0/WMStats.RequestSummaryList.js",
            "Views/HTMLList/T0/WMStats.RequestDataList.js"]

models =["Models/WMStats._ModelBase.js",
         "Models/WMStats._AjaxModelBase.js",
         "Models/WMStats._RequestModelBase.js",
         "Models/WMStats.JobSummaryModel.js",
         "Models/WMStats.JobDetailModel.js",
         "Models/WMStats.AgentModel.js",
         "Models/WMStats.WorkloadSummaryModel.js",
         "Models/WMStats.HistoryModel.js"]

t1_models =["Models/T1/WMStats.ActiveRequestModel.js",
            "Models/T1/WMStats.ReqMgrRequestModel.js",
            "Models/T1/WMStats.RequestSearchModel.js",
            "Models/T1/WMStats.RequestLogDetailModel.js",
            "Models/T1/WMStats.RequestLogModel.js"]

t0_models =["Models/T0/WMStats.ActiveRequestModel.js",
            "Models/T0/WMStats.RequestModel.js",
            "Models/T0/WMStats.RequestSearchModel.js",
            "Models/T0/WMStats.RequestLogDetailModel.js",
            "Models/T0/WMStats.RequestLogModel.js"]


controller = ["Controller/WMStats.GenericController.js",
             "Controller/WMStats.ActiveRequestController.js",
             "Controller/WMStats.WorkloadSummaryController.js"]


def concatenateFiles(filelist, fileName):
    destPath = os.path.join(MINIFIED_DIR, fileName)
    destination = open(destPath, 'wb')
    for filename in filelist:
        filePath = os.path.join(WMSTATS_SCRIPT_DIR, filename)
        shutil.copyfileobj(open(filePath, 'rb'), destination)
    destination.close()
    try:
        from subprocess import call
        print(destPath)
        call(["uglifyjs", destPath, "-o", destPath])
    except Exception as ex:
        print("%s" % str(ex))
        pass

if __name__ == "__main__":

    initLoadDest = 'global.min.js'
    t1Dest = 'import-all-t1.min.js'
    t0Dest = 'import-all-t0.min.js'

    concatenateFiles(globalLib, initLoadDest)

    t1_lib = []
    t1_lib.extend(dataStrunct)
    t1_lib.extend(t1_dataStruct)
    t1_lib.extend(viewModel)
    t1_lib.extend(views)
    t1_lib.extend(t1_views)
    t1_lib.extend(models)
    t1_lib.extend(t1_models)
    t1_lib.extend(controller)
    concatenateFiles(t1_lib, t1Dest)

    t0_lib = []
    t0_lib.extend(dataStrunct)
    t0_lib.extend(t0_dataStruct)
    t0_lib.extend(viewModel)
    t0_lib.extend(views)
    t0_lib.extend(t0_views)
    t0_lib.extend(models)
    t0_lib.extend(t0_models)
    t0_lib.extend(controller)
    concatenateFiles(t0_lib, t0Dest)


