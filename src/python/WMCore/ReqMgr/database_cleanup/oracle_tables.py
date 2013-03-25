# definition of the production Oracle DB for ReqMgr
# names of the columns as returned by the "describe <table name>" command
# on the SQLPlus prompt
# Oracle DB tables defined in 
#   WMCore/RequestManager/RequestDB/Oracle/Create.py


reqmgr_oracle_tables_defition = {
    "reqmgr_request_type":
        ["TYPE_ID", "TYPE_NAME"],
    
    "reqmgr_request_status":
        ["STATUS_ID", "STATUS_NAME"],
    
    "reqmgr_group":
        ["GROUP_ID", "GROUP_NAME", "GROUP_BASE_PRIORITY"],
    
    "reqmgr_requestor":
        ["REQUESTOR_ID", "REQUESTOR_HN_NAME", "CONTACT_EMAIL",
         "REQUESTOR_DN_NAME", "REQUESTOR_BASE_PRIORITY"],
    
    "reqmgr_group_association":
        ["ASSOCIATION_ID", "REQUESTOR_ID", "GROUP_ID"],
    
    "reqmgr_teams":
        ["TEAM_ID", "TEAM_NAME"],
    
    "reqmgr_request":
        ["REQUEST_ID", "REQUEST_NAME", "REQUEST_TYPE", "REQUEST_STATUS",
         "REQUEST_PRIORITY", "REQUESTOR_GROUP_ID", "WORKFLOW", 
         "REQUEST_EVENT_SIZE", "REQUEST_SIZE_FILES", "PREP_ID", 
         "REQUEST_NUM_EVENTS"],
    
    "reqmgr_assignment":
        ["REQUEST_ID", "TEAM_ID", "PRIORITY_MODIFIER"],
    
    "reqmgr_input_dataset":
        ["DATASET_ID", "REQUEST_ID", "DATASET_NAME", "DATASET_TYPE"],
    
    "reqmgr_output_dataset":
        ["DATASET_ID", "REQUEST_ID", "DATASET_NAME", "SIZE_PER_EVENT",
         "CUSTODIAL_SITE"],
    
    "reqmgr_software":
        ["SOFTWARE_ID", "SOFTWARE_NAME", "SCRAM_ARCH"],
    
    "reqmgr_software_dependency":
        ["REQUEST_ID", "SOFTWARE_ID"],
    
    "reqmgr_progress_update":
        ["REQUEST_ID", "UPDATE_TIME", "EVENTS_WRITTEN", "EVENTS_MERGED",
        "FILES_WRITTEN", "FILES_MERGED", "ASSOCIATED_DATASET",
        "TIME_PER_EVENT", "SIZE_PER_EVENT", "PERCENT_SUCCESS", 
        "PERCENT_COMPLETE"],
    
    "reqmgr_message":
        ["REQUEST_ID", "UPDATE_TIME", "MESSAGE"],
    
    "reqmgr_assigned_prodmgr":
        ["REQUEST_ID", "PRODMGR_ID"],
    
    "reqmgr_assigned_prodagent":
        ["REQUEST_ID", "PRODAGENT_ID"],
    
    "reqmgr_campaign":
        ["CAMPAIGN_ID", "CAMPAIGN_NAME"],
    
    "reqmgr_campaign_assoc":
        ["REQUEST_ID", "CAMPAIGN_ID"]
}