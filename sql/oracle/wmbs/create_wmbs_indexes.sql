-- Index creation statements for Oracle

-- File related indexes
CREATE INDEX wmbs_fileset_files_idx_fileset ON wmbs_fileset_files(fileset)
/

CREATE INDEX wmbs_fileset_files_idx_fileid ON wmbs_fileset_files(fileid)
/

CREATE INDEX wmbs_file_runlumi_map_fileid ON wmbs_file_runlumi_map(fileid)
/

CREATE INDEX wmbs_file_location_fileid ON wmbs_file_location(fileid)
/

CREATE INDEX wmbs_file_location_pnn ON wmbs_file_location(pnn)
/

CREATE INDEX wmbs_file_parent_parent ON wmbs_file_parent(parent)
/

CREATE INDEX wmbs_file_parent_child ON wmbs_file_parent(child)
/

-- Workflow related indexes
CREATE INDEX wmbs_workflow_output_workflow ON wmbs_workflow_output(workflow_id)
/

CREATE INDEX wmbs_workflow_output_fileset ON wmbs_workflow_output(output_fileset)
/

CREATE INDEX wmbs_workflow_output_merged ON wmbs_workflow_output(merged_output_fileset)
/

-- Subscription related indexes
CREATE INDEX wmbs_subscription_fileset ON wmbs_subscription(fileset)
/

CREATE INDEX wmbs_subscription_workflow ON wmbs_subscription(workflow)
/

CREATE INDEX wmbs_subscription_subtype ON wmbs_subscription(subtype)
/

CREATE INDEX wmbs_sub_files_available_sub ON wmbs_sub_files_available(subscription)
/

CREATE INDEX wmbs_sub_files_available_file ON wmbs_sub_files_available(fileid)
/

CREATE INDEX wmbs_sub_files_acquired_sub ON wmbs_sub_files_acquired(subscription)
/

CREATE INDEX wmbs_sub_files_acquired_file ON wmbs_sub_files_acquired(fileid)
/

CREATE INDEX wmbs_sub_files_failed_sub ON wmbs_sub_files_failed(subscription)
/

CREATE INDEX wmbs_sub_files_failed_file ON wmbs_sub_files_failed(fileid)
/

CREATE INDEX wmbs_sub_files_complete_sub ON wmbs_sub_files_complete(subscription)
/

CREATE INDEX wmbs_sub_files_complete_file ON wmbs_sub_files_complete(fileid)
/

-- Job related indexes
CREATE INDEX wmbs_jobgroup_sub ON wmbs_jobgroup(subscription)
/

CREATE INDEX wmbs_jobgroup_output ON wmbs_jobgroup(output)
/

CREATE INDEX wmbs_job_jobgroup ON wmbs_job(jobgroup)
/

CREATE INDEX wmbs_job_state_idx ON wmbs_job(state)
/

CREATE INDEX wmbs_job_location_idx ON wmbs_job(location)
/

CREATE INDEX wmbs_job_assoc_job ON wmbs_job_assoc(job)
/

CREATE INDEX wmbs_job_assoc_file ON wmbs_job_assoc(fileid)
/

CREATE INDEX wmbs_job_mask_job ON wmbs_job_mask(job)
/

-- Workunit related indexes
CREATE INDEX wmbs_workunit_taskid ON wmbs_workunit(taskid)
/

CREATE INDEX wmbs_workunit_status ON wmbs_workunit(status)
/

CREATE INDEX wmbs_job_workunit_job ON wmbs_job_workunit_assoc(job)
/

CREATE INDEX wmbs_job_workunit_workunit ON wmbs_job_workunit_assoc(workunit)
/

CREATE INDEX frl_workunit_assoc_workunit ON wmbs_frl_workunit_assoc(workunit)
/

CREATE INDEX frl_workunit_assoc_fileid ON wmbs_frl_workunit_assoc(fileid)
/

CREATE INDEX frl_workunit_assoc_run ON wmbs_frl_workunit_assoc(run)
/

CREATE INDEX frl_workunit_assoc_lumi ON wmbs_frl_workunit_assoc(lumi)
/

-- Location related indexes
CREATE INDEX wmbs_location_pnns_loc_idx ON wmbs_location_pnns(location)
/

CREATE INDEX wmbs_location_pnns_pnn_idx ON wmbs_location_pnns(pnn)
/

-- Checksums related indexes
CREATE INDEX wmbs_file_checksums_type ON wmbs_file_checksums(typeid)
/

CREATE INDEX wmbs_file_checksums_file ON wmbs_file_checksums(fileid)
/