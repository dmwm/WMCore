-- Initial data inserts for MariaDB

-- Job states
INSERT INTO wmbs_job_state (name) VALUES ('new');
INSERT INTO wmbs_job_state (name) VALUES ('created');
INSERT INTO wmbs_job_state (name) VALUES ('executing');
INSERT INTO wmbs_job_state (name) VALUES ('complete');
INSERT INTO wmbs_job_state (name) VALUES ('success');
INSERT INTO wmbs_job_state (name) VALUES ('retrydone');
INSERT INTO wmbs_job_state (name) VALUES ('exhausted');
INSERT INTO wmbs_job_state (name) VALUES ('killed');
INSERT INTO wmbs_job_state (name) VALUES ('createcooloff');
INSERT INTO wmbs_job_state (name) VALUES ('createfailed');
INSERT INTO wmbs_job_state (name) VALUES ('createpaused');
INSERT INTO wmbs_job_state (name) VALUES ('submitcooloff');
INSERT INTO wmbs_job_state (name) VALUES ('submitfailed');
INSERT INTO wmbs_job_state (name) VALUES ('submitpaused');
INSERT INTO wmbs_job_state (name) VALUES ('jobcooloff');
INSERT INTO wmbs_job_state (name) VALUES ('jobfailed');
INSERT INTO wmbs_job_state (name) VALUES ('jobpaused');
INSERT INTO wmbs_job_state (name) VALUES ('cleanout');

-- Subscription types
INSERT INTO wmbs_sub_types (name, priority) VALUES ('Production', 0);
INSERT INTO wmbs_sub_types (name, priority) VALUES ('Processing', 0);
INSERT INTO wmbs_sub_types (name, priority) VALUES ('Cleanup', 1);
INSERT INTO wmbs_sub_types (name, priority) VALUES ('LogCollect', 2);
INSERT INTO wmbs_sub_types (name, priority) VALUES ('Skim', 3);
INSERT INTO wmbs_sub_types (name, priority) VALUES ('Merge', 4);
INSERT INTO wmbs_sub_types (name, priority) VALUES ('Harvesting', 5);

-- Location states
INSERT INTO wmbs_location_state (name) VALUES ('Normal');
INSERT INTO wmbs_location_state (name) VALUES ('Down');
INSERT INTO wmbs_location_state (name) VALUES ('Draining');
INSERT INTO wmbs_location_state (name) VALUES ('Aborted');

-- Checksum types
INSERT INTO wmbs_checksum_type (type) VALUES ('cksum');
INSERT INTO wmbs_checksum_type (type) VALUES ('adler32');
INSERT INTO wmbs_checksum_type (type) VALUES ('md5');
