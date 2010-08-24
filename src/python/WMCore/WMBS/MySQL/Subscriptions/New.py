sql = """insert into wmbs_subscription (fileset, workflow, type, parentage, last_update) 
                values ((select id from wmbs_fileset where name =:fileset),
                (select id from wmbs_workflow where spec = :spec and owner = :owner), :type, 
                :parentage, :timestamp)"""