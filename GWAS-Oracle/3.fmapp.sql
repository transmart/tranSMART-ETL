

Update biomart.BIO_EXPERIMENT set BIO_EXPERIMENT_TYPE = 'experiment' where BIO_EXPERIMENT_ID=391523270;

insert into fmapp.fm_folder
 (folder_name
 ,folder_level
 ,folder_type
 ,active_ind
 )
 select 'MAGIC'
    ,1
    ,'STUDY'
    ,'1'
 from dual
 where not exists
    (select 1 from fmapp.fm_folder x
     where x.folder_name = 'MAGIC');




 insert into fmapp.fm_folder_association
 (folder_id
 ,object_uid
 ,object_type
 )
 select ff.folder_id
    ,'EXP:' || 'MAGIC'
    ,'bio.Experiment'
 from fmapp.fm_folder ff
 where folder_name = 'MAGIC'
   and not exists
      (select 1 from fmapp.fm_folder_association x
    where ff.folder_id = x.folder_id);

commit;
/
exit
