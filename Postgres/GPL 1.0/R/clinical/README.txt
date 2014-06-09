Here you find an alternative to upload clinical data into TranSMART.
Instead of using the Kettle-scripts an R-script is here to do the same job.
Differences with respect to the Ketlle-scripts:
- The R-script does not support "DATA_LABEL" columns (yet).
- The R-script supports uploading 'modifier_cd', 'units_cd' and 
  'start_time' (timestamp) into the observation_fact table.
  See the pdf in this directory.
In the "samllTest" directory you find a very small example which might explain
things a little more.
