Here you find an alternative to upload clinical data into TranSMART.
Instead of using the Kettle-scripts an R-script is here to do the same job.
Differences with respect to the Ketlle-scripts:lumns (yet).
- The R-script supports uploading 'modifier_cd', 'units_cd' and 
  'start_time' (timestamp) into the observation_fact table.
  See the pdf in this directory.
- In case modifier_cd is used (longitudinal data) derived data from this
  serie will be calculated and shown in the "Data Explorer" view.
  For numerical series:
  - first, last
  - min, max, median, mean, std-dev
  - number
  - summation
  For categorical series
  - first, last
  - fractions of the specific value
  - number
In the "samllTest" directory you find a very small example which might explain
things a little more.
