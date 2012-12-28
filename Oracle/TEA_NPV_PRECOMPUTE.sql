set define off;
  CREATE OR REPLACE FUNCTION "TEA_NPV_PRECOMPUTE" ( 
  foldChg IN NUMBER, 
  mu IN NUMBER, 
  sigma IN NUMBER
) RETURN NUMBER AS
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
******************************************************************/
 -------------------------------------------------------------------------------
  -- param foldChg: input is fold change ratio from from analysis_data table
  -- param mu: mean of all analsyis_data records for a given analysis
  -- param sigma: std dev of all analsyis_data records for a given analysis
  -------------------------------------------------------------------------------
  npv number;
  outlier_cutoff number:=1.0e-5;
  
BEGIN
  -- normalized p-value 
  npv:= 1.0 - cum_normal_dist(abs(foldChg),mu,sigma);
  
  -- cap outliers to minimum value
  IF npv<outlier_cutoff THEN npv:= outlier_cutoff; END IF;
  
  RETURN npv;
  
END TEA_NPV_PRECOMPUTE;

