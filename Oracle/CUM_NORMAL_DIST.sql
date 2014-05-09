set define off;
CREATE OR REPLACE FUNCTION "CUM_NORMAL_DIST" ( 
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
  -- param foldChg: fold change ratio from from analysis_data table
  -- param mu: mean of all analsyis_data records for a given analysis
  -- param sigma: std dev of all analsyis_data records for a given analysis
  -------------------------------------------------------------------------------

  -- temporary vars  
  t1 NUMBER;
  
  -- fractional error dist input
  fract_error_input NUMBER;
  
  -- return result (i.e. Prob [X<=x])
  ans NUMBER;  

BEGIN
  t1:= (foldChg-mu)/sigma;  
  fract_error_input:= t1/SQRT(2);
  ans:= 0.5 * (1.0 + fract_error_dist(fract_error_input));
  return ans; 
END CUM_NORMAL_DIST;

 
