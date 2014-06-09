  set define off;
  CREATE OR REPLACE FUNCTION "FRACT_ERROR_DIST" 
( normInput IN NUMBER
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

  -- temp var
  t1 NUMBER:= 1.0 / (1.0 + 0.5 * ABS(normInput));
  
  -- exponent input to next equation
  exponent_input NUMBER:= -normInput*normInput - 1.26551223 + 
                           t1*(1.00002368 + t1*(0.37409196 + t1*(0.09678418 + t1*(-0.18628806 + t1*(0.27886807 + t1*(-1.13520398 + t1*(1.48851587 + t1*(-0.82215223 + t1*(0.17087277)))))))));
  -- Horner's method
  ans NUMBER:= 1 - t1 * EXP(exponent_input);

  fractError NUMBER;

BEGIN
  -- handle sign
  IF normInput>0 THEN fractError:= ans; ELSE fractError:= -ans; END IF;
  return fractError;

END FRACT_ERROR_DIST;

 

