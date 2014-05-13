set define off;
  CREATE OR REPLACE PROCEDURE "TM_CZ"."I2B2_DELETE_1_NODE" 
(
  path VARCHAR2
)
AS
BEGIN
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

  if path != ''  and path != '%'
  then 
    --I2B2
    DELETE 
      FROM OBSERVATION_FACT 
    WHERE 
      concept_cd IN (SELECT C_BASECODE FROM I2B2 WHERE C_FULLNAME = PATH);
    COMMIT;

      --CONCEPT DIMENSION
    DELETE 
      FROM CONCEPT_DIMENSION
    WHERE 
      CONCEPT_PATH = path;
    COMMIT;
    
      --I2B2
      DELETE
        FROM i2b2
      WHERE 
        C_FULLNAME = PATH;
    COMMIT;

  --i2b2_secure
      DELETE
        FROM i2b2_secure
      WHERE 
        C_FULLNAME = PATH;
    COMMIT;

  --i2b2_secure
      DELETE
        FROM concept_counts
      WHERE 
        concept_path = PATH;
    COMMIT;

  END IF;
  
END;

 
