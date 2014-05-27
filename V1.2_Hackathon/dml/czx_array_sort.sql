-- Function: tm_cz.czx_array_sort(anyarray)

DROP FUNCTION if exists tm_cz.czx_array_sort(anyarray);

CREATE OR REPLACE FUNCTION tm_cz.czx_array_sort(anyarray)
  RETURNS anyarray AS
$BODY$   
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
SELECT ARRAY(   
    SELECT $1[s.i] AS "foo"   
    FROM   
        generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)   
    ORDER BY foo   
);   
$BODY$
	LANGUAGE sql VOLATILE
	security definer 
	-- set a secure search_path: trusted schema(s), then pg_temp
	set search_path=tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, pg_temp
	cost 100;

