create or replace function tm_cz.is_date
(p_text_date character varying
,p_date_format character varying default 'YYYY-MM-DD HH24:mi') returns numeric 
as $$ 
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
declare 
	v_tmp_date	timestamp without time zone;
 
begin 
    v_tmp_date := to_date(p_text_date, p_date_format);
    return 0; 
    EXCEPTION WHEN OTHERS then 
                return 1; 
end; 
$$ language plpgsql immutable strict; 