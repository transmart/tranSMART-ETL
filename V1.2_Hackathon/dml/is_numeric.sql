drop function if exists tm_cz.is_numeric(varchar);

create or replace function tm_cz.is_numeric(varchar) returns numeric 
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
        i numeric; 
begin 
        i := $1::numeric; 
        return 0; 
        EXCEPTION WHEN invalid_text_representation then 
                return 1; 
end; 
$$ language plpgsql immutable strict; 