create or replace function tm_cz.is_date(varchar) returns numeric as $$ 
declare 
        i numeric; 
begin 
        i := $1::date; 
        return 0; 
        EXCEPTION WHEN invalid_text_representation then 
                return 1; 
end; 
$$ language plpgsql immutable strict; 