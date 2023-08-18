-- {{ ctx.name }} definition

-- Drop table

--DROP TABLE {{ ctx.name }} CASCADE;

CREATE TABLE rdv.{{ ctx.name }} (
{%- for field in ctx.field_ctx_list %}
{{ field.name.lower() }} {{  field.datatype.lower() }} 
 {%- if not field.is_nullable -%} 
{{ ' not null' }} 
{%-  endif -%},
{%- endfor %}
)
WITH (
 appendonly=true,
 orientation=column,
 compresstype=zstd,
 compresslevel=1
)
DISTRIBUTED BY (<<change_me>>);

--grant all on {{ ctx.name }} to dev_zi21_services;
--grant all on {{ ctx.name }} to dev_zi21_etl;

--select * from {{ ctx.name }};
