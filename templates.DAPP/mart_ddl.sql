-- drop table if exists {{ ctx.schema }}.{{ ctx.name }} cascade;
CREATE TABLE {{ ctx.schema }}.{{ ctx.name }} (
{%- for field in ctx.field_ctx_list %}
  {{ field.name.lower() }} {{  field.datatype.lower() }}
  {%- if not field.is_nullable -%} 
{{ ' not null' }} 
  {%- endif -%}
{%- if not loop.last -%},{% endif -%}
{%- endfor %}
)
WITH (
 appendonly=true,
 orientation=column,
 compresstype=zstd,
 compresslevel=1
)
DISTRIBUTED BY ({{ ctx.distributed_by }});

-- Комментарии к полям таблицы
{%- for field in ctx.field_ctx_list %}
{%- if  field.comment|length %}
COMMENT ON COLUMN {{ ctx.schema }}.{{ ctx.name }}.{{ field.name.lower() }} IS '{{ field.comment }}';
{%- endif -%}
{%- endfor %}