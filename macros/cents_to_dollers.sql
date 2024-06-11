{% macro cents_to_dollars(target_column, renamed_column) -%}
{{target_column}} / 100 as {{renamed_column}}
{%- endmacro -%}