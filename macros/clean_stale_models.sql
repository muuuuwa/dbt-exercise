{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run=True) %}

    {% set get_drop_commands_query %}
        with table_info as (
            select
            case 
                when table_type = 'VIEW' 
                    then table_type
                else
                    'TABLE'
            end as drop_type,
            table_schema,
            table_name
            from {{ target.database }}.{{ target.schema }}.INFORMATION_SCHEMA.TABLES
            where table_schema = '{{ schema }}'
            and DATE(creation_time, 'Asia/Tokyo') < current_date - {{ days }}
        )
        select 
            drop_type,
            'DROP' || drop_type || ' ROBOTIC-TOTEM-344012.' || table_schema || '.' || table_name || ';' as drop_statement
        from table_info            
    {% endset %}

    {{ log(get_drop_commands_query, info=True) }}
    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for drop_query in drop_queries %}
        {% if execute and not dry_run %}
            {{ log('Dropping table/view with command: ' ~ drop_query, info=True) }}
            {% do run_query(drop_query) %}    
        {% else %}
            {{ log(drop_query, info=True) }}
        {% endif %}
    {% endfor %}
  
{% endmacro %}