{% macro build_shares() %}

    -- Get the Shared Sites
    {% set shared_sites_sql %}
        SELECT 
             ss.site_id
            ,REPLACE(LOWER(ss.slug), ' ', '_') AS slug
        FROM stage.shared_sites AS ss
    {% endset %}
    {% set shared_sites_results = run_query(shared_sites_sql) %}
    {% set site_ids = shared_sites_results.columns[0].values() %}
    {% set slugs = shared_sites_results.columns[1].values() %}

    -- Build new schemas
    {% set new_schemas_sql %}
        SELECT REPLACE(LOWER(ss.slug), ' ', '_') AS slug
        FROM stage.shared_sites AS ss
            LEFT JOIN information_schema.schemata AS s
                ON LOWER(ss.slug) = LOWER(s.schema_name)
        WHERE s.schema_name IS NULL
    {% endset %}
    {% set new_schemas_results = run_query(new_schemas_sql) %}
    {% set new_schemas = new_schemas_results.columns[0].values() %}

    {% for schema_name in new_schemas %}
        {% set create_schema %}
            CREATE SCHEMA {{ schema_name }}
        {% endset %}
        {% do run_query(create_schema) %}

        {% if target.database.upper() == 'DATA_WAREHOUSE' %}
            -- set create_managed_account here once you have it figured out

            {% set create_share %}
                CREATE SHARE {{ schema_name }}
            {% endset %}
            {% do run_query(create_share) %}

            -- set alter_share here once you have it figured out
            {% set grant_database %}
                GRANT USAGE ON DATABASE {{target.database}} TO SHARE {{ schema_name }}
            {% endset %}
            {% do run_query(grant_database) %}

            {% set grant_share %}
                GRANT USAGE ON SCHEMA {{ schema_name }} TO SHARE {{ schema_name }}
            {% endset %}
            {% do run_query(grant_share) %}

        {% endif %}
    {% endfor %}

    -- Refresh Views
    {% set views_sql %}
        SELECT 
             LOWER(t.table_schema) AS table_schema
            ,LOWER(t.table_name) AS table_name
        FROM information_schema.tables AS t
        WHERE t.table_schema IN ('REPORTS')
    {% endset %}
    {% set views_results = run_query(views_sql) %}
    {% set table_schemas = views_results.columns[0].values() %}
    {% set table_names = views_results.columns[1].values() %}

    {% for site_id in site_ids %}
        {% set site_id = site_ids[loop.index0] %}
        {% set slug = slugs[loop.index0] %}

        {% for table_schema in table_schemas %}
            {{ log("view_count: ", view_count ) }}

            {% set schema_name = table_schemas[loop.index0] %}
            {% set view_name = table_names[loop.index0] %}

            {% set create_view %}
                CREATE OR REPLACE SECURE VIEW {{ slug }}.{{ view_name }} AS
                SELECT * FROM {{ schema_name }}.{{ view_name }}
                WHERE site_id =  {{ site_id }}
            {% endset %}
            {% do run_query(create_view) %}

            {% if target.database.upper() == 'DATA_WAREHOUSE' %}
                {% set grant_view %}
                    GRANT SELECT ON VIEW {{ slug }}.{{ view_name }} TO SHARE {{ slug }}
                {% endset %}
                {% do run_query(grant_view) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

{% endmacro %}