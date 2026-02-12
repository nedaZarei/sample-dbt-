-- Macro: Get fiscal quarter from calendar date
-- Note: This macro exists but models DUPLICATE the logic inline instead of using it
-- This is an intentional anti-pattern for Artemis to consolidate

{% macro get_fiscal_quarter(date_column) %}
    case
        when extract(month from {{ date_column }}) between 1 and 3 then 'Q3'
        when extract(month from {{ date_column }}) between 4 and 6 then 'Q4'
        when extract(month from {{ date_column }}) between 7 and 9 then 'Q1'
        when extract(month from {{ date_column }}) between 10 and 12 then 'Q2'
    end
{% endmacro %}


-- Macro: Get fiscal year from calendar date
{% macro get_fiscal_year(date_column) %}
    case
        when extract(month from {{ date_column }}) >= 7
        then extract(year from {{ date_column }}) + 1
        else extract(year from {{ date_column }})
    end
{% endmacro %}


-- Macro: Snowflake-specific date difference in days
-- Translated from PostgreSQL: uses Snowflake DATEDIFF function
{% macro date_diff_days(start_date, end_date) %}
    DATEDIFF('day', {{ start_date }}, {{ end_date }})
{% endmacro %}
