-- Macro: Calculate simple return
-- Note: This macro exists but is NOT used in models (intentional anti-pattern)
-- Models duplicate the return calculation inline instead of using this macro

{% macro calculate_return(current_value, previous_value) %}
    case
        when {{ previous_value }} is not null and {{ previous_value }} != 0
        then ({{ current_value }} - {{ previous_value }}) / {{ previous_value }}
        else null
    end
{% endmacro %}


-- Macro: Calculate TVPI (Total Value to Paid-In)
{% macro calculate_tvpi(distributions, nav, paid_in) %}
    case
        when {{ paid_in }} > 0
        then ({{ distributions }} + {{ nav }}) / {{ paid_in }}
        else null
    end
{% endmacro %}


-- Macro: Calculate DPI (Distributions to Paid-In)
{% macro calculate_dpi(distributions, paid_in) %}
    case
        when {{ paid_in }} > 0
        then {{ distributions }} / {{ paid_in }}
        else null
    end
{% endmacro %}
