{% macro calculate_mrr(total_amount_col, term_months_col) %}
    cast(
        round(
            ({{ total_amount_col }} / nullif({{ term_months_col }}, 0)) 
        , 2) 
    as numeric(16,2))
{% endmacro %}