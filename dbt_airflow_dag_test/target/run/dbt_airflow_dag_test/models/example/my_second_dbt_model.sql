
  create or replace   view AIRFLOW_DB.AIRFLOW_DBT.my_second_dbt_model
  
   as (
    -- Use the `ref` function to select from other models

select *
from AIRFLOW_DB.AIRFLOW_DBT.my_first_dbt_model
where id = 1
  );

