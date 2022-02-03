--ex2 
with start_studies as ( select user_id
                              , min(flt.date_created) as first_lesson     
                        from finished_lesson_test as flt
                        join lesson_index_test as lit
                            on flt.lesson_id = lit.lesson_id 
                        where lit.profession_name = 'data-analyst'                              
                        group by user_id
                       ) , 
     all_deltas as (    select  EXTRACT(EPOCH FROM (lead(flt.date_created) over 
                                                            (partition by flt.user_id order by flt.date_created ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING )
                                            - flt.date_created))  as delta_seconds 
                                , flt.date_created as lesson_datetime
                                --, flt.lesson_id
                                , lead(flt.lesson_id) over (partition by flt.user_id order by flt.date_created ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) 
                                            as lesson_id  
                                , lead(flt.date_created) over (partition by flt.user_id order by flt.date_created ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) 
                                            as next_lesson_datetime
                                , lit.profession_name 
                                , flt.user_id
                        from finished_lesson_test as flt
                        join lesson_index_test as lit
                            on flt.lesson_id  = lit.lesson_id 
                        join start_studies as ss
                            on flt.user_id = ss.user_id 
                               and ss.first_lesson at time zone 'Europe/Moscow' between '2020-04-01' and '2020-05-01'   
                                -- считаю, что условие по времени (апрель) актуально для Московского времени
                        where lit.profession_name = 'data-analyst'   
                        )
select  round(cast(delta_seconds as numeric), 0) as delta_seconds 
        , lesson_datetime
        , lesson_id                -- исходя из логики задания я поместил сюда id следующего (короткого) урока
        , next_lesson_datetime
        , profession_name
        , user_id        
from all_deltas
where delta_seconds <= 5

;

