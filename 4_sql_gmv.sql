/*
Запрос считает по дням, сколько пользователей было привлечено в этот день, общую стоимость их 
заказазов в последующие 360 дней. С учетом того, что пользователи могут становиться неактивными 
и реактивироваться. Также считается количество реактивированных пользователей и стоимость их заказов.
*/





-- основной подзапрос, где одному пользователю соответсвует одна дата
-- (отбросил order_id и success_order_flg, не нашел требований о них в условии задачи)
with main_table as (select user_id
                           --, order_id
                           , cast(to_timestamp(order_time) as date) as date
                           , sum(order_cost) as order_cost
                           --, success_order_flg
                     from BU1a_team.ig_ca_test
                     group by user_id, cast(to_timestamp(order_time) as date)
                    ),
     -- пользователи, у которых была реактивация
     reactivation as (  select mt1.user_id
                             --, mt1.date
                             , Min(mt2.date) as react_date
                             , datediff('day', mt1.date, Min(mt2.date)) - 1 as off_period
                        from main_table as mt1
                        join main_table as mt2
                            on mt1.user_id = mt2.user_id
                                and mt1.date < mt2.date
                        group by mt1.user_id, mt1.date
                        having datediff('day', mt1.date, Min(mt2.date)) >= 90
                        ),
     -- заказы сделанные реактивированными пользователями за 360 дней
      reactivated_orders as (   select distinct s.user_id
                                     , s.react_date
                                     , s.react_orders_360
                                     , dense_rank() over (partition by s.user_id order by s.react_date) as rank
                                from ( select   mt.user_id
                                               , mt.date as react_date
                                               , sum(mt.order_cost) over (partition by mt.user_id, r.react_date
                                                                          order by mt.date range between current row 
                                                                                and interval '360 day' FOLLOWING) as react_orders_360
                                      from main_table as mt
                                      join reactivation as r
                                         on mt.user_id = r.user_id
                                            and mt.date >= r.react_date
                                        ) as s
                            join reactivation as r
                                on s.user_id = r.user_id
                                   and s.react_date = r.react_date
                            ),
     -- вычел из предыдущих этапов реактивации деньги последующих
     reactivated_orders_fix as (select a.user_id
                                       , a.react_date
                                        , a.react_orders_360 - coalesce(b.react_orders_360, 0) as react_orders_360
                                from reactivated_orders as a
                                left join reactivated_orders as b
                                    on a.user_id = b.user_id
                                        and b.rank = a.rank + 1
                                ),
     --дата, когда пришел каждый пользователь
     newbies as (select user_id
                      , min(date) as min_date
                  from main_table
                  group by user_id
                ),
     -- заказы, сделанные новыми пользователями в первые 360 дней (минус реактивированные)
     newbies_orders as (select  mt.user_id
                                , mt.date as min_date
                                , mt.orders_360

                        from (
                                 select mt.user_id
                                      , mt.date
                                      , sum(mt.order_cost) over (partition by mt.user_id order by mt.date range
                                                                between current row and interval '360 day' FOLLOWING) as orders_360
                                 from main_table as mt
                                 left join (select user_id, min(react_date) as react_date from reactivation group by user_id) as r
                                     on mt.user_id = r.user_id
                                 where r.user_id is null
                                        or (r.user_id is not null and mt.date < r.react_date)
                             ) as mt
                        join newbies as n
                            on mt.user_id = n.user_id
                                and mt.date = n.min_date
                        ),
     newbies_agg as   ( select d.date
                               , count( no.user_id) as users_count_new
                               , isnull(sum(no.orders_360), 0) as gmv360d_new
                        from (select distinct date from main_table) as d
                        left join newbies_orders as no
                            on d.date = no.min_date
                        group by d.date
                        ),
     reactivated_agg as (   select d.date
                                   , count(ro.user_id) as users_count_reactivated
                                   , isnull(sum(ro.react_orders_360), 0) as gmv360d_reactivated
                            from (select distinct date from main_table) as d
                            left join reactivated_orders_fix as ro
                                on d.date = ro.react_date
                            group by d.date
                         )
select na.date
       , na.gmv360d_new
       , ra.gmv360d_reactivated
       , na.users_count_new
       , ra.users_count_reactivated
from newbies_agg as na
join reactivated_agg as ra
    on na.date = ra.date



;



