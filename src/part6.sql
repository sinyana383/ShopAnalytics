create or replace function margin_cross_sell(count_of_group int, max_churn_rate numeric, max_stability_index numeric,
                                  max_rate_SKU numeric, max_margin_rate numeric)
    RETURNS TABLE
            (customer_id_ int, sku_name_ varchar, offer_discount_depth numeric)
as
$$
begin
    return query
        (with a as (select customer_id, group_id,
                           row_number() over (partition by customer_id) as row_number,
                           group_minimum_discount
                    from v_groups
                    where group_churn_rate <= max_churn_rate
                      and group_stability_index < max_stability_index
                    order by group_affinity_index desc),
              b as (select customer_id, group_id, group_minimum_discount
                    from a
                    where row_number < count_of_group),
              c as (select distinct customer_id, b.group_id, pg.sku_id, sku_name,
                                    sku_retail_price - sku_purchase_price AS delta,
                                    sku_retail_price, customer_primary_store, group_minimum_discount
                    from b
                             join (select customer_id as ci, customer_primary_store from v_customers_view) as tt
                                  on ci = customer_id
                             join product_grid pg on pg.group_id = b.group_id
                             join stores s on pg.sku_id = s.sku_id),
              d as (select customer_id, group_id, sku_id, sku_name, delta, customer_primary_store,
                           group_minimum_discount, sku_retail_price,
                           rank()
                           over (partition by customer_id,group_id,customer_primary_store order by delta desc ) as rank
                    from c),
              e as (select customer_id, group_id, sku_id, sku_name, delta,
                           customer_primary_store, sku_retail_price, group_minimum_discount
                    from d
                    where rank < 2),
              f as (select distinct customer_id, group_id, e.sku_id, sku_name, delta, customer_primary_store,
                                    transaction_id, sku_retail_price, group_minimum_discount,
                                    (c_ts::numeric / c_tg) * 100 as value
                    from e
                             join checks c on e.sku_id = c.sku_id
                             left join (select count(transaction_id) as c_ts,
                                               sku_id                as c_s
                                        from checks
                                        group by sku_id) as ff on e.sku_id = ff.c_s
                             left join (select group_id                 c_g,
                                               count(transaction_id) as c_tg
                                        from checks
                                                 join product_grid p on checks.sku_id = p.sku_id
                                        group by group_id) as fff on group_id = fff.c_g
                    order by 1, 2),
              g as (select customer_id,
                           sku_name,
                           delta * max_margin_rate / sku_retail_price          as tttmp,
                           ceil(group_minimum_discount::numeric * 100 / 5) * 5 as disc
                    from f
                    where value <= max_rate_SKU),
              h as (select distinct customer_id, sku_name, disc
                    from g
                    where tttmp * 100 >= disc)
         select * from h);
end;
$$ language plpgsql;

select *
from margin_cross_sell(5, 3, 0.5, 100, 30);