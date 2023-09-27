create or replace function get_reward(in_customer_id int, max_churn numeric, max_dis_share numeric, margin_per numeric)
    returns table(Group_Name varchar, Offer_Discount_Depth int)
as
$$
declare
    len     int = (select count(*)
                       from v_groups
                       where customer_id = in_customer_id);
    r       record;
    margin  numeric;
begin
    for i in 1..len
        loop
            select group_margin, group_minimum_discount, group_id
            from v_groups
            where customer_id = in_customer_id
              and group_churn_rate <= max_churn
              and group_discount_share < max_dis_share
            order by group_affinity_index desc, group_id
            limit 1 offset i - 1
            into r;
            if r.group_margin is not null and r.group_minimum_discount is not null and r.group_margin > 0 then
                margin = r.group_margin * margin_per;
                if margin > ceil(r.group_minimum_discount / 0.05) * 0.05 * r.group_margin then
--                     RAISE NOTICE 'Approved: % % % %', in_customer_id, r.group_id, margin, ceil(r.group_minimum_discount / 0.05) * 0.05;
                    return query (select sku_group.group_name as aaa,
                    round(ceil(r.group_minimum_discount / 0.05) * 0.05 * 100)::int as Offer_Discount_Depth from sku_group where group_id = r.group_id);
                end if;
            end if;
        end loop;
end
$$ language plpgsql;

create or replace function offers_to_increase_frequency_of_visits
    (start date, finish date, trans_num int, max_churn numeric, max_dis_share numeric, margin_per numeric)
    returns table (Customer_ID int, Start_Date date, End_Date date,
    Required_Transactions_Count numeric, Group_Name varchar, Offer_Discount_Depth int)
as
$$
    begin
        return query (
        select  vc.customer_id as Customer_ID,
                start as Start_Date,
                finish as End_Date,
                case when customer_frequency = 0 then 0 else
                round((finish - start) / customer_frequency) end
                    + trans_num as Required_Transactions_Count,
                gr.aaa,
                gr.Offer_Discount_Depth
--                 get_reward(vc.customer_id, max_churn, max_dis_share / 100, margin_per / 100)
        from v_customers_view vc
        JOIN LATERAL (
        SELECT * FROM get_reward(vc.customer_id, max_churn, max_dis_share / 100, margin_per / 100)
        ) AS gr(aaa, Offer_Discount_Depth) ON TRUE
        group by vc.customer_id, start, finish, customer_frequency, trans_num,
                 max_churn, max_dis_share, gr.aaa, gr.Offer_Discount_Depth);
    end
$$ language plpgsql;

select * from offers_to_increase_frequency_of_visits('2022-08-18 00:00:00'::date, '2022-08-18 00:00:00'::date,
    1, 3, 70, 30);