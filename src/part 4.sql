---------------------расчет среднего чека по методу 1----------------------

create or replace function calc_avg_receipt_1(id int, start_time timestamp, end_time timestamp,
                                                       average_check numeric) returns numeric
as
$$
declare
    start_date    timestamp = (select min(transaction_datetime) from transactions t
                                join cards c on t.customer_card_id = c.customer_card_id
                            where customer_id = id);
    date_analysis timestamp = (select analysis_formation from date_analysis);
    rez           numeric;
begin
    if start_time < start_date then
        start_time = start_date;
    end if;
    if end_time > date_analysis then
        end_time = date_analysis;
    end if;
    rez = (select sum(transaction_summ) / count(transaction_id) from transactions t
                join cards c on t.customer_card_id = c.customer_card_id
            where customer_id = id
                and transaction_datetime between start_time and end_time) * average_check;
    return rez;
end;
$$ language plpgsql;

select * from calc_avg_receipt_1(1, '2021-01-21 13:10:46', '2022-08-25 13:10:46', 2);

---------------------------расчет среднего чека по методу 2-------------------------------

create or replace function calc_avg_receipt_2(id int, count_tran int, average_check numeric) returns numeric
as
$$
declare
    rez numeric;
begin
    create temp table tran as
        select t.* , c.customer_id AS card_customer_id from transactions t
            join cards c on c.customer_card_id = t.customer_card_id
        where c.customer_id = id
        order by t.transaction_datetime desc
        limit count_tran;
    rez = (select sum(transaction_summ) / count(transaction_id) from tran) * average_check;
    drop table tran;
    return rez;
end;
$$language plpgsql;

select * from calc_avg_receipt_2(1, 5, 3);

--------------------------discount-------------------------

create or replace function discount (id int, churn_index numeric, dis_rate numeric, margin numeric) returns numeric
as
$$
declare
    i int = 1;
    r record;
    n_margin numeric;
begin
    for i in 1..(select count(*) from v_groups where customer_id = id)
        loop
            select group_margin, group_minimum_discount, group_id
            from v_groups
            where customer_id = id and group_churn_rate <= churn_index
                        and group_discount_share < dis_rate
            order by group_affinity_index desc, group_id
            limit 1 offset i - 1
            into r;
            if r.group_margin is not null and r.group_minimum_discount is not null then
                n_margin = r.group_margin * margin;
                if margin > ceil(r.group_minimum_discount / 0.05) * 0.05 * r.group_margin then
                    return (ceil(r.group_minimum_discount * 100 / 5) * 5);
                end if;
            end if;
        end loop;
    return 0;
end;
$$ language plpgsql;

select * from discount(1, 1, 3, 30);

-------------------------group_name-----------------

create or replace function group_name (id int, churn_index numeric, dis_rate numeric, margin numeric) returns varchar
as
$$
declare
    i int = 1;
    r record;
    n_margin numeric;
begin
    for i in 1..(select count(*) from v_groups where customer_id = id)
        loop
            select group_margin, group_minimum_discount, group_id from v_groups
            where customer_id = id and group_churn_rate <= churn_index
                        and group_discount_share < dis_rate
            order by group_affinity_index desc, group_id
            limit 1 offset i - 1
            into r;
            if r.group_margin is not null and r.group_minimum_discount is not null then
                n_margin = r.group_margin * margin;
                if margin > ceil(r.group_minimum_discount / 0.05) * 0.05 * r.group_margin then
                    return (select group_name from sku_group where group_id = r.group_id);
                end if;
            end if;
        end loop;
    return 0;
end;
$$ language plpgsql;

select * from group_name(1, 3, 0.7, 0.3);

--------------------------------get_merge--------------------------------

create or replace function get_merge (met_average_check integer,
                                            first_date_p date,
                                            last_date_p date,
                                            amount_tr integer,
                                            c_aver_check numeric,
                                            max_churn_rate numeric,
                                            max_share_tr numeric,
                                            ad_share_mar numeric)
returns table (customer_id            int,
                Required_Check_Measure numeric,
                Group_Name             varchar,
                Offer_Discount_Depth   numeric
              )
as
$$
declare
    first_date timestamp = (select first_date_p::timestamp);
    last_date timestamp = (select last_date_p::timestamp);
begin
    if met_average_check = 1 then
        return query (select pi.customer_id,
                             calc_avg_receipt_1(pi.customer_id, first_date, last_date, c_aver_check),
                             group_name(pi.customer_id, max_churn_rate, max_share_tr::numeric / 100, ad_share_mar::numeric / 100),
                             discount(pi.customer_id, max_churn_rate, max_share_tr::numeric / 100,ad_share_mar::numeric / 100)
                      from personal_information pi);
        elseif met_average_check = 2 then
            return query (select pi.customer_id,
                                calc_avg_receipt_2(pi.customer_id, amount_tr, c_aver_check),
                                group_name(pi.customer_id, max_churn_rate, max_share_tr::numeric / 100, ad_share_mar::numeric / 100),
                                discount(pi.customer_id, max_churn_rate, max_share_tr::numeric / 100,ad_share_mar::numeric / 100)
                      from personal_information pi);
    end if;
end;
$$ language plpgsql;

SELECT *
FROM get_merge(2, '2021-01-21', '2022-08-25', 100, 1.15, 3, 70, 30);