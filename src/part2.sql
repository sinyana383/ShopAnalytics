---------------------------- Customer ----------------------------

create or replace function get_card_by_id(id int)
    returns table (card_id int)
as
$$
    begin
        return query
        select customer_card_id from cards
        where customer_id = id;
    end;
$$ language plpgsql;

-- select * from get_card_by_id(1);

-------------------------------2222222222-------------------------

create or replace function  get_customer_average_check (id int)
    returns numeric
as
$$
    declare
        res numeric;
    begin
        res = (select avg(transaction_summ) from transactions
                    where customer_card_id in (select get_card_by_id(id)));
        if res is not null then
            return res;
        end if;
        return 0;
    end;
$$ language plpgsql;

-- select * from get_customer_average_check(1);

---------------------------------333333333-------------------------

create or replace function get_customer_average_check_segment(id int)
returns varchar as
$$
declare
    total_count int = (select count(Customer_ID) from personal_information);
begin

    if id in (select Customer_ID from personal_information order by get_customer_average_check(Customer_ID) desc limit ceil(total_count * 0.10)) then
        return 'High';
    elseif id in (select Customer_ID from personal_information order by get_customer_average_check(Customer_ID) desc limit ceil(total_count * 0.25) offset ceil(total_count * 0.10)) then
        return 'Medium';
    else
        return 'Low';
    end if;
end;
$$
language plpgsql;

-- select * from get_customer_average_check_segment(3);

--------------------------444444444---------------------------

create or replace function get_customer_frequency(id int) returns numeric
as
$$
    declare
        min_date date = (select min(transaction_datetime)
                                from cards c
                                     join transactions t on c.customer_card_id = t.customer_card_id
                                where c.customer_id = id);
        max_date date = (select max(transaction_datetime)
                                from cards c
                                     join transactions t on c.customer_card_id = t.customer_card_id
                                where c.customer_id = id);
        total_transaction_count int = (select count(transaction_id)
                                    from transactions
                                    where customer_card_id in (select * from get_card_by_id(id)));
        frequency numeric;
    begin
        if total_transaction_count != 0 then
            frequency = (max_date - min_date)::numeric / total_transaction_count;
            if frequency is not null then
                return frequency;
            end if;
        end if;
    return 0;
    end;
$$
language plpgsql;

-- select * from get_customer_frequency(1);
-- select * from transactions where customer_card_id = 4;

------------------------------555555555------------------------------

create or replace function get_customer_frequency_segment(id int) returns varchar
as
$$
    declare
        total_count int = (select count(Customer_ID) from personal_information);
    begin
        if id in (select Customer_ID from personal_information order by get_customer_frequency(Customer_ID) desc limit ceil(total_count * 0.10)) then
        return 'Often';
    elseif id in (select Customer_ID from personal_information order by get_customer_frequency(Customer_ID) desc limit ceil(total_count * 0.25) offset ceil(total_count * 0.10)) then
        return 'Occasionally';
    else
        return 'Rarely';
    end if;
    end;
$$ language plpgsql;

--------------------------------666666666-----------------------------------

create or replace function get_customer_inactive_period(id int) returns numeric
as
$$
    declare
        last_date timestamp = (select transaction_datetime from transactions
                                    where customer_card_id in (select get_card_by_id(id))
                                    and transaction_datetime < (select * from date_analysis)
                                    order by 1 desc
                                    limit 1);
    begin
        return
            (extract(epoch from (select * from date_analysis)) - extract(epoch from last_date)) / (60 * 60 * 24)::numeric;
    end;
$$ language plpgsql;

-- select * from get_customer_inactive_period(1);

-----------------------------77777777-------------------------

create or replace function get_customer_churn_rate(id int) returns numeric
as
$$BEGIN
    return (get_customer_inactive_period(id) / get_customer_frequency(id));
end;
$$ language plpgsql;

------------------------------88888888------------------------

create or replace function get_Customer_Churn_Segment(id int) returns varchar
as
$$
    begin
        if (get_customer_inactive_period(id) / get_customer_frequency(id)) <= 2 then return 'Low';
        elseif (get_customer_inactive_period(id) / get_customer_frequency(id)) > 2 and (get_customer_inactive_period(id) / get_customer_frequency(id)) <= 5 then return 'Medium';
        else return 'High';
        end if;
    end;
$$ language plpgsql;

------------------------------999999999-----------------------

-- select * from generate_customer_segment('High', 'Often', 'Low');

create or replace function generate_customer_segment(average_check varchar, frequency varchar, churn varchar) returns numeric
as
$$
    declare
        arr_average_check varchar[] = array ['Low', 'Medium', 'High'];
        arr_frequency varchar[] = array ['Rarely','Occasionally','Often'];
        arr_churn varchar[] = array ['High', 'Medium', 'Low'];
        rez numeric = 1;
        current_average_check varchar;
        current_frequency varchar;
        current_churn varchar;
    begin
        foreach current_average_check in array arr_average_check
        loop
            foreach current_frequency in array arr_frequency
            loop
                foreach  current_churn in array arr_churn
                loop
                    if current_average_check = average_check
                        and current_frequency = frequency
                        and current_churn = churn
                        then return rez;
                    end if;
                    rez = rez + 1;
                end loop;
            end loop;
        end loop;
    end;
$$ language plpgsql;

--

create or replace function get_customer_segment(id int) returns numeric
as
$$
    begin
        return generate_customer_segment(
            get_customer_average_check_segment(id),
            get_customer_frequency_segment (id),
            get_customer_churn_segment (id)
            );
    end;
$$ language plpgsql;

-- select * from get_customer_segment(1);

-------------------------------101010101010-------------

-- Перечень магазинов клиента:

select customer_id, transaction_store_id,
       count(tr_share) over (partition by customer_id, tr_share) as count_tr_share
from (select distinct personal_information.customer_id, t.transaction_store_id,
            (count(t.transaction_id) over (partition by transaction_store_id) /
            nullif(count(transaction_id) over (partition by personal_information.customer_id), 0)) as tr_share
      from personal_information
            left join cards c on personal_information.customer_id = c.customer_id
            left join transactions t on c.customer_card_id = t.customer_card_id
      where transaction_datetime < (select analysis_formation from date_analysis)
      order by 1, 2) qwe;

-- Последние три транзакции:

select customer_id, transaction_datetime, transaction_store_id, tmp,
       (count(transaction_store_id) over (partition by customer_id,transaction_store_id)) as uniq_st
from
(select distinct personal_information.customer_id, t.transaction_datetime as transaction_datetime, transaction_store_id,
        (row_number() over (partition by personal_information.customer_id order by t.transaction_datetime desc)) as tmp
    from personal_information
        left join cards c on personal_information.customer_id = c.customer_id
        left join transactions t on c.customer_card_id = t.customer_card_id
    where transaction_datetime < (select analysis_formation from date_analysis)
    order by 1, 4 desc) as t
where tmp <= 3;

--Последний столбец

create or replace function get_customer_primary_store(id int) returns numeric
as
$$
declare
    rez numeric = 0;
begin
    create temp table customer_store_list as
        (select customer_id, transaction_store_id, tr_share,
            count(tr_share) over (partition by customer_id, tr_share) as count_tr_share
        from (select distinct personal_information.customer_id, t.transaction_store_id,
                    (count(t.transaction_id) over (partition by transaction_store_id) /
                    nullif(count(transaction_id) over (partition by personal_information.customer_id), 0)) as tr_share
            from personal_information
                left join cards c on personal_information.customer_id = c.customer_id
                left join transactions t on c.customer_card_id = t.customer_card_id
            where transaction_datetime < (select analysis_formation from date_analysis)
            order by 1, 2) qwe);
    create temp table last_three_tranz as
        (select customer_id, transaction_datetime, transaction_store_id, tmp,
            (count(transaction_store_id) over (partition by customer_id,transaction_store_id)) as uniq_st
        from (select distinct personal_information.customer_id, t.transaction_datetime as transaction_datetime, transaction_store_id,
            (row_number() over (partition by personal_information.customer_id order by t.transaction_datetime desc)) as tmp
                from personal_information
                    left join cards c on personal_information.customer_id = c.customer_id
                    left join transactions t on c.customer_card_id = t.customer_card_id
                where transaction_datetime < (select analysis_formation from date_analysis)
                order by 1, 4 desc) as t
        where tmp <= 3);
    rez = (select case
                when max(uniq_st) != 3
                    then (select case
                        when max(count_tr_share) > 1
                            then (select transaction_store_id from last_three_tranz
                            where tmp = 1 and customer_id = id)
                    else (select transaction_store_id from customer_store_list
                            where customer_id = id
                            order by tr_share desc
                            limit 1) end
                    from customer_store_list
                    where customer_id = id)
                else min(uniq_st) end
           from last_three_tranz
           where customer_id = id);
    drop table last_three_tranz;
    drop table customer_store_list;
    return rez;
end;
$$ language plpgsql;

-- select * from get_customer_primary_store(1);



------------------Итоговая таблица v_customers_view----------------------
create or replace view v_customers_view as
select
    Customer_ID,
    get_customer_average_check(Customer_ID) as Customer_Average_Check,
    get_customer_average_check_segment(Customer_ID) as Customer_Average_Check_Segment,
    get_customer_frequency(Customer_ID) as Customer_Frequency,
    get_customer_frequency_segment (Customer_id) as Customer_Frequency_Segment,
    get_customer_inactive_period (Customer_id) as Customer_Inactive_Period,
    get_customer_churn_rate(Customer_id) as Customer_Churn_Rate,
    get_customer_churn_segment(Customer_id) as Customer_Churn_Segment,
    get_customer_segment(customer_id) as Customer_Segment,
    get_customer_primary_store (customer_id) as Customer_Primary_Store
from personal_information;


-- select * from v_customers_view order by 1;
--
-- drop view if exists v_customers_view;


---------------------------- Purchase History ----------------------------

create materialized view v_purchase_history as
select Customer_ID,
       transactions.Transaction_ID,
       Transaction_DateTime,
       pg.group_id as Group_ID,
       sum(SKU_Purchase_Price * SKU_Amount) as Group_Cost,
       sum(SKU_Summ) as Group_Summ,
       sum(SKU_Summ_Paid) as Group_Summ_Paid
from transactions
join cards c on c.customer_card_id = transactions.customer_card_id
join checks c2 on transactions.transaction_id = c2.transaction_id
join product_grid pg on c2.sku_id = pg.sku_id
join stores s on
    transactions.transaction_store_id = s.transaction_store_id and
    pg.sku_id = s.sku_id
group by Customer_ID, transactions.Transaction_ID, pg.group_id,
         SKU_Purchase_Price, SKU_Amount;
create index idx_v_purchase_history on v_purchase_history
    (Customer_ID, Transaction_ID, Transaction_DateTime, Group_ID, Group_Cost,
     Group_Summ, Group_Summ_Paid);

---------------------------- Purchase History ----------------------------

create materialized view v_periods as
select Customer_ID,
       group_id,
       min(transaction_datetime) as First_Group_Purchase_Date,
       max(transaction_datetime) as Last_Group_Purchase_Date,
       count(v_ph.transaction_id) as Group_Purchase,
       (extract(day from
           (max(transaction_datetime) - min(transaction_datetime)) + interval '1 day') /
            count(v_ph.transaction_id)) as Group_Frequency,
       coalesce(min(c.SKU_Discount / c.SKU_Summ)
           filter (where c.SKU_Discount / c.SKU_Summ <> 0), 0) as Group_Min_Discount
from v_purchase_history v_ph
join checks c on v_ph.transaction_id = c.transaction_id
group by Customer_ID, group_id;
create index idx_v_periods on v_periods
    (Customer_ID, group_id, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, Group_Min_Discount);

---------------------------- Groups ----------------------------

create or replace function get_group_churn_rate(in_customer_ID int, in_group_ID int)
    returns numeric
as
$$
    declare date_analysis date := (select analysis_formation from date_analysis limit 1);
    declare days_ago numeric;
    declare average numeric;
    begin
        days_ago = extract(epoch from (date_analysis -
              (select max(transaction_datetime) from v_purchase_history vh
                where vh.customer_id = in_customer_ID and vh.group_id = in_group_ID
                   and transaction_datetime <= date_analysis limit 1))) / 86400.0;
        average = (select group_frequency from v_periods vp
                    where vp.customer_id = in_customer_ID and vp.group_id = in_group_ID limit 1);
        return days_ago / average;
    end;
$$ language plpgsql;

create or replace function get_margin
    (in_customer_ID int, in_group_ID int, days_period int default 0, trans_num int default 0)
returns numeric
AS
$$
declare
    margin numeric;
begin
    if days_period > 0 then
        select sum(group_summ_paid - group_cost)
        into margin
        from v_purchase_history
        where customer_id = in_customer_ID and group_id = in_group_ID
        and transaction_datetime >= (SELECT analysis_formation FROM date_analysis) - days_period * interval '1 day';

    elseif trans_num > 0 then
        select sum(group_summ_paid - group_cost)
        into margin
        from (
            select group_summ_paid, group_cost
            from v_purchase_history
            where customer_id = in_customer_ID and group_id = in_group_ID
            order by Transaction_DateTime desc
            limit trans_num
             ) as last_trans;

    else
        select sum(group_summ_paid - group_cost)
        into margin
        from v_purchase_history vh
        where in_customer_ID = customer_id and in_group_ID = group_id;
    end if;

    return margin;
end;
$$
language plpgsql;


create or replace function get_group_discount_share(in_customer_ID int, in_group_ID int)
    returns numeric
as
$$
    declare card_id int[] = ARRAY(select get_card_by_id(in_customer_ID));
    declare group_purchase numeric = (select group_purchase from v_periods vp
                                        where vp.customer_id = in_customer_ID and vp.group_id = in_group_ID limit 1);
    declare discount_share numeric;
    begin
        discount_share =    CASE WHEN group_purchase = 0 THEN 0
                            ELSE (select count(t.transaction_id) from checks c
                            join transactions t on t.transaction_id = c.transaction_id
                            join checks c2 on t.transaction_id = c2.transaction_id
                            join product_grid pg on c.sku_id = pg.sku_id
                            where c.sku_discount > 0 and
                            t.customer_card_id = any(card_id) and pg.group_id = in_group_ID
                            ) / group_purchase END;
        return discount_share;
    end;
$$ language plpgsql;

create or replace function get_group_average_discount(in_customer_ID int, in_group_ID int)
    returns numeric
as
$$
    begin
        RETURN (SELECT sum(vh.group_summ_paid) / sum(vh.group_summ)
            FROM v_purchase_history vh
            join checks c on vh.transaction_id = c.transaction_id
            WHERE vh.customer_id = in_customer_ID AND vh.group_id = in_group_ID
                    and c.sku_discount != 0);
    end;
$$ language plpgsql;

create materialized view v_groups as
with trans_total_num as (
    select vp.customer_id, vp.group_id, group_purchase, count( distinct vh.transaction_id) as total
    from v_purchase_history vh
    join v_periods vp on vh.customer_id = vp.customer_id
    where vh.transaction_datetime between vp.first_group_purchase_date and vp.last_group_purchase_date
    group by vp.customer_id, vp.group_id, group_purchase
),
group_stability_index as(
with deviation as(
    with intervals as(
    SELECT
        customer_id,
        group_id,
        coalesce(extract(epoch from transaction_datetime - lag(transaction_datetime)
            OVER (PARTITION BY customer_id, group_id ORDER BY transaction_datetime)) / 86400.0, 0) AS Diff
    FROM v_purchase_history
    )
    select i.customer_id,
           i.group_id,
           Diff,
           (case when Diff - vp.group_frequency < 0 then (Diff - vp.group_frequency) * -1
               else Diff - vp.group_frequency end) / vp.group_frequency as absolute
    from intervals i
    join v_periods vp on i.customer_id = vp.customer_id and i.group_id = vp.group_id)
select customer_id, group_id, avg(absolute) as total from deviation
GROUP BY customer_id, group_id
)
select c.Customer_ID,
       pg.Group_ID,
       (ttn.group_purchase::numeric / ttn.total::numeric) as Group_Affinity_Index,
       get_group_Churn_Rate(c.Customer_ID,pg.Group_ID) as Group_Churn_Rate,
       gis.total as Group_Stability_Index,
       get_margin(c.Customer_ID,pg.Group_ID, 0, 0) as Group_Margin,
       get_group_discount_share(c.Customer_ID,pg.Group_ID) as Group_Discount_Share,
       (min(group_min_discount) filter ( where group_min_discount <> 0 )) as Group_Minimum_Discount,
       get_group_average_discount(c.Customer_ID,pg.Group_ID) as Group_Average_Discount
from checks ch
join transactions t on t.transaction_id = ch.transaction_id
join cards c on t.customer_card_id = c.customer_card_id
join product_grid pg on ch.sku_id = pg.sku_id
join trans_total_num ttn on c.customer_id = ttn.customer_id and ttn.group_id = pg.group_id
join group_stability_index gis on c.customer_id = gis.customer_id and gis.group_id = pg.group_id
join v_periods v on c.customer_id = v.customer_id and pg.group_id = v.group_id
group by c.Customer_ID, pg.Group_ID, ttn.group_purchase, ttn.total, gis.total;
create index idx_v_groups on v_groups
    (Customer_ID, Group_ID, Group_Churn_Rate, Group_Stability_Index, Group_Margin, Group_Discount_Share,
     Group_Minimum_Discount, Group_Average_Discount);