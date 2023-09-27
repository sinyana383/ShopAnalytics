create table personal_information(
    Customer_ID int primary key,
    Customer_Name varchar(100)
        check ( Customer_Name ~  '^[A-ZА-Я][a-zа-я\\-\\s*]*$'),
    Customer_Surname varchar (100)
        check ( Customer_Surname ~  '^[A-ZА-Я][a-zа-я\\-\\s*]*$'),
    Customer_Primary_Email varchar (100)
        check ( Customer_Primary_Email ~ '^([a-z0-9\._-]+)@([a-z0-9\._-]+)\.([a-z\.]{2,})$' ),
    Customer_Primary_Phone varchar (12)
        check ( Customer_Primary_Phone ~ '^\+7[0-9]{10}$')
);

create table cards
(
    Customer_Card_ID int primary key,
    Customer_ID      int,
    constraint fk_cards_Customer_ID foreign key (Customer_ID) references personal_information(Customer_ID)
);

create table sku_group (
    Group_ID int primary key,
    Group_Name varchar(100)
        check (Group_Name ~ '^[A-Za-zА-Яа-я0-9\.\-\+\\\@\#\$]+$')
);

create table stores (
    Transaction_Store_ID int,
    SKU_ID int,
    SKU_Purchase_Price decimal,
    SKU_Retail_Price decimal,
    constraint fk_stores_SKU_ID foreign key (SKU_ID) references SKU_group(Group_ID)
);

create table transactions(
    Transaction_ID int primary key,
    Customer_Card_ID int,
    Transaction_Summ decimal,
    Transaction_DateTime timestamp(0),
    Transaction_Store_ID int,
    constraint fk_transactions_Customer_Card_ID foreign key (Customer_Card_ID) references cards(Customer_Card_ID)
);

create table checks(
    Transaction_ID int,
    SKU_ID int,
    SKU_Amount decimal,
    SKU_Summ decimal,
    SKU_Summ_Paid decimal,
    SKU_Discount decimal,
    constraint fk_checks_SKU_ID foreign key (SKU_ID) references SKU_group(Group_ID),
    constraint fk_checks_Transaction_ID foreign key (Transaction_ID) references transactions(Transaction_ID)
);

create table product_grid(
    SKU_ID int primary key,
    SKU_name varchar(100)
        check (SKU_name ~ '^[A-Za-zА-Яа-я0-9\ \.\-\+\\\@\#\$]+$'),
    Group_ID int,
    constraint fk_product_grid_SKU_ID foreign key (SKU_ID) references SKU_group(Group_ID)
);


create table date_analysis (
    Analysis_Formation timestamp(0)
);

create or replace procedure import_from_tsv ()
as $$
declare
	import_path varchar := '/Users/ddurrand/Desktop/retail/src/import_mini/';
	import_name_tsv varchar[] := array ['Personal_Data_Mini', 'Cards_Mini', 'Groups_SKU_Mini', 'Stores_Mini', 'Transactions_Mini', 'Checks_Mini', 'SKU_Mini', 'Date_Of_Analysis_Formation'];
-- 	import_name_tsv varchar[] := array ['Personal_Data', 'Cards', 'Groups_SKU', 'Stores', 'Transactions', 'Checks', 'SKU', 'Date_Of_Analysis_Formation'];
	import_name_table varchar[] := array ['personal_information', 'cards', 'sku_group', 'stores', 'transactions', 'checks', 'product_grid', 'date_analysis'];
begin
    execute 'SET datestyle TO European';
	for i in 1..array_length(import_name_tsv, 1)
		loop
			execute format ('copy %I from %L with delimiter ''%s''', import_name_table[i], import_path || import_name_tsv[i] || '.tsv', E'\t');
		end loop;
end
$$
language plpgsql;

call import_from_tsv();

create or replace procedure export_to_tsv ()
as $$
declare
	export_path varchar := '/Users/ddurrand/Desktop/retail/src/export/';
	export_name_table varchar[] := array ['personal_information', 'cards', 'sku_group', 'stores', 'transactions', 'checks', 'product_grid', 'date_analysis'];
begin
	for i in 1..array_length(export_name_table, 1)
		loop
	        execute format ('copy %I to %L with delimiter ''%s''', export_name_table[i], export_path || export_name_table[i] || '.tsv', E'\t');
		end loop;
end;
$$
	language plpgsql;

call export_to_tsv();