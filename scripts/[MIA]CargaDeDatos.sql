LOAD DATA INFILE '/var/lib/mysql-files/DataCenterData.csv' 
INTO TABLE p2db.data_center
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(company_name, company_contact, company_email, company_phone_number, person_type, person_name, person_email, person_phone_number, 
@person_registration_date, person_address, person_city, person_postal_code, person_region, 
product_name, product_category, product_qty, product_unit_price)
SET person_registration_date = STR_TO_DATE(@person_registration_date,'%d/%m/%Y');

-- filling company table
insert into p2db.company (company_name, company_contact ,company_email, company_phone_number) 
select distinct dc.company_name, dc.company_contact,dc.company_email, dc.company_phone_number from p2db.data_center dc;

-- filling address table
insert into p2db.address (address_description, address_city ,address_postal_code, address_region) 
select distinct dc.person_address, dc.person_city,dc.person_postal_code, dc.person_region from p2db.data_center dc;

-- filling product category table
insert into p2db.product_category (category) select distinct product_category from p2db.data_center;

-- filling product table 
insert into p2db.product (product_name, product_category, product_unit_price)
select distinct product_name, pc.product_category_id, product_unit_price from p2db.data_center dc
join p2db.product_category pc where (pc.category = dc.product_category);

-- filling provider table
insert into p2db.provider (provider_name, provider_email, provider_phone_number, provider_registration_date, provider_address) 
select distinct dc.person_name, dc.person_email, dc.person_phone_number, dc.person_registration_date, a.address_id from p2db.data_center dc
join p2db.address a where a.address_description = dc.person_address and dc.person_type = 'P';

-- filling provider table
insert into p2db.clientp (client_name, client_email, client_phone_number, client_registration_date, client_address) 
select distinct dc.person_name, dc.person_email, dc.person_phone_number, dc.person_registration_date, a.address_id from p2db.data_center dc
join p2db.address a where a.address_description = dc.person_address and dc.person_type = 'C';

-- filling purchase_detail table
insert into p2db.purchase_detail (client_id, client_address_id, product_id, company_id, product_qty)
select distinct c.client_id, c.client_address, p.product_id, co.company_id, dc.product_qty from p2db.data_center dc
join p2db.clientp c on (c.client_name = dc.person_name)
join p2db.product p on (p.product_name = dc.product_name)
join p2db.company co on (co.company_name = dc.company_name);

-- filling order_detail table
insert into p2db.order_detail (provider_id, provider_address_id, product_id, company_id, product_qty)
select distinct pr.provider_id, pr.provider_address, p.product_id, co.company_id, dc.product_qty from p2db.data_center dc
join p2db.provider pr on (pr.provider_name = dc.person_name)
join p2db.product p on (p.product_name = dc.product_name)
join p2db.company co on (co.company_name = dc.company_name);
