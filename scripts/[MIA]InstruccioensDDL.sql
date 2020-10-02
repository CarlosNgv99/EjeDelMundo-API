
create database p2db;

use p2db;
CREATE TABLE data_center (
	company_name	text,
	company_contact	text,
	company_email	text,
	company_phone_number	text,
	person_type	char,
	person_name	text,
	person_email	text,
	person_phone_number	text,
	person_registration_date date,
	person_address	text,
	person_city	text,
	person_postal_code	int,
	person_region	text,
	product_name	text,
	product_category	text,
	product_qty	int,
	product_unit_price	decimal(10,2)
);


use p2db;
create table address (
	address_id int not null auto_increment,
    address_description text,
    address_city text,
    address_postal_code int,
    address_region text,
    primary key (address_id)
);

use p2db;
create table company ( 
	company_id int not null auto_increment,
    company_name text,
    company_contact text,
    company_email text,
    company_phone_number text,
	primary key (company_id)
);

use p2db;
create table clientp (
	client_id int not null auto_increment,
    client_name text,
    client_email text,
    client_phone_number text,
    client_registration_date date,
	client_address int,
	constraint fk_client_address foreign key (client_address) references address(address_id),
    primary key (client_id)
);

use p2db;
create table provider (
	provider_id int not null auto_increment,
    provider_name text,
    provider_email text,
    provider_phone_number text,
    provider_registration_date date,
	provider_address int,
	constraint fk_provider_address foreign key (provider_address) references address(address_id),
    primary key (provider_id)
);


use p2db;
create table product_category(
	product_category_id int not null auto_increment,
	category text,
    primary key (product_category_id)
);

use p2db;
create table product ( 
	product_id int not null auto_increment,
    product_name text,
    product_category int,
    product_unit_price decimal(10,2),
    primary key (product_id),
	constraint fk_product_category foreign key (product_category) references product_category(product_category_id)
);



use p2db;
create table purchase_detail (
	purchase_detail_id int not null auto_increment,
	client_id int,
    client_address_id int,
    product_id int,
    company_id int,
    product_qty int,
    primary key (purchase_detail_id),
	constraint fk_company_purchase foreign key (company_id) references company(company_id),
    constraint fk_person_purchase foreign key (client_id) references clientp(client_id),
	constraint fk_product_purchase foreign key (product_id) references product(product_id),
	constraint fk_address_purchase foreign key (client_address_id) references clientp(client_address)
);

use p2db;
create table order_detail (
	order_detail_id int not null auto_increment,
	provider_id int,
    provider_address_id int,
    product_id int,
    company_id int,
    product_qty int,
    primary key (order_detail_id),
	constraint fk_company_order foreign key (company_id) references company(company_id),
    constraint fk_person_order foreign key (provider_id) references provider(provider_id),
	constraint fk_product_order foreign key (product_id) references product(product_id),
	constraint fk_address_order foreign key (provider_address_id) references provider(provider_address)
);




use p2db;
drop table order_detail;
drop table purchase_detail;
drop table product;
drop table clientp;
drop table provider;
drop table address;
drop table company;
drop table product_category;

drop table data_center;





select * from p2db.address;