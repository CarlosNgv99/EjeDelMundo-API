const mysql = require('mysql')

const pool = mysql.createPool({
  connectionLimit: 10,
  password: '!Juniode7',
  user: 'root',
  database: 'p1db',
  host: 'localhost',
  port: '3306'
});


let p1db = {};

p1db.getData = () => {
  return new Promise((resolve, reject) => {
    pool.query('SELECT * FROM data_center;', (err, res) => {
      if(err){
        reject(err);
      }
      return resolve(res);
    });
  });
}

p1db.setModel = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
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
    
    create table person (
      person_id int not null auto_increment,
        person_type char,
        person_name text,
        person_email text,
        person_phone_number text,
        person_registration_date date,
        person_address text,
        person_city text,
        person_postal_code int,
        person_region text,
        primary key (person_id)
    );

  create table company ( 
    company_id int not null auto_increment,
      company_name text,
      company_contact text,
      company_email text,
      company_phone_number text,
    primary key (company_id)
  );


  create table product ( 
    product_id int not null auto_increment,
      product_name text,
      product_category text,
      product_unit_price decimal(10,2),
      primary key (product_id)
  );

  create table disposition (
    disposition_id int not null auto_increment,
    person_id int,
      product_id int,
      company_id int,
      product_qty int,
      primary key (disposition_id),
    constraint fk_company_order foreign key (company_id) references company(company_id),
      constraint fk_person_order foreign key (person_id) references person(person_id),
    constraint fk_product_order foreign key (product_id) references product(product_id)
  );
    `, (err, res) => {
      if(err) {
        reject(err);
      } 
      return resolve(res);
    });
  });
}

p1db.setData = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    LOAD DATA INFILE '/var/lib/mysql-files/DataCenterData.csv' 
    INTO TABLE data_center
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (company_name, company_contact, company_email, company_phone_number, person_type, person_name, person_email, person_phone_number, 
    @person_registration_date, person_address, person_city, person_postal_code, person_region, 
    product_name, product_category, product_qty, product_unit_price)
    SET person_registration_date = STR_TO_DATE(@person_registration_date,'%d/%m/%Y');


    -- filling company table
    insert into p1db.company (company_name, company_contact ,company_email, company_phone_number) 
    select distinct dc.company_name, dc.company_contact,dc.company_email, dc.company_phone_number from p1db.data_center dc;

    -- filling person table
    insert into p1db.person (person_type, person_name, person_email, person_phone_number, person_registration_date, person_address, person_city,
    person_postal_code, person_region) 
    select distinct dc.person_type,dc.person_name, dc.person_email, dc.person_phone_number, dc.person_registration_date, dc.person_address, dc.person_city,
    dc.person_postal_code, dc.person_region from data_center dc;


    -- filling product table
    insert into p1db.product (product_name, product_category, product_unit_price) 
    select product_name, product_category, product_unit_price 
    from p1db.data_center 
    group by product_name, product_category, product_unit_price;


    -- Getting id

    -- gets person, qty correct
    select person_name,product_name, product_category, product_unit_price, product_qty 
    from p1db.data_center 
    group by person_name,product_name, product_category, product_unit_price,product_qty;

    -- Helps filling fk table (Disposition)

    insert into p1db.disposition (person_id, product_id, company_id, product_qty)
    SELECT distinct p.person_id, pr.product_id, c.company_id, dc.product_qty FROM p1db.data_center dc
    JOIN p1db.person p ON (p.person_name = dc.person_name)
    JOIN p1db.product pr ON (pr.product_name = dc.product_name)
    JOIN p1db.company c on (c.company_name = dc.company_name); 
        

    `, (err, res) => {
      if(err) {
        reject(err);
      } 
      let result = {
        message: 'Data loaded into data_center table.'
      }
      return result;
    });
  });
}

p1db.dropTemporal = () => {
  return new Promise((resolve, reject) => {
    pool.query('DROP TABLE IF EXISTS data_center;', (err, res) => {
      if(err) {
        reject(err)
      }
      let result = {
        message: 'Table successfully dropped.'
      }
      return resolve(result);
    });
  });
}

p1db.query1 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT per.person_name, c.company_name, per.person_phone_number,
    SUM(d.product_qty * p.product_unit_price) as total
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    JOIN p1db.company c on c.company_id = d.company_id
    WHERE per.person_type = 'P' and per.person_id = d.person_id and c.company_id = d.company_id
    GROUP BY per.person_name, c.company_name, per.person_phone_number
    order by total desc limit 1;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}


p1db.query2 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT per.person_id, per.person_name,
    SUM(d.product_qty * p.product_unit_price) as total,
    Count(per.person_id) as ocurrences
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    JOIN p1db.company c on c.company_id = d.company_id
    WHERE per.person_type = 'C' and per.person_id = d.person_id 
    GROUP BY per.person_id, per.person_name
    ORDER BY total DESC limit 1;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

module.exports = p1db;