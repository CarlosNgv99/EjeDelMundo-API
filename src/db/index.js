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

    SELECT query2.person_id, query2.person_name, SUM(total_products) as total_purchased FROM
    (SELECT per.person_id, per.person_name, c.company_name, d.product_qty,p.product_name,
    SUM(d.product_qty) as total_products,
    Count(per.person_Name) as person_ocurrences
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    JOIN p1db.company c on c.company_id = d.company_id
    WHERE c.company_id = d.company_id and per.person_type = 'C'
    GROUP BY per.person_id, per.person_name, c.company_name, d.product_qty, p.product_name) as query2
    GROUP BY query2.person_id, query2.person_name
    ORDER BY total_purchased DESC limit 1;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

p1db.query3 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`

    SELECT query1.person_address, query1.person_region, query1.person_city, query1.person_postal_code, query1.orders_made FROM
    (SELECT per.person_address, per.person_region, per.person_city, per.person_postal_code,
    Count(per.person_address) as orders_made
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE per.person_type = 'P'
    GROUP BY per.person_address,per.person_region, per.person_city, per.person_postal_code
    ORDER BY orders_made DESC LIMIT 1) as query1
    UNION
    SELECT query2.person_address, query2.person_region, query2.person_city, query2.person_postal_code, query2.orders_made FROM
    (SELECT per.person_address, per.person_region, per.person_city, per.person_postal_code,
    Count(per.person_address) as orders_made
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE per.person_type = 'P'
    GROUP BY per.person_address,per.person_region, per.person_city, per.person_postal_code
    ORDER BY orders_made ASC LIMIT 1) as query2;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}



p1db.query4 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT per.person_id, per.person_name, p.product_category,
    COUNT(per.person_name) as person_orders,
    SUM(d.product_qty * p.product_unit_price) as total
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE p.product_category = 'Cheese' and per.person_type = 'C'
    GROUP BY per.person_id, per.person_name, p.product_category
    ORDER BY total DESC LIMIT 5;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

p1db.query5 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT query1.month1 , query1.person_name, query1.person_orders FROM
    (SELECT EXTRACT(MONTH FROM per.person_registration_date) as month1, per.person_name,
    COUNT(per.person_name) as person_orders
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE per.person_type = 'C'
    GROUP BY per.person_name, month1
    ORDER BY person_orders DESC LIMIT 5) as query1
    UNION
    SELECT query2.month2, query2.person_name, query2.person_orders FROM
    (SELECT EXTRACT(MONTH FROM per.person_registration_date) as month2, per.person_name,
    COUNT(per.person_name) as person_orders
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE per.person_type = 'C'
    GROUP BY  per.person_name, month2
    ORDER BY person_orders ASC LIMIT 5) as query2;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

p1db.query6 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT query5.product_category, query5.total_amount FROM
    (SELECT query1.product_category, SUM(query1.total) as total_amount FROM
    (SELECT p.product_category, p.product_name, d.product_qty, p.product_unit_price,
    SUM(d.product_qty * p.product_unit_price) as total,
    COUNT(p.product_category) as product_count
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    GROUP BY p.product_category, p.product_name, d.product_qty, p.product_unit_price) as query1
    GROUP BY query1.product_category
    ORDER BY total_amount DESC LIMIT 1) as query5
    UNION
    SELECT query4.product_category, query4.total_amount FROM
    (SELECT query2.product_category, SUM(query2.total) as total_amount FROM
    (SELECT p.product_category, p.product_name, d.product_qty, p.product_unit_price,
    SUM(d.product_qty * p.product_unit_price) as total,
    COUNT(p.product_category) as product_count
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    GROUP BY p.product_category, p.product_name, d.product_qty, p.product_unit_price) as query2
    GROUP BY query2.product_category
    ORDER BY total_amount ASC LIMIT 1) as query4;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}


p1db.query7 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT per.person_id, per.person_name, p.product_category,
    COUNT(per.person_name) as person_orders,
    SUM(d.product_qty * p.product_unit_price) as total
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE p.product_category = 'Fresh Vegetables' and per.person_type = 'P'
    GROUP BY per.person_id, per.person_name, p.product_category
    ORDER BY total DESC LIMIT 5;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

p1db.query10 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT per.person_name, p.product_category,
    COUNT(per.person_name) as person_ocurrences,
    SUM(d.product_qty) as total_products
    FROM p1db.disposition d
    JOIN p1db.product p ON p.product_id = d.product_id
    JOIN p1db.person per on per.person_id = d.person_id
    WHERE p.product_category = 'Seafood' and per.person_type = 'C'
    GROUP BY per.person_name, p.product_category
    ORDER BY total_products DESC LIMIT 10;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

module.exports = p1db;