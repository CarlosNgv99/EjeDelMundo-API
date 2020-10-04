const mysql = require('mysql')

const pool = mysql.createPool({
  connectionLimit: 10,
  password: '!Juniode7',
  user: 'root',
  database: 'p2db',
  host: 'localhost',
  port: '3306',
  multipleStatements: true
});


let p1db = {};

p1db.getData = () => {
  return new Promise((resolve, reject) => {
    pool.query('SELECT * FROM p2db.data_center;', (err, res) => {
      if(err){
        reject(err);
      }
      return resolve(res);
    });
  });
}

p1db.dropTemporal = () => {
  return new Promise((resolve, reject) => {
    pool.query('TRUNCATE TABLE p2db.data_center;', (err, res) => {
      if(err) {
        reject(err)
      }
      let result = {
        message: 'Table successfully truncated.'
      }
      return resolve(result);
    });
  });
}

p1db.dropModel = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    drop table p2db.order_detail;
    drop table p2db.purchase_detail;
    drop table p2db.product;
    drop table p2db.clientp;
    drop table p2db.provider;
    drop table p2db.address;
    drop table p2db.company;
    drop table p2db.product_category;

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      let result = {
        message: 'Tables successfully dropped.'
      }
      return resolve(result);
    });
  });
}

p1db.setModel = () => {
  return new Promise((resolve, reject) => {
    pool.query(`    
      
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
    use p2db;
    CREATE TABLE IF NOT EXISTS data_center (
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

    LOAD DATA INFILE '/var/lib/mysql-files/DataCenterData.csv' 
    INTO TABLE p2db.data_center
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (company_name, company_contact, company_email, company_phone_number, person_type, person_name, person_email, person_phone_number, 
    @person_registration_date, person_address, person_city, person_postal_code, person_region, 
    product_name, product_category, product_qty, product_unit_price)
    SET person_registration_date = STR_TO_DATE(@person_registration_date,'%d/%m/%Y');   

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



p1db.query1 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    select queryaux.provider_name, queryaux.provider_phone_number, queryaux.total from
    (select pr.provider_registration_date,pr.provider_name, pr.provider_phone_number, c.company_name, 
    COUNT(pr.provider_registration_date) as date_ocurrences,
    SUM(od.product_qty * p.product_unit_price) as total
    from p2db.order_detail od 
    join p2db.provider pr on pr.provider_id = od.provider_id
    join p2db.product p on p.product_id = od.product_id
    join p2db.company c on c.company_id = od.company_id
    group by pr.provider_registration_date, pr.provider_name, pr.provider_phone_number, c.company_name
    order by total desc limit 1) as queryaux;

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

    SELECT queryaux.client_id, queryaux.client_name, SUM(total_products) as total_purchased FROM
    (SELECT c.client_id, c.client_name, co.company_name, pd.product_qty, p.product_name,
    SUM(pd.product_qty) as total_products,
    Count(c.client_name) as person_ocurrences
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c on c.client_id = pd.client_id
    JOIN p2db.company co on co.company_id = pd.company_id
    GROUP BY c.client_id, c.client_name, co.company_name, pd.product_qty, p.product_name) as queryaux
    group by queryaux.client_id, queryaux.client_name
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

    SELECT queryaux1.address_description, queryaux1.address_region, queryaux1.address_city, queryaux1.address_postal_code, queryaux1.total_ocurrences from
    (SELECT query1.address_description, query1.address_region, query1.address_city, query1.address_postal_code, count(query1.address_postal_code) as total_ocurrences FROM
    (SELECT pr.provider_registration_date, c.company_name ,a.address_description, a.address_region, a.address_city, a.address_postal_code,
    COUNT(pr.provider_registration_date) as date_ocurrences,
    COUNT(a.address_description) as address_ocurrences
    FROM p2db.order_detail od
    JOIN p2db.provider pr on pr.provider_id = od.provider_id
    JOIN p2db.address a on a.address_id = od.provider_address_id
    JOIN p2db.company c on c.company_id = od.company_id
    GROUP BY pr.provider_registration_date,c.company_name, a.address_description, a.address_region, a.address_city, a.address_postal_code
    order by address_ocurrences DESC) as query1
    group by query1.address_description, query1.address_region, query1.address_city, query1.address_postal_code
    order by total_ocurrences DESC LIMIT 1) as queryaux1
    UNION
    SELECT queryaux2.address_description, queryaux2.address_region, queryaux2.address_city, queryaux2.address_postal_code, queryaux2.total_ocurrences from
    (SELECT query1.address_description, query1.address_region, query1.address_city, query1.address_postal_code, count(query1.address_postal_code) as total_ocurrences FROM
    (SELECT pr.provider_registration_date, c.company_name ,a.address_description, a.address_region, a.address_city, a.address_postal_code,
    COUNT(pr.provider_registration_date) as date_ocurrences,
    COUNT(a.address_description) as address_ocurrences
    FROM p2db.order_detail od
    JOIN p2db.provider pr on pr.provider_id = od.provider_id
    JOIN p2db.address a on a.address_id = od.provider_address_id
    JOIN p2db.company c on c.company_id = od.company_id
    GROUP BY pr.provider_registration_date,c.company_name, a.address_description, a.address_region, a.address_city, a.address_postal_code
    order by address_ocurrences DESC) as query1
    group by query1.address_description, query1.address_region, query1.address_city, query1.address_postal_code
    order by total_ocurrences ASC LIMIT 1) as queryaux2;

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
    
    SELECT queryaux1.client_id, queryaux1.client_name, COUNT(queryaux1.client_name) as total_orders, SUM(queryaux1.total) as total_purchased FROM
    (SELECT co.company_name, c.client_registration_date, c.client_id, c.client_name, pc.category,
    count(c.client_registration_date) as date_ocurrences,
    SUM(pd.product_qty * p.product_unit_price) as total
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.product_category pc ON pc.product_category_id = p.product_category
    JOIN p2db.clientp c ON c.client_id = pd.client_id
    JOIN p2db.company co on co.company_id = pd.company_id
    WHERE pc.category = 'Cheese'
    GROUP BY co.company_name, c.client_registration_date, c.client_id, c.client_name, pc.category
    ORDER BY total DESC) as queryaux1
    GROUP BY queryaux1.client_id, queryaux1.client_name
    ORDER BY total_orders DESC LIMIT 5;

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
    
    SELECT query1.month1 , query1.client_name, query1.client_orders, query1.total FROM
    (SELECT EXTRACT(MONTH FROM c.client_registration_date) as month1, c.client_name,
    COUNT(c.client_name) as client_orders,
    SUM(p.product_unit_price * pd.product_qty) as total
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c on c.client_id = pd.client_id
    GROUP BY c.client_name, month1
    ORDER BY total DESC LIMIT 5) as query1
    UNION
    SELECT query2.month2 , query2.client_name, query2.client_orders, query2.total FROM
    (SELECT EXTRACT(MONTH FROM c.client_registration_date) as month2, c.client_name,
    COUNT(c.client_name) as client_orders,
    SUM(p.product_unit_price * pd.product_qty) as total
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c on c.client_id = pd.client_id
    GROUP BY c.client_name, month2
    ORDER BY total ASC LIMIT 5) as query2;

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
    
    SELECT query5.category, query5.total_amount FROM
    (SELECT query1.category, SUM(query1.total) as total_amount FROM
    (SELECT pc.category, pd.product_qty, p.product_unit_price,
    SUM(pd.product_qty * p.product_unit_price) as total,
    COUNT(p.product_category) as product_count
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c on c.client_id = pd.client_id
    JOIN p2db.product_category pc ON pc.product_category_id = p.product_category
    GROUP BY pc.category, pd.product_qty, p.product_unit_price) as query1
    GROUP BY query1.category
    ORDER BY total_amount DESC LIMIT 1) as query5
    UNION
    SELECT query3.category, query3.total_amount FROM
    (SELECT query2.category, SUM(query2.total) as total_amount FROM
    (SELECT pc.category, pd.product_qty, p.product_unit_price,
    SUM(pd.product_qty * p.product_unit_price) as total,
    COUNT(p.product_category) as product_count
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c on c.client_id = pd.client_id
    JOIN p2db.product_category pc ON pc.product_category_id = p.product_category
    GROUP BY pc.category, pd.product_qty, p.product_unit_price) as query2
    GROUP BY query2.category
    ORDER BY total_amount ASC LIMIT 1) as query3;

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
    select queryaux.provider_id, queryaux.provider_name, SUM(queryaux.total) as total_sold from 
    (SELECT pr.provider_id, pr.provider_name, pc.category, p.product_name,od.product_qty, p.product_unit_price,
    SUM(od.product_qty * p.product_unit_price) as total
    FROM p2db.order_detail od
    JOIN p2db.product p ON p.product_id = od.product_id
    JOIN p2db.provider pr on pr.provider_id = od.provider_id
    JOIN p2db.product_category pc ON pc.product_category_id = p.product_category
    WHERE pc.category = 'Fresh Vegetables'
    GROUP BY pr.provider_id, pr.provider_name, pc.category, p.product_name,od.product_qty, p.product_unit_price) as queryaux
    group by queryaux.provider_id, queryaux.provider_name
    ORDER BY total_sold DESC limit 5;
    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

p1db.query8 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`
    
    SELECT query1.address_description, query1.address_region, query1.address_city, query1.address_postal_code,query1.total FROM
    (SELECT c.client_name, a.address_description, a.address_region, a.address_city, a.address_postal_code,
    COUNT(c.client_name) as client_orders,
    SUM(pd.product_qty * p.product_unit_price) as total
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c ON c.client_id = pd.client_id
    JOIN p2db.address a ON a.address_id = pd.client_address_id
    GROUP BY c.client_name, a.address_description, a.address_region, a.address_city, a.address_postal_code
    ORDER BY total DESC LIMIT 1) as query1
    UNION 
    SELECT query2.address_description, query2.address_region, query2.address_city, query2.address_postal_code,query2.total FROM
    (SELECT c.client_name, a.address_description, a.address_region, a.address_city, a.address_postal_code,
    COUNT(c.client_name) as client_orders,
    SUM(pd.product_qty * p.product_unit_price) as total
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c ON c.client_id = pd.client_id
    JOIN p2db.address a ON a.address_id = pd.client_address_id
    GROUP BY c.client_name, a.address_description, a.address_region, a.address_city, a.address_postal_code
    ORDER BY total ASC LIMIT 1) as query2;
    

    `, (err, res) => {
      if(err) {
        reject(err)
      }
      return resolve(res);
    });
  });
}

p1db.query9 = () => {
  return new Promise((resolve, reject) => {
    pool.query(`

    SELECT queryaux.provider_registration_date, queryaux.provider_name, queryaux.provider_phone_number, queryaux.total_products, queryaux.total from
    (SELECT pr.provider_registration_date, c.company_name, pr.provider_name, pr.provider_phone_number, SUM(od.product_qty) as total_products,
    COUNT(pr.provider_registration_date) as date_ocurrences,
    COUNT(c.company_name) as comp_ocurrences,
    SUM(od.product_qty * p.product_unit_price) as total
    FROM p2db.order_detail od
    JOIN p2db.product p ON p.product_id = od.product_id
    JOIN p2db.provider pr on pr.provider_id = od.provider_id
    JOIN p2db.company c on c.company_id = od.company_id
    GROUP BY pr.provider_registration_date, c.company_name,pr.provider_name, pr.provider_phone_number
    ORDER BY date_ocurrences ASC) as queryaux
    ORDER BY queryaux.total_products ASC limit 12;


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
    
    SELECT c.client_name, pc.category,
    COUNT(c.client_name) as person_ocurrences,
    SUM(pd.product_qty) as total_products
    FROM p2db.purchase_detail pd
    JOIN p2db.product p ON p.product_id = pd.product_id
    JOIN p2db.clientp c on c.client_id = pd.client_id
    JOIN p2db.product_category pc ON pc.product_category_id = p.product_category
    WHERE pc.category = 'Seafood'
    GROUP BY c.client_name, pc.category
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