-- QUERY 1
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

-- QUERY 2
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

-- QUERY 3
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

-- QUERY 4
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


-- QUERY 5
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

-- QUERY 6
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

-- QUERY 7 
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

-- QUERY 8
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

-- QUERY 9
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
ORDER BY queryaux.total_products ASC;

-- QUERY 10
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