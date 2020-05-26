DROP TABLE IF EXISTS u_b;
DROP TABLE IF EXISTS active_users_date;
DROP TABLE IF EXISTS active_users_hr;

SELECT user_id,
       item_id,
       TIMESTAMP
FROM userbehavior_small
GROUP BY user_id,
         item_id,
         TIMESTAMP
HAVING count(1) > 1;

CREATE TABLE u_b
SELECT *
FROM userbehavior_small;
         

SELECT COUNT(user_id), COUNT(item_id), COUNT(cate_id), COUNT(behavior_type), COUNT(TIMESTAMP)
FROM u_b;

ALTER TABLE u_b ADD ID INT UNSIGNED PRIMARY KEY AUTO_INCREMENT;

ALTER TABLE u_b ADD (longdate VARCHAR(255), DATE VARCHAR(255), TIME VARCHAR(255));

UPDATE u_b
SET longdate=FROM_UNIXTIME(TIMESTAMP,'%Y-%m-%d %k:%i:%s'),
DATE=FROM_UNIXTIME(TIMESTAMP,'%Y-%m-%d'),
TIME=FROM_UNIXTIME(TIMESTAMP,'%k:%i:%s');

ALTER TABLE u_b ADD HOUR INTEGER(30);
UPDATE u_b SET HOUR = HOUR(TIME);
       
SELECT COUNT(longdate)
FROM u_b
WHERE longdate<'2017-11-25 00:00:00' OR longdate >'2017-12-04 00:00:00';

DELETE FROM u_b
WHERE longdate<'2017-11-25 00:00:00' OR longdate >'2017-12-04 00:00:00';

SELECT 
COUNT(DISTINCT user_id) AS user_amount,
COUNT(DISTINCT item_id) AS item_amount,
COUNT(DISTINCT cate_id) AS item_cate_amount,
SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS pv_amount,
SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS favor_amount,
SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS add_cart_amount,
SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS purchase_amount,
COUNT(longdate) AS total_amount
FROM u_b;


-- Find the amount of Page Visit GROUP BY DATE
CREATE TABLE page_visit_date AS (
SELECT DATE, count(behavior_type) AS total_pv 
FROM u_b
WHERE behavior_type='pv'
GROUP BY DATE
ORDER BY DATE ASC
);

  
-- Find the amount of Active Users GROUP BY DATE
CREATE TABLE active_users_date AS (
SELECT DATE, count(DISTINCT(user_id)) AS active_users_date
FROM u_b
GROUP BY DATE
ORDER BY DATE ASC
);


-- Calculate the Bounce Rate
SELECT 
(SELECT count(DISTINCT user_id) FROM u_b
WHERE user_id NOT IN 
(SELECT DISTINCT user_id FROM u_b WHERE behavior_type='fav')
AND user_id NOT IN
(SELECT DISTINCT user_id FROM u_b WHERE behavior_type='cart')
AND user_id NOT IN 
(SELECT DISTINCT user_id FROM u_b WHERE behavior_type='buy') ) / (SELECT count(DISTINCT user_id) FROM u_b);


-- Find the amount of Active Users GROUP BY HOUR
CREATE TABLE active_users_hr AS (
SELECT HOUR, COUNT(DISTINCT(user_id)) AS active_users_hr
FROM u_b 
GROUP BY HOUR
ORDER BY HOUR ASC
);


SELECT count(DISTINCT a.user_id) AS cart_amount,count(DISTINCT b.user_id) AS cart_and_purchase_amount FROM
(SELECT DISTINCT user_id,item_id,cate_id,TIMESTAMP FROM u_b WHERE behavior_type='cart') a
LEFT JOIN
(SELECT DISTINCT user_id,item_id,cate_id,TIMESTAMP FROM u_b WHERE behavior_type='buy') b
ON a.user_id=b.user_id AND a.item_id=b.item_id AND a.cate_id=b.cate_id AND a.timestamp<b.timestamp;

SELECT count(DISTINCT user_id) FROM u_b WHERE behavior_type='pv';





-- CHECK the different types of behavior across different date
CREATE TABLE behavior_type_date AS (
SELECT DATE, 
    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS visit_amount,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_amount,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS favorite_amount,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS purchase_amount
FROM u_b
GROUP BY DATE
ORDER BY DATE ASC
);


-- CREATE correlation matrix
WITH correlation_CTE AS
(
SELECT visit_amount AS ROW, 
       visit_amount AS col, 
       corr(visit_amount, visit_amount) AS coeff
FROM behavior_type_date
UNION
SELECT visit_amount AS ROW, 
       cart_amount AS col, 
       corr(visit_amount, cart_amount) AS coeff
FROM behavior_type_date
UNION
SELECT visit_amount AS ROW, 
       favorite_amount AS col, 
       corr(visit_amount, favorite_amount) AS coeff
FROM behavior_type_date
UNION
SELECT visit_amount AS ROW, 
       purchase_amount AS col, 
       corr(visit_amount, purchase_amount) AS coeff
FROM behavior_type_date
UNION
SELECT cart_amount AS ROW, 
       cart_amount AS col, 
       corr(cart_amount, cart_amount) AS coeff
FROM behavior_type_date
UNION
SELECT cart_amount AS ROW, 
       favorite_amount AS col, 
       corr(cart_amount, favorite_amount) AS coeff
FROM behavior_type_date
UNION
SELECT cart_amount AS ROW, 
       purchase_amount AS col, 
       corr(cart_amount, purchase_amount) AS coeff
FROM behavior_type_date
UNION
SELECT favorite_amount AS ROW, 
       favorite_amount AS col, 
       corr(favorite_amount, favorite_amount) AS coeff
FROM behavior_type_date
UNION
SELECT favorite_amount AS ROW, 
       purchase_amount AS col, 
       corr(favorite_amount, purchase_amount) AS coeff
FROM behavior_type_date
UNION
SELECT purchase_amount AS ROW, 
       purchase_amount AS col, 
       corr(purchase_amount, purchase_amount) AS coeff
FROM behavior_type_date
)
SELECT ROW,
       sum(CASE
               WHEN col= 'visit_amount' THEN coeff
               ELSE 0
           END) AS visit_amount,
       sum(CASE
               WHEN col='cart_amount' THEN coeff
               ELSE 0
           END) AS cart_amount,
       sum(CASE
               WHEN col='favorite_amount' THEN coeff
               ELSE 0
           END) AS favorite_amount,
       sum(CASE
               WHEN col='purchase_amount' THEN coeff
               ELSE 0
           END) AS purchase_amount
FROM corelation_CTE
GROUP BY ROW
ORDER BY ROW DESC
;

-- CHECK the different types of behavior across different hours
CREATE TABLE behavior_type_date AS (
SELECT DATE, 
    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS visit_amount,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_amount,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS favorite_amount,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS purchase_amount
FROM u_b
GROUP BY DATE
ORDER BY DATE ASC
);

-- Conversion Rate
-- Overall Conversion Rate 1.43%, total visit is 856840
SELECT SUM(visit), SUM(purchase), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0;

-- Direct purchase Group without adding to cart, add to favorite, overall Conversion Rate 0.86%
-- Direct Puchase Group Conversion Rate 1.15%
SELECT SUM(visit), SUM(purchase), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite = 0
AND cart = 0;

-- Indirect Purchase Group with adding to cart, without adding to favorite, overall Conversion Rate 0.34%
-- From Visit to "Add to cart" Conversion Rate 25.75%, from "Add to cart" to purchase Conversion Rate 10.08%, from visit to purchase Conversion Rate 2.6%
SELECT SUM(visit), SUM(cart), SUM(purchase), SUM(cart) / SUM(visit), SUM(purchase) / SUM(cart), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite = 0
AND cart > 0;

-- Indirect Purchase Group with adding to favorite, without adding to cart, overall Conversion Rate 0.10%
-- From Visit to "Add to favorite" Conversion Rate 25.21%, from "Add to favorite" to purchase Conversion Rate 6.75%, from visit to purchase Conversion Rate 1.7%
SELECT SUM(visit), SUM(cart), SUM(purchase), SUM(favorite) / SUM(visit), SUM(purchase) / SUM(favorite), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite > 0
AND cart = 0;

-- Indirect Purchase Group with adding to favorite and adding to cart, overall Conversion Rate 0.14%
-- From Visit to "Add to cart" or "Add to favorite" Conversion Rate 20.06%, from "Add to cart" or "Add to favorite" to purchase Conversion Rate 10.42%, from visit to purchase Conversion Rate 2.09%
SELECT SUM(visit), SUM(cart), SUM(purchase), (SUM(cart) + SUM(favorite)) / SUM(visit), SUM(purchase) / (SUM(cart) + SUM(favorite)), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite > 0
AND cart > 0;



-- Recency
CREATE TABLE score_01 AS
SELECT user_id,
(CASE WHEN purchase_time_period BETWEEN 0 AND 2 THEN 1
WHEN purchase_time_period BETWEEN 3 AND 4 THEN 2
WHEN purchase_time_period BETWEEN 5 AND 6 THEN 3
WHEN purchase_time_period BETWEEN 7 AND 8 THEN 4 ELSE 0 END
) AS recency_rate
FROM
(SELECT user_id,datediff(max(DATE),'2017-11-25') AS purchase_time_period
FROM u_b
WHERE behavior_type='buy'
GROUP BY user_id) AS a
ORDER BY recency_rate;

SELECT * FROM score_01;


-- Frequency:
SELECT user_id,count(user_id) AS purchase_amount FROM u_b
WHERE behavior_type='buy'
GROUP BY user_id
ORDER BY count(user_id) DESC
LIMIT 10;

CREATE TABLE score_02 AS
SELECT user_id,
(CASE WHEN purchase_times BETWEEN 1 AND 4 THEN 1
WHEN purchase_times BETWEEN 5 AND 8 THEN 2
WHEN purchase_times BETWEEN 9 AND 12 THEN 3
WHEN purchase_times BETWEEN 13 AND 17 THEN 4 ELSE 0 END 
)AS frequency_rate
FROM
(SELECT user_id,count(behavior_type)AS purchase_times
FROM u_b
WHERE behavior_type='buy'
GROUP BY user_id) b
ORDER BY frequency_rate DESC;

SELECT * FROM score_02;


-- Calculate the mean rate for Recency and Frequency
SELECT AVG(recency_rate) FROM score_01;
SELECT AVG(frequency_rate)FROM score_02;


-- Classify users
CREATE TABLE users_classify AS (
SELECT user_id,
(CASE WHEN R>2.38 AND F>1 THEN 'Most Valuable User'
WHEN R>2.38 AND F<=1 THEN 'Important Sustainable User'
WHEN R<=2.38 AND F>1 THEN 'Important Developable User'
WHEN R<=2.38 AND F<=1 THEN 'Normal User' ELSE 0 END 
)AS user_type
FROM
(SELECT a.user_id,a.recency_rate AS R,b.frequency_rate AS F
FROM score_01 AS a INNER JOIN score_02 AS b
ON a.user_id=b.user_id)c);

SELECT * FROM users_classify;

CREATE TABLE users_classify_result AS (
SELECT user_type,count(DISTINCT user_id) AS amount FROM users_classify GROUP BY user_type);

SELECT * FROM users_classify_result ORDER BY amount;

ALTER SCHEMA Ali_userbehavior  DEFAULT COLLATE utf8_bin;
ALTER TABLE `Ali_userbehavior`.`active_users_date` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`active_users_hr` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`behavior_type_date` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`behavior_type_hr` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`page_visit_date` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`score_01` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`score_02` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`u_b` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`user_beha` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`userbehavior_small` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`users_classify` CONVERT TO CHARACTER SET UTF8MB3;
ALTER TABLE `Ali_userbehavior`.`users_classify_result` CONVERT TO CHARACTER SET UTF8MB3;

