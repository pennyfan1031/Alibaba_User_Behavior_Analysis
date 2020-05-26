# Alibaba User behavior Analysis

## Business Content



## Data Cleaning

### Select subset

Because the raw data has more than 100M rows, we don't have the enough storage to load all of them. Here we select only the first 1M rows for analysis.

```Python
DATA_PATH = '/Users/Documents/project/Alibaba user behavior analysis/dataset/UserBehavior.csv'
df = pd.read_csv(DATA_PATH, nrows = 1000000)
target_name = '/UsersDocuments/project/Alibaba user behavior analysis/dataset/UserBehavior_small.csv'
df.to_csv(target_name)
```



### Load data into SQL Database

I renamed the columns in this data file as user_id, item_id, cate_id, behavior_type and timestamp. And when the transformed csv data was loaded into the SQL database, we need to make sure every column except the timestamp are the VARCHAR type while the timestamp column is INT type.



### Duplicates Detection

1. We treat the rows with same users_id, item_id and timestamp as duplicates.

```SQL
SELECT user_id,
       item_id,
       TIMESTAMP
FROM userbehavior_small
GROUP BY user_id,
         item_id,
         TIMESTAMP
HAVING count(1) > 1
```

The result shows there is no duplicate.

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200427235609912.png" alt="image-20200427235609912" style="zoom:50%;" />

2. Create a new table called u_b to store distinct raw data.

   ```SQL
   CREATE TABLE u_b
   SELECT *
   FROM userbehavior_small
   GROUP BY user_id,
            item_id,
            cate_id,
            behavior_type,
            TIMESTAMP
   ```



### Missing Value Treatment

```SQL
SELECT COUNT(user_id), COUNT(item_id), COUNT(cate_id), COUNT(behavior_type), COUNT(timestamp)
FROM u_b
```

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200428000350862.png" alt="image-20200428000350862" style="zoom:75%;" />

The result shows there is no missing value.



### Unifying Process

1. Add ID column as PK.

   ```SQL
   ALTER TABLE u_b ADD ID int unsigned primary key auto_increment
   ```

2. Add columns 'longdate', 'date' and 'time' to store the time.

   ```SQL
   ALTER TABLE u_b ADD(longdate VARCHAR(255), date VARCHAR(255), time VARCHAR(255));
   ```

3. Transform the timestamp, and store it in  'longdate', 'date' and 'time'.

   ```SQL
   UPDATE u_b
   SET longdate=FROM_UNIXTIME(timestamp,'%Y-%m-%d %k:%i:%s'),
   date=FROM_UNIXTIME(timestamp,'%Y-%m-%d'),
   time=FROM_UNIXTIME(timestamp,'%k:%i:%s')
   WHERE ID BETWEEN 1 and 1000000;
   ```

4. Add hour column.

   ```SQL
   ALTER TABLE u_b ADD hour INTEGER(30);
   UPDATE u_b SET hour = HOUR(time);
   ```

   

### Anomalies Treatment

Based on the intro of this dataset, the data should be between 2017/11/25 to 2017/12/3. Therefore we treat any data outside this range as anomalies.

```SQL
SELECT COUNT(longdate)
FROM u_b
WHERE longdate<'2017-11-25 00:00:00' OR longdate >'2017-12-03 24:00:00';
```

And then we deleted those anomalies.

```SQL
DELETE FROM u_b
WHERE longdate<'2017-11-25 00:00:00' or longdate >'2017-12-03 24:00:00';
```

A basic data summary:

```SQL
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
```

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200428194854730.png" alt="image-20200428194854730" style="zoom:80%;" />



## Set Up Model

![image-20200503104419226](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200503104419226.png)

Note: 

1. Since we don't have data for AOP and Profit Ratio, we will not conduct analysis on this part. 
2. Since we don't know about M in this RFM model, we will not focus on this part.



![image-20200503104213856](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200503104213856.png)



## Analysis Logic

![image-20200503105243128](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200503105243128.png)



## Exploratory Data Analysis

### 1. From users

#### (a) Page View & Unique Visit Analysis

```SQL
-- Find the amount of Page Visit GROUP BY DATE
CREATE TABLE page_visit_date AS (
SELECT DATE, count(behavior_type) AS total_pv 
FROM u_b
WHERE behavior_type='pv'
GROUP BY DATE
ORDER BY DATE ASC
);
```

![image-20200524003046858](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200524003046858.png)

Bounce Rate: No any action on the websites.

```SQL
-- Calculate the Bounce Rate
SELECT 
(SELECT count(DISTINCT user_id) FROM u_b
WHERE user_id NOT IN 
(SELECT DISTINCT user_id FROM u_b WHERE behavior_type='fav')
AND user_id NOT IN
(SELECT DISTINCT user_id FROM u_b WHERE behavior_type='cart')
AND user_id NOT IN 
(SELECT DISTINCT user_id FROM u_b WHERE behavior_type='buy') ) / (SELECT count(DISTINCT user_id) FROM u_b);

```

The Bounce Rate is high, around 80%. We might want to pay attention to the detailed page layout design to make sure it can earn decent conversions.

#### (b) Active User Analysis

```SQL
-- Find the amount of Active Users group by date
CREATE TABLE active_users AS (
SELECT DATE, count(DISTINCT(user_id)) AS active_users_date
FROM u_b
GROUP BY DATE
ORDER BY DATE ASC
)
```

We can tell from this graph that active user number show a steady trend from Nov 25th to Dec 1st. However, start from Dec 1st, the number shows a increasing trend. We suggest the analyst to do dome retrospective analysis on why this happen.

![image-20200504003838879](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200504003838879.png)

```SQL
-- Find the amount of Active Users group by hour
SELECT hour, COUNT(DISTINCT(user_id)) AS active_users_hr
FROM u_b
GROUP BY hour
ORDER BY hour ASC
```

![image-20200504003917430](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200504003917430.png)

What we can learn from this graph is that 4am has the lowest amount of actiev users. The active users amount show a stable trend from 10am to 6pm, and peaks at around 9pm .



####(c) User Behavior Analysis

``````SQL
-- Check the different type of behavior amount across different date
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
``````

![image-20200504232128900](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200504232128900.png)

We can see that the visits amount is much larger than the other user behaviors, therefore we will focus on other user behaviros in the next step.

![image-20200504232921087](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200504232921087.png)

All of the four different types of user behaviors are very similar to each other. To quantify this finding, I calculated the correlation matrix.

```SQL
-- Create correlation matrix
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
```

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200505103723591.png" alt="image-20200505103723591" style="zoom:75%;" />

Again, we confirmed our finding for this step. 

Findings on user behvaiors analysis on date granularity:

1. The shape of total amount from each type of user behavior is similar to the date-level active user situation.
2. Different types user behaviors are indeed very similar to each other according to the correlation values.
3. Visits are much more higher than the other three behaviors. And the 2nd highest - add to cart is also much higher than the rest two behaviors.



To continue on user behavior analysis, I aggregated the data into hour level.

```SQL
-- CHECK the different types of behavior across different hours
CREATE TABLE behavior_type_hr AS (
SELECT HOUR, 
    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS visit_amount,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_amount,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS favorite_amount,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS purchase_amount
FROM u_b
GROUP BY HOUR
ORDER BY HOUR ASC
);
```

![image-20200505111635744](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200505111635744.png)

![image-20200505111846710](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200505111846710.png)

Findings on user behaviors analysis from hour granularity:

1. The shape of total amount from each type of user behavior is similar to the hour-level active user situation.
2. Same findings 2.3 from date level analysis.

#### (d) RFM model Analysis

As we mentioed in the Analysis Logic section, we will not analyze **M(Monetary)**.

**R(Recency):**

Since the time range from 11/25 to 12/03, here we will set 11/25 as a base reference. That's being said, we will set diffrent level by measuring the distance from 11/25. 0-2 days corresponds to 1, 3-4 days corresponds to 2, 5-6 days corresponds to 3 and 7-8 days corresponds to 4.

```SQL
-- Recency:
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
```

**F(Frequency)：**

First we need to find the highest frequency stats.

```SQL
SELECT user_id,count(user_id) AS purchase_amount FROM u_b
WHERE behavior_type='buy'
GROUP BY user_id
ORDER BY count(user_id) DESC
LIMIT 10;
```

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200524170209541.png" alt="image-20200524170209541" style="zoom:50%;" />

The result shows that user "3122135" has the highest Frequncy of 17. Therefore, we will divide all the users into four different levels, 1-4 corresponds to 1, 5-8 corresponds to 2, 9-12 corresponds to 3 and 13-17 corresponds to 4.

```SQL
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
```

To decide who are our most valuable customers, here we calculate the average rate for R and F.

```SQL
-- Calculate the mean rate for Recency and Frequency
SELECT AVG(recency_rate) FROM score_01; -- 2.38
SELECT AVG(frequency_rate)FROM score_02; -- 1.00
```

```SQL
-- Classify users
CREATE table users_classify AS
SELECT user_id,
(CASE WHEN R>2.38 AND F>1 THEN 'Most Valuable User'
WHEN R>2.38 AND F<=1 THEN 'Important Sustainable User'
WHEN R<=2.38 AND F>1 THEN 'Important Developable User'
WHEN R<=2.38 AND F<=1 THEN 'Normal User' ELSE 0 END 
)AS user_type
FROM
(SELECT a.user_id,a.recency_rate AS R,b.frequency_rate AS F
FROM score_01 AS a INNER JOIN score_02 AS b
ON a.user_id=b.user_id)c;

SELECT * FROM users_classify;

CREATE TABLE users_classify_result AS (
SELECT user_type,count(DISTINCT user_id) AS amount FROM users_classify GROUP BY user_type);

SELECT * FROM users_classify_result ORDER BY amount;
```

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200525144053874.png" alt="image-20200525144053874" style="zoom:50%;" />

The result shows that only 0.26% customers are the most Important Users about a half of th customers are Normal Users and the other half are Important Sustainable Users. To retain our customers, we need to lay much emphasize on Important Sustaninable Users. This types of customers are easy to maintain but at the same time, are likely to be turned into most valuable users.

### 2. From products

####  (a) Conversion Rate Analysis (Using funnel model)

```SQL
-- Create a table to store data for each cuomer
CREATE TABLE user_beha AS (
SELECT user_id, cate_id,
	sum( CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END ) AS visit,
	sum( CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END ) AS favorite,
	sum( CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END ) AS cart,
	sum( CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END ) AS purchase
FROM u_b 
GROUP BY user_id, cate_id
);
```

<img src="/Users/peiningfan/Library/Application Support/typora-user-images/image-20200505112614347.png" alt="image-20200505112614347" style="zoom:50%;" />

Then I will follow these four different types of conversions to analysis:

![image-20200505112733419](/Users/peiningfan/Library/Application Support/typora-user-images/image-20200505112733419.png)



```SQL
-- Conversion Rate
-- Overall Conversion Rate 1.43%
SELECT SUM(visit), SUM(purchase), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0;

-- Direct purchase without adding to cart, add to favorite
-- Direct Puchase Conversion Rate 1.15%
SELECT SUM(visit), SUM(purchase), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite = 0
AND cart = 0;

-- Indirect Purchase with adding to cart, without adding to favorite
-- From Visit to "Add to cart" Conversion Rate 25.75%, from "Add to cart" to purchase Conversion Rate 10.08%, from visit to purchase Conversion Rate 2.6%
SELECT SUM(visit), SUM(cart), SUM(purchase), SUM(cart) / SUM(visit), SUM(purchase) / SUM(cart), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite = 0
AND cart > 0;

-- Indirect Purchase with adding to favorite, without adding to cart
-- From Visit to "Add to cart" Conversion Rate 25.21%, from "Add to favorite" to purchase Conversion Rate 6.75%, from visit to purchase Conversion Rate 1.7%
SELECT SUM(visit), SUM(cart), SUM(purchase), SUM(favorite) / SUM(visit), SUM(purchase) / SUM(favorite), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite > 0
AND cart = 0;

-- Indirect Purchase with adding to favorite and adding to cart
-- From Visit to "Add to cart" or "Add to favorite" Conversion Rate 20.06%, from "Add to cart" or "Add to favorite" to purchase Conversion Rate 10.42%, from visit to purchase Conversion Rate 2.09%
SELECT SUM(visit), SUM(cart), SUM(purchase), (SUM(cart) + SUM(favorite)) / SUM(visit), SUM(purchase) / (SUM(cart) + SUM(favorite)), SUM(purchase) / SUM(visit)
FROM user_beha 
WHERE visit > 0
AND favorite > 0
AND cart > 0;

```

<img src="/Users/peiningfan/Desktop/screenshots/Screen Shot 2020-05-24 at 10.19.46.png" style="zoom:50%;" />

<img src="/Users/peiningfan/Desktop/screenshots/Screen Shot 2020-05-24 at 10.20.23.png" style="zoom:50%;" />

Based on the funnel result, we can tell that users are almost equally likely to use "add to cart" and "add to favorite" function. The purchase conversion rate, however, is different for Path 1 and Path2. Users' conversion rate after "adding to cart" is 1% higher than that after "adding to favorite".

One possible reason for that is from cart page you can directly make a purchase, but for "favorite list" page, you need to go to the cart page again to make the purchase. The process of purchasing for path 2 will add an additional step comparing to path 1. During this process, the website might loose some of their active users.