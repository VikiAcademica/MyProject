CREATE DATABASE Customers_transactions;
UPDATE customers SET Gender = NULL WHERE Gender ='';
UPDATE customers SET Age = NULL WHERE Age ='';
ALTER TABLE Customers MODIFY AGE INT NULL;

SELECT * FROM Customers;

CREATE TABLE Transactions
(date_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL(10,3),
Sum_payment DECIMAL(10,2));

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions_final.csv"
INTO TABLE Transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'secure_file_priv';

# Список клиентов с непрерывной историей за год
WITH transactions_per_month AS (
    SELECT 
        ID_client, 
        DATE_FORMAT(date_new, '%Y-%m') AS month,
        COUNT(*) AS transactions_count,
        AVG(Sum_payment) AS avg_check
    FROM transactions
    WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
    GROUP BY ID_client, month
),
monthly_activity AS (
    SELECT ID_client, COUNT(DISTINCT month) AS active_months
    FROM transactions_per_month
    GROUP BY ID_client
),
continuous_clients AS (
    SELECT ID_client 
    FROM monthly_activity
    WHERE active_months = 12
)
SELECT 
    t.ID_client,
    SUM(t.Sum_payment) / 12 AS avg_monthly_spend,
    AVG(t.Sum_payment) AS avg_check,
    COUNT(t.ID_check) AS total_operations
FROM transactions t
JOIN continuous_clients cc ON t.ID_client = cc.ID_client
WHERE t.date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY t.ID_client;

#Информация в разрезе месяцев
#a) Средняя сумма чека в месяц
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    AVG(Sum_payment) AS avg_check
FROM transactions
WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY month
ORDER BY month;

#b) Среднее количество операций в месяц
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(ID_check) / COUNT(DISTINCT ID_client) AS avg_operations
FROM transactions
WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY month
ORDER BY month;

#c) Среднее количество клиентов, совершавших операции

SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(DISTINCT ID_client) AS avg_active_clients
FROM transactions
WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY month
ORDER BY month;

#d) Доли операций и сумм в месяце от общего годового объема

WITH yearly_totals AS (
    SELECT 
        COUNT(ID_check) AS total_operations_year,
        SUM(Sum_payment) AS total_sum_year
    FROM transactions
    WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
)
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(ID_check) AS monthly_operations,
    SUM(Sum_payment) AS monthly_sum,
    COUNT(ID_check) * 100.0 / (SELECT total_operations_year FROM yearly_totals) AS operations_share,
    SUM(Sum_payment) * 100.0 / (SELECT total_sum_year FROM yearly_totals) AS sum_share
FROM transactions
WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY month
ORDER BY month;

#e) % соотношение M/F/NA в месяц + доля затрат

SELECT 
    DATE_FORMAT(t.date_new, '%Y-%m') AS month,
    c.Gender,
    COUNT(DISTINCT t.ID_client) AS clients_count,
    COUNT(DISTINCT t.ID_client) * 100.0 / SUM(COUNT(DISTINCT t.ID_client)) OVER (PARTITION BY DATE_FORMAT(t.date_new, '%Y-%m')) AS client_share,
    SUM(t.Sum_payment) AS total_spent,
    SUM(t.Sum_payment) * 100.0 / SUM(SUM(t.Sum_payment)) OVER (PARTITION BY DATE_FORMAT(t.date_new, '%Y-%m')) AS spend_share
FROM transactions t
JOIN customers c ON t.ID_client = c.Id_client
WHERE t.date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY month, c.Gender
ORDER BY month, c.Gender;

#Возрастные группы клиентов с шагом 10 лет и поквартальный анализ
#Группировка по возрасту + сумма и количество операций за год

SELECT 
    CASE 
        WHEN Age IS NULL THEN 'Unknown'
        ELSE CONCAT(FLOOR(Age / 10) * 10, '-', FLOOR(Age / 10) * 10 + 9)
    END AS age_group,
    COUNT(DISTINCT t.ID_client) AS clients_count,
    COUNT(t.ID_check) AS total_operations,
    SUM(t.Sum_payment) AS total_spent
FROM transactions t
JOIN customers c ON t.ID_client = c.Id_client
WHERE t.date_new BETWEEN '2015-06-01' AND '2016-05-31'
GROUP BY age_group
ORDER BY age_group;

#Средние показатели поквартально + %

WITH quarterly_data AS (
    SELECT 
        CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)) AS quarter,
        CASE 
            WHEN Age IS NULL THEN 'Unknown'
            ELSE CONCAT(FLOOR(Age / 10) * 10, '-', FLOOR(Age / 10) * 10 + 9)
        END AS age_group,
        COUNT(DISTINCT t.ID_client) AS clients_count,
        COUNT(t.ID_check) AS total_operations,
        SUM(t.Sum_payment) AS total_spent
    FROM transactions t
    JOIN customers c ON t.ID_client = c.Id_client
    WHERE t.date_new BETWEEN '2015-06-01' AND '2016-05-31'
    GROUP BY quarter, age_group
)
SELECT 
    quarter,
    age_group,
    clients_count,
    total_operations,
    total_spent,
    clients_count * 100.0 / SUM(clients_count) OVER (PARTITION BY quarter) AS clients_pct,
    total_operations * 100.0 / SUM(total_operations) OVER (PARTITION BY quarter) AS operations_pct,
    total_spent * 100.0 / SUM(total_spent) OVER (PARTITION BY quarter) AS spent_pct
FROM quarterly_data
ORDER BY quarter, age_group;
