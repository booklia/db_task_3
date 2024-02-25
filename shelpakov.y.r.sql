SELECT job_industry_category, COUNT(*) AS customer_count
FROM customers
GROUP BY job_industry_category
ORDER BY customer_count DESC;

SELECT EXTRACT(MONTH FROM TO_DATE(transaction_date, 'DD.MM.YYYY')) AS year_month,
    job_industry_category,
    SUM(list_price) AS total_transaction_amount
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
GROUP BY year_month, job_industry_category
ORDER BY year_month, job_industry_category;

SELECT t.brand, COUNT(*) AS online_order_count
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
WHERE c.job_industry_category = 'IT' AND t.order_status = 'Approved' AND t.online_order = 'Yes' 
GROUP BY t.brand;


SELECT
    c.customer_id,
    SUM(t.list_price) AS total_amount,
    MAX(t.list_price) AS max_transaction,
    MIN(t.list_price) AS min_transaction,
    COUNT(t.transaction_id) AS transaction_count
FROM
    customers c
JOIN
    transactions t ON c.customer_id = t.customer_id
GROUP BY
    c.customer_id
ORDER BY
    total_amount DESC, transaction_count DESC;


SELECT DISTINCT
    customer_id,
    SUM(list_price) OVER (PARTITION BY customer_id) AS total_amount,
    MAX(list_price) OVER (PARTITION BY customer_id) AS max_transaction,
    MIN(list_price) OVER (PARTITION BY customer_id) AS min_transaction,
    COUNT(transaction_id) OVER (PARTITION BY customer_id) AS transaction_count
FROM
    transactions
ORDER BY
    total_amount DESC, transaction_count DESC;


SELECT
    c.first_name,
    c.last_name,
    MIN(total_amount) AS min_total_amount
FROM
    customers c
JOIN (
    SELECT
        customer_id,
        SUM(list_price) AS total_amount
    FROM
        transactions
    GROUP BY
        customer_id
) t ON c.customer_id = t.customer_id
WHERE
    total_amount IS NOT NULL
GROUP BY
    c.customer_id, c.first_name, c.last_name
ORDER BY
    min_total_amount;


SELECT
    c.first_name,
    c.last_name,
    MAX(total_amount) AS max_total_amount
FROM
    customers c
JOIN (
    SELECT
        customer_id,
        SUM(list_price) AS total_amount
    FROM
        transactions
    GROUP BY
        customer_id
) t ON c.customer_id = t.customer_id
WHERE
    total_amount IS NOT NULL
GROUP BY
    c.customer_id, c.first_name, c.last_name
ORDER BY
    max_total_amount DESC;



WITH rankedTransactions AS (
    SELECT
        customer_id,
        transaction_id,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) AS row_num
    FROM
        transactions
)
SELECT
    t.customer_id,
    t.transaction_id,
    t.transaction_date,
    t.list_price
FROM
    transactions t
JOIN
    rankedTransactions rt ON t.transaction_id = rt.transaction_id
WHERE
    rt.row_num = 1;



WITH TransactionIntervals AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.job_title,
        transactions.transaction_date,
        LAG(TO_DATE(transactions.transaction_date, 'DD.MM.YYYY')) OVER (PARTITION BY transactions.customer_id ORDER BY transactions.transaction_date) AS prev_transaction_date,
        LEAD(TO_DATE(transactions.transaction_date, 'DD.MM.YYYY')) OVER (PARTITION BY transactions.customer_id ORDER BY transactions.transaction_date) AS next_transaction_date,
        COALESCE(TO_DATE(transactions.transaction_date, 'DD.MM.YYYY') - LAG(TO_DATE(transactions.transaction_date, 'DD.MM.YYYY')) OVER (PARTITION BY transactions.customer_id ORDER BY transactions.transaction_date), 0) AS interval_days
    FROM
        customers
    JOIN transactions ON customers.customer_id = transactions.customer_id
)
SELECT
    customer_id,
    first_name,
    last_name,
    job_title,
    transaction_date,
    interval_days
FROM TransactionIntervals
ORDER BY interval_days DESC
LIMIT 10;