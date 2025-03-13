-- Load Customers Dataset
customers = LOAD '/user/Project1/data/customers.csv'
    USING PigStorage(',')
    AS (ID: INT, Name: CHARARRAY, Age: INT, Gender: CHARARRAY, CountryCode: INT, Salary: DOUBLE);

-- Load Transactions Dataset
transactions = LOAD '/user/Project1/data/transactions.csv'
    USING PigStorage(',')
    AS (TransID: INT, CustID: INT, TransTotal: DOUBLE, TransNumItems: INT, TransDesc: CHARARRAY);

-- Assign age ranges
customers_age_group = FOREACH customers GENERATE 
    ID, 
    (CASE 
        WHEN Age >= 10 AND Age < 20 THEN '[10,20)'
        WHEN Age >= 20 AND Age < 30 THEN '[20,30)'
        WHEN Age >= 30 AND Age < 40 THEN '[30,40)'
        WHEN Age >= 40 AND Age < 50 THEN '[40,50)'
        WHEN Age >= 50 AND Age < 60 THEN '[50,60)'
        WHEN Age >= 60 AND Age <= 70 THEN '[60,70]'
        ELSE 'UNKNOWN' 
    END) AS AgeRange, 
    Gender;

-- Join customers with transactions to get transaction details with age and gender
customer_transactions = JOIN transactions BY CustID, customers_age_group BY ID;

-- Group by AgeRange and Gender
grouped_data = GROUP customer_transactions BY (AgeRange, Gender);

-- Compute Min, Max, and Average TransTotal
result = FOREACH grouped_data GENERATE
    group.AgeRange AS AgeRange, 
    group.Gender AS Gender, 
    MIN(customer_transactions.TransTotal) AS MinTransTotal, 
    MAX(customer_transactions.TransTotal) AS MaxTransTotal, 
    AVG(customer_transactions.TransTotal) AS AvgTransTotal;

-- Store the results
STORE result INTO '/user/Project1/output/query_4_3' USING PigStorage(',');
