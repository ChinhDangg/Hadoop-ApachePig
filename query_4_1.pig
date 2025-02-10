-- Load Customers Dataset
customers = LOAD '/user/Project1/data/customers.csv'
    USING PigStorage(',')
    AS (ID: INT, Name: CHARARRAY, Age: INT, Gender: CHARARRAY, CountryCode: INT, Salary: DOUBLE);

-- Load Transactions Dataset
transactions = LOAD '/user/Project1/data/transactions.csv'
    USING PigStorage(',')
    AS (TransID: INT, CustID: INT, TransTotal: DOUBLE, TransNumItems: INT, TransDesc: CHARARRAY);

-- Count transactions per customer
trans_count = GROUP transactions BY CustID;
customer_trans_count = FOREACH trans_count GENERATE group AS CustID, COUNT(transactions) AS TransCount;

-- Use MIN() to get the minimum transaction count (instead of LIMIT 1)
min_trans_count_value = FOREACH (GROUP customer_trans_count ALL) GENERATE MIN(customer_trans_count.TransCount) AS MinTrans;

-- Filter all customers with MinTrans value
customer_info_trans_count = JOIN customer_trans_count BY CustID, customers BY ID;
filtered_customers = FILTER customer_info_trans_count BY TransCount == min_trans_count_value.MinTrans;

-- Select Name and Transaction Count
result = FOREACH filtered_customers GENERATE Name, TransCount;

-- Store the result
STORE result INTO '/user/Project1/output/query_4_1' USING PigStorage(',');




