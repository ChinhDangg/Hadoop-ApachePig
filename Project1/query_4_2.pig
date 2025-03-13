-- Load Customers Dataset
customers = LOAD '/user/Project1/data/customers.csv'
    USING PigStorage(',')
    AS (ID: INT, Name: CHARARRAY, Age: INT, Gender: CHARARRAY, CountryCode: INT, Salary: DOUBLE);

-- Group customers by CountryCode
grouped_by_country = GROUP customers BY CountryCode;

-- Count number of customers per CountryCode
country_customer_count = FOREACH grouped_by_country GENERATE group AS CountryCode, COUNT(customers) AS NumCustomers;

-- Filter CountryCodes with customers > 5000 or < 2000
filtered_countries = FILTER country_customer_count BY NumCustomers > 5000 OR NumCustomers < 2000;

-- Store the result
STORE filtered_countries INTO '/user/Project1/output/query_4_2' USING PigStorage(',');
