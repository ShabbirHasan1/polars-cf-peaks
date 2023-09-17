-- Load the Master.csv file
LOAD DATA INFILE 'Inbox/Master.csv' INTO TABLE master;

-- Display the first 10 rows of the master table
SELECT * FROM master LIMIT 10;

-- Load the source file
LOAD DATA INFILE 'Inbox/100M_Fact.csv' INTO TABLE fact_table;

-- Display the first 10 rows of the fact_table
SELECT * FROM fact_table LIMIT 10;

-- Perform an inner join on the 'Product' and 'Style' columns, and calculate the 'Amount'
SELECT fact_table.*, master.*, (fact_table.Quantity * master.Unit_Price) AS Amount
FROM fact_table
INNER JOIN master ON fact_table.Product = master.Product AND fact_table.Style = master.Style;

-- Save the result to a new file
INTO OUTFILE 'Outbox/Polars_Sinkcsv_Innerjoin_Result_100M_Fact.csv';

-- Load the result file
LOAD DATA INFILE 'Outbox/Polars_Sinkcsv_Innerjoin_Result_100M_Fact.csv' INTO TABLE result_table;

-- Display the first 10 rows of the result table
SELECT * FROM result_table LIMIT 10;
