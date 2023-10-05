-- Connect to your PostgreSQL database
\c your_database

-- Import the necessary CSV files
COPY master FROM 'Inbox/Master.csv' DELIMITER ',' CSV HEADER;
COPY fact_table FROM 'Inbox/' || current_setting('myvars.sys_argv') DELIMITER ',' CSV HEADER;

-- Filter the master table
CREATE VIEW filter_master AS SELECT * FROM master WHERE Style = 'F';

-- Create the detail result view
CREATE VIEW detail_result AS 
SELECT fact_table.*, Quantity * Unit_Price AS Amount 
FROM fact_table 
JOIN filter_master ON fact_table.Product = filter_master.Product AND fact_table.Style = filter_master.Style
WHERE Shop >= 'S90' AND Shop <= 'S99' AND Amount > 100000;

-- Create the summary result view
CREATE VIEW summary_result AS 
SELECT Shop, Product, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Amount) AS Sum_Amount 
FROM detail_result 
GROUP BY Shop, Product;

-- Export the results to CSV files
COPY (SELECT * FROM detail_result) TO 'Outbox/PostgreSQL_Detail_Result_' || current_setting('myvars.sys_argv') WITH CSV HEADER;
COPY (SELECT * FROM summary_result) TO 'Outbox/PostgreSQL_Summary_Result_' || current_setting('myvars.sys_argv') WITH CSV HEADER;
