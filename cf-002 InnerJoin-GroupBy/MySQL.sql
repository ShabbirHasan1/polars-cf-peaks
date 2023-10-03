-- Read from the master and fact table files
LOAD DATA INFILE 'Inbox/Master.csv' INTO TABLE Master;
LOAD DATA INFILE 'Inbox/FactTable.csv' INTO TABLE FactTable;

-- Perform the operations and write to the first output CSV file
SELECT Shop, Product, Quantity, Unit_Price, (Quantity * Unit_Price) AS Amount
INTO OUTFILE 'Outbox/Detail_Result.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM (
    SELECT *
    FROM FactTable
    WHERE Shop BETWEEN 'S90' AND 'S99'
    AND (Quantity * Unit_Price) > 100000
    AND EXISTS (
        SELECT 1
        FROM Master
        WHERE Master.Product = FactTable.Product
        AND Master.Style = 'F'
    )
) AS FilteredFactTable;

-- Perform the operations and write to the second output CSV file
SELECT Shop, Product, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Quantity * Unit_Price) AS Sum_Amount
INTO OUTFILE 'Outbox/Summary_Result.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM FilteredFactTable
GROUP BY Shop, Product;
