SHOW DATABASES;


CREATE DATABASE TRANSACTION;

USE TRANSACTION;

SHOW tables;

-- LOADING Table in MySQL from CSV file

CREATE TABLE transaction_summary (
    Account_No varchar(255),
	Date DATE,
	Transaction_Details TEXT,
	Chq_No varchar(255),
	Value_Date DATE,
	Withdrawal_Amt DECIMAL(20, 2),
	Deposit_Amt DECIMAL(20, 2),
	Balance_Amt DECIMAL(20, 2)
);

LOAD DATA INFILE "B:/data/assignment_2/transaction_summary.csv"
INTO TABLE transaction_summary
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Account_No, @Date, Transaction_Details, Chq_No, @Value_Date, @Withdrawal_Amt, @Deposit_Amt, @Balance_Amt)
SET
    Date = STR_TO_DATE(@Date, '%d-%b-%y'),
    Value_Date = STR_TO_DATE(@Value_Date, '%d-%b-%y'),
    Withdrawal_Amt = IF(@Withdrawal_Amt = '', 0, @Withdrawal_Amt),
    Deposit_Amt = IF(@Deposit_Amt = '', 0, @Deposit_Amt),
    Balance_Amt = IF(@Balance_Amt = '', 0, @Balance_Amt);

SELECT * FROM transaction_summary;

-- Creating balance_amt column

ALTER TABLE transaction_summary 
ADD COLUMN ID int(255) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY; 

SELECT
    Deposit_Amt,
    Withdrawal_Amt,
    Balance_Amt,
    (SELECT SUM(ts2.Deposit_Amt - ts2.Withdrawal_Amt) 
     FROM transaction_summary ts2 
     WHERE ts2.ID <= ts1.ID) AS cumulative_balance
FROM
    transaction_summary ts1;

SELECT Deposit_Amt, Withdrawal_Amt, Balance_Amt,
SUM(Deposit_Amt-Withdrawal_Amt) OVER (ORDER BY ID) AS Balance 
FROM transaction_summary ts ;

   





