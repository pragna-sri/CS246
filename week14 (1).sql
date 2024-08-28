--task1
create database week14a;
use week14a;

--task2a
create table account(
account_number varchar(5),
balance integer,
original_balance integer,
primary key(account_number)
);

--task2b
create table move_funds(
from_acc varchar(5),
to_acc varchar(5),
transfer_amount integer
);

--task2c
create table move_funds_log(
account_number varchar(5),
move_fund_type SET('deposit','withdraw'),
amount integer,
timestamp datetime,
foreign key(account_number)
references account(account_number)
);

--task3a
LOAD DATA LOCAL INFILE '/home/mathsuser/Downloads/account.csv'
INTO TABLE account FIELDS terminated by ',' LINES terminated by '\n'
ignore 1 rows;

--task3b
LOAD DATA LOCAL INFILE '/home/mathsuser/Downloads/trnx.csv'
INTO TABLE move_funds FIELDS terminated by ',' LINES terminated by '\n'
ignore 1 rows;

--task4a
CREATE USER 'saradhi'@'localhost' IDENTIFIED BY 'password';

--task4b
CREATE USER 'pbhaduri'@'localhost' IDENTIFIED BY 'password1';

--task5a
GRANT SELECT, INSERT, UPDATE, DELETE ON week14a.account TO 'saradhi'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON week14a.move_funds TO 'saradhi'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON week14a.move_funds_log TO 'saradhi'@'localhost';


grant all privileges on week14a.* to 'saradhi'@'localhost';

GRANT SELECT, INSERT, UPDATE, DELETE ON week14a.account TO 'pbhaduri'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON week14a.move_funds TO 'pbhaduri'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE ON week14a.move_funds_log TO 'pbhaduri'@'localhost';
GRANT all privileges ON week14a.* TO 'pbhaduri'@'localhost';


--task6a
DELIMITER //
CREATE PROCEDURE transfer_funds_1(IN from_acc VARCHAR(5), IN to_acc VARCHAR(5), IN transfer_amt INT)
BEGIN
  DECLARE from_bal INT;
  DECLARE to_bal INT;
  DECLARE insufficient_funds CONDITION FOR SQLSTATE '45000';
  
  START TRANSACTION;
  
  -- Get the balance of the source account
  SELECT balance INTO from_bal FROM account WHERE account_number = from_acc;
  
  -- Check if the source account has enough funds to transfer
  IF from_bal < transfer_amt THEN
    SIGNAL insufficient_funds SET MESSAGE_TEXT = 'Insufficient funds';
  END IF;
  
  -- Get the balance of the destination account
  SELECT balance INTO to_bal FROM account WHERE account_number = to_acc;
  
  -- Update the balances of the accounts
  UPDATE account SET balance = from_bal - transfer_amt WHERE account_number = from_acc;
  UPDATE account SET balance = to_bal + transfer_amt WHERE account_number = to_acc;
  
  -- Insert a log record of the transaction
  INSERT INTO move_funds_log(account_number, move_fund_type,use week14a;

LOCK TABLES account WRITE;

CALL transfer_funds_1('52149', '15873', 250);

UNLOCK TABLES; amount, timestamp)
  VALUES (from_acc, 'withdraw', transfer_amt, NOW());

  INSERT INTO move_funds_log(account_number, move_fund_type, amount, timestamp)
  VALUES (to_acc, 'deposit', transfer_amt, NOW());
  
  COMMIT;
END//
DELIMITER ;

GRANT EXECUTE ON PROCEDURE week14a.transfer_funds_1 TO 'saradhi'@'localhost';


GRANT EXECUTE ON PROCEDURE week14a.transfer_funds_1 TO 'pbhaduri'@'localhost';

--task6b
mysql -u saradhi -p
LOCK TABLES week14a.account READ;
SET @from_acc = (SELECT from_acc FROM move_funds LIMIT 1);
SET @to_acc = (SELECT to_acc FROM move_funds LIMIT 1);
SET @amount = (SELECT transfer_amount FROM move_funds LIMIT 1);
CALL transfer_funds_1(@from_acc, @to_acc, @amount);
UNLOCK TABLES;


--task6c
mysql -u pbhaduri -p
LOCK TABLES account READ;
SET @from_acc = (SELECT from_acc FROM move_funds LIMIT 1);
SET @to_acc = (SELECT to_acc FROM move_funds LIMIT 1);
SET @amount = (SELECT transfer_amount FROM move_funds LIMIT 1);
CALL transfer_funds_1(@from_acc, @to_acc, @amount);
UNLOCK TABLES;


--task7
DELIMITER //
CREATE PROCEDURE transfer_funds_2 (IN from_acc CHAR(5), IN to_acc CHAR(5), IN transfer_amt INT)
BEGIN
    DECLARE from_balance INT;
    DECLARE to_balance INT;
   
    START TRANSACTION;
   
    SELECT balance INTO from_balance FROM account WHERE account_number = from_acc;
   
    IF from_balance < 100 OR from_balance < transfer_amt/2 THEN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Insufficient balance for transfer';
    ELSE
        UPDATE account SET balance = balance - transfer_amt WHERE account_number = from_acc;
        UPDATE account SET balance = balance + transfer_amt WHERE account_number = to_acc;
        INSERT INTO move_funds_log (account_number, move_fund_type, amount, timestamp) VALUES (from_acc, 'withdraw', transfer_amt, NOW());

	INSERT INTO move_funds_log (account_number, move_fund_type, amount, timestamp) VALUES (from_acc, 'deposit', transfer_amt, NOW());
        COMMIT;
    END IF;
   
END  //

DELIMITER ;


GRANT EXECUTE ON PROCEDURE week14a.transfer_funds_2 TO 'saradhi'@'localhost';


GRANT EXECUTE ON PROCEDURE week14a.transfer_funds_2 TO 'pbhaduri'@'localhost';

--task8
DELIMITER $$
CREATE PROCEDURE main_transfer_2()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE from_acc2 CHAR(5);
    DECLARE to_acc2 CHAR(5);
    DECLARE transfer_amt2 INT;
    DECLARE cur CURSOR FOR SELECT from_acc, to_acc, transfer_amount FROM move_funds;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
   
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO from_acc2, to_acc2, transfer_amt2;
        IF done THEN
            LEAVE read_loop;
        END IF;
       CALL transfer_funds_2(from_acc2, to_acc2, transfer_amt2);
    END LOOP;
   
    CLOSE cur;
END$$
DELIMITER ;

--task9
use week14a;
call main_transfer_2();


--task10a
use week14a;
SELECT  account_number, SUM(amount) AS total_withdraw
FROM move_funds_log
WHERE move_fund_type = 'withdraw'
GROUP BY account_number;

--task10b
SELECT account_number, SUM(amount) AS total_deposit
FROM move_funds_log
WHERE move_fund_type = 'deposit'
GROUP BY account_number;

--task10c
SELECT a.account_number, a.original_balance,
COALESCE(b.total_deposit, 0) AS total_deposit, COALESCE(c.total_withdraw, 0) AS total_withdraw,
(a.original_balance + COALESCE(b.total_deposit, 0) - COALESCE(c.total_withdraw, 0)) AS new_balance
FROM account a
LEFT JOIN (SELECT account_number, SUM(amount) AS total_deposit
FROM move_funds_log WHERE move_fund_type = 'deposit' GROUP BY account_number) b ON a.account_number = b.account_number
LEFT JOIN (SELECT  account_number, SUM(amount) AS total_withdraw
FROM move_funds_log WHERE move_fund_type = 'withdraw' GROUP BY account_number) c ON a.account_number = c.account_number;



