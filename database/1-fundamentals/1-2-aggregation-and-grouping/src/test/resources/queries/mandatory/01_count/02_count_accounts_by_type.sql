-- 2. Count the number of accounts for each account type.
select account_type, count(*) as total_accounts from accounts
group by account_type;