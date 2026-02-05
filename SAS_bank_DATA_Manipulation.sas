
/*----------------------------------------------------------
   STEP 1: Import the two input CSV files
----------------------------------------------------------*/

/* Import accounts.csv */
proc import datafile="/home/u64404552/sasuser.v94/datasets/accounts.csv"
    out=accounts
    dbms=csv
    replace;
    guessingrows=max;
run;

/* Import transactions.csv */
proc import datafile="/home/u64404552/sasuser.v94/datasets/transactions.csv"
    out=transactions
    dbms=csv
    replace;
    guessingrows=max;
run;

/*----------------------------------------------------------
   STEP 2: Prepare transactions for aggregation
            - Normalize transaction_type
            - Robustly create SAS date (tx_date) from transaction_date
            - Convert debit amounts to negative values
----------------------------------------------------------*/
data tx_prep;
    set transactions;

    /* Normalize type and strip spaces */
    transaction_type = strip(lowcase(transaction_type));

    /* Create a proper SAS date in tx_date regardless of source type */
    length tx_date 8;
    if vtypex('transaction_date') = 'C' then do;
        /* Character date like '17-12-2025' -> numeric SAS date */
        tx_date = input(strip(transaction_date), anydtdte.);
    end;
    else if vtypex('transaction_date') = 'N' then do;
        /* Already numeric (likely SAS date) */
        tx_date = transaction_date;
    end;
    format tx_date ddmmyy10.;

    /* Ensure signed amounts: debit negative, credit positive */
    if transaction_type = "debit"  then signed_amount = -transaction_amount;
    else if transaction_type = "credit" then signed_amount =  transaction_amount;
    else signed_amount = .;  /* unexpected type, if any */
run;

/*----------------------------------------------------------
   STEP 3: Create BALANCE TABLE
            - total credits
            - total debits
            - final balance
            - join account_name
----------------------------------------------------------*/

proc sql;
    create table balance as
    select 
        a.account_id,
        a.account_name,
        sum(case when t.transaction_type = "credit" then t.transaction_amount else 0 end) as total_credits,
        sum(case when t.transaction_type = "debit"  then t.transaction_amount else 0 end) as total_debits,
        sum(t.signed_amount) as balance
    from accounts as a
    left join tx_prep as t
        on a.account_id = t.account_id
    group by a.account_id, a.account_name
    order by a.account_id;
quit;

/* Export balance.csv */
proc export data=balance
    outfile="/home/u64404552/sasuser.v94/datasets/balance.csv"
    dbms=csv
    replace;
run;

/*----------------------------------------------------------
   STEP 4: Create TRANSACTION SUMMARY TABLE
            - total transactions
            - avg amount
            - max/min transaction amount
----------------------------------------------------------*/

proc sql;
    create table transaction_summary as
    select 
        account_id,
        count(*)                         as total_txns,
        mean(transaction_amount)         as avg_amount,
        max(transaction_amount)          as max_txn,
        min(transaction_amount)          as min_txn
    from transactions
    group by account_id
    order by account_id;
quit;

/* Export transaction_summary.csv */
proc export data=transaction_summary
    outfile="/home/u64404552/sasuser.v94/datasets/transaction_summary.csv"
    dbms=csv
    replace;
run;

/*----------------------------------------------------------
   STEP 5: Create DATE-LEVEL TRANSACTION COUNTS
            - total/debit/credit counts per date
            - optional amounts + net
----------------------------------------------------------*/
proc sql;
    create table transactions_by_date_num as
    select
        tx_date,
        count(*)                                                     as total_txns,
        sum(case when transaction_type='debit'  then 1 else 0 end)   as debit_txns,
        sum(case when transaction_type='credit' then 1 else 0 end)   as credit_txns,
        /* Optional amount-level checks (drop if you only want counts) */
        sum(case when transaction_type='credit' then transaction_amount else 0 end) as total_credits_amt,
        sum(case when transaction_type='debit'  then transaction_amount else 0 end) as total_debits_amt,
        sum(signed_amount)                                            as net_amount
    from tx_prep
    where not missing(tx_date)
    group by tx_date
    order by tx_date;
quit;

/* Convert SAS date to character to guarantee it shows up in the CSV */
data transactions_by_date;
    set transactions_by_date_num;
    length transaction_date $10;
    transaction_date = put(tx_date, ddmmyy10.);
    drop tx_date;
run;

/* Export transactions_by_date.csv */
proc export data=transactions_by_date
    outfile="/home/u64404552/sasuser.v94/datasets/transactions_by_date.csv"
    dbms=csv
    replace;
run;

