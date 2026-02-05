/* Step 1: Create a cleaned claims dataset */
data claims_clean;
    set claims_raw;
    
    /* Handle missing claim amounts */
    if claim_amount = . then claim_amount = 0;

    /* Create claim severity flag */
    if claim_amount >= 10000 then severity = "High";
    else if claim_amount >= 3000 then severity = "Medium";
    else severity = "Low";

    /* Extract year from claim date */
    claim_year = year(claim_date);

    format claim_date date9.;
run;


/* Step 2: Aggregate claims at customer level */
proc sql;
    create table customer_claims as
    select 
        customer_id,
        count(*) as num_claims,
        sum(claim_amount) as total_claim_amount,
        avg(claim_amount) as avg_claim_amount
    from claims_clean
    group by customer_id;
quit;


/* Step 3: Sort for reporting */
proc sort data=customer_claims;
    by descending total_claim_amount;
run;


/* Step 4: Join with customer master data */
proc sql;
    create table final_dataset as
    select 
        a.customer_id,
        b.customer_name,
        b.city,
        b.join_date,
        a.num_claims,
        a.total_claim_amount,
        a.avg_claim_amount
    from customer_claims a
    left join customers b
        on a.customer_id = b.customer_id;
quit;


/* Step 5: Filter high-value customers */
data high_value_customers;
    set final_dataset;
    where total_claim_amount > 20000;
run;
