/**************************************************************
   STEP 1 — Define a macro to process multiple years of data
**************************************************************/
%let start_year = 2021;
%let end_year   = 2023;

%macro build_yearly_claims;

    %do yr = &start_year %to &end_year;

        /* Load each year's data and clean */
        data claims_&yr.;
            set claims_all;

            /* Keep only given year */
            if year(claim_date) = &yr.;

            /* Standardize missing values */
            if claim_amount = . then claim_amount = 0;

            /* Create severity bucket */
            if claim_amount >= 15000 then severity = "High";
            else if claim_amount >= 5000 then severity = "Medium";
            else severity = "Low";

            claim_year = &yr.;
            format claim_date date9.;
        run;

    %end;

%mend;

%build_yearly_claims;


/**************************************************************
   STEP 2 — Stack data across years
**************************************************************/
data claims_clean;
    set claims_:;
run;


/**************************************************************
   STEP 3 — Aggregate using PROC SUMMARY (similar to groupby)
**************************************************************/
proc summary data=claims_clean nway;
    class customer_id;
    var claim_amount;
    output out=agg_claims
        n=num_claims
        sum=total_claim_amount
        mean=avg_claim_amount;
run;

/**************************************************************
   STEP 4 — SQL Join with customer dimension
**************************************************************/
proc sql;
    create table combined_data as
    select 
        a.customer_id,
        b.customer_name,
        b.region,
        b.join_date,
        a.num_claims,
        a.total_claim_amount,
        a.avg_claim_amount
    from agg_claims a
    left join customers b
        on a.customer_id = b.customer_id;
quit;


/**************************************************************
   STEP 5 — Create loyalty score via DATA step
**************************************************************/
data final_output;
    set combined_data;

    /* Customer tenure in years */
    tenure_years = intck('year', join_date, today());

    /* Loyalty heuristic */
    if tenure_years >= 5 and avg_claim_amount < 3000 then loyalty = "Gold";
    else if tenure_years >= 2 then loyalty = "Silver";
    else loyalty = "New";
run;
