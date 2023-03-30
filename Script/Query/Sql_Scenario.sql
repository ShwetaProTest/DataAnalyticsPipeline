Query1:
select distinct(retraced_pqdb.user_login_locations_csv.user_id) as user_id  from retraced_pqdb.user_login_locations_csv
JOIN retraced_pqdb.user_companies_csv  on retraced_pqdb.user_login_locations_csv.user_id = retraced_pqdb.user_companies_csv.user_id
where retraced_pqdb.user_companies_csv.has_agreed_with_terms = '1'

Query2:
select distinct(retraced_pqdb.user_companies_csv.company_id) as company_id  from retraced_pqdb.user_companies_csv
    JOIN  retraced_pqdb.user_login_locations_csv on retraced_pqdb.user_companies_csv.user_id = retraced_pqdb.user_login_locations_csv.user_id
where retraced_pqdb.user_companies_csv.has_agreed_with_terms = '1' 

Query3:
select distinct(retraced_pqdb.user_companies_csv.company_id) as company_id  from retraced_pqdb.user_companies_csv
JOIN  retraced_pqdb.user_login_locations_csv on retraced_pqdb.user_companies_csv.user_id = retraced_pqdb.user_login_locations_csv.user_id
where retraced_pqdb.user_companies_csv.has_agreed_with_terms = '1' and TRY(date_parse(retraced_pqdb.user_companies_csv.user_activation_last_notified_at, '%Y-%m-%d %H:%i:%s')) >= date_trunc('month', current_date ) - interval '30' day