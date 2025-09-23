SELECT event_name, num_seats, section_name, row_name, seat_num, last_seat, seat_increment, total_events, team,CONVERT(varchar, event_date, 120) AS event_date, CONVERT(varchar, event_day, 120) AS event_day, season_name, arena_name,  manifest_name, game_number, event_name_long, event_type, section_type, row_desc,
returnable, dsps_allowed, season_id, event_id, section_id, row_id, row_index, acct_id, name, class_id, class_name, price_code, price_code_desc, full_price,
purchase_price, inet_purchase_price, pc_ticket, pc_tax, pc_licfee, pc_other1, trans_type, comp_code, comp_name, group_flag, consignment, group_sales_id,
group_sales_name, upd_user, CONVERT(varchar, upd_datetime, 120) AS upd_datetime,CONVERT(varchar, printed_datetime, 120) AS printed_datetime, disc_code,
disc_name, disc_amount, surchg_code, surchg_name, sell_location_id, sell_location, aisle, order_num, order_line_item, paid, plan_event_name,
plan_total_events, plan_event_id, plan_type, CONVERT(varchar, plan_datetime, 120) AS plan_datetime, pricing_method, block_full_price, block_purchase_price, 
orig_price_code, add_usr, CONVERT(varchar, add_datetime, 120) AS add_datetime, section_name_right, tm_event_name, tm_row_name, tm_section_name,
season_line1, acct_rep_id, acct_rep_full_name, delivery_method, delivery_method_name, portal_low, portal_high, portal_mid_seat_num, ticket_status, ticket_status_code,
status, CONVERT(varchar, return_datetime, 120) AS return_datetime,full_name_1, full_name_2, company_name, name_last_first_mi, street_addr_1, street_addr_2, city, state, zip,country, phone_day, acct_type, acct_type_desc, salutation, priority, CONVERT(varchar, customer_add_date, 120) AS customer_add_date, email_addr, ticket_type_code, ticket_type, ticket_type_category, renewal_ind, cust_name_id, comp_requested_by, name_type, owner_name_full, owner_name, country_code, language_name
FROM tbl


----------------------------------------------------Esquema en SQL Anywhere---------------------------------------------------------------------------------------------- 
SELECT 
    sc.column_name,
    sc.base_type_str AS data_type
FROM systabcol sc
JOIN systable st ON sc.table_id = st.table_id
WHERE st.table_name = 'v_ticket'
ORDER BY sc.column_id;

----------------------------------------------Esquema en BQ-------------------------------------------------------------------------------------------------------------
SELECT 
  column_name, 
  data_type
FROM 
  `sacred-epigram-307901.TKM_RV_SALES.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = '900_Archtics_F1MEXICO'
AND data_type = 'BOOL'
