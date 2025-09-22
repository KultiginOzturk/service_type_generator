## pco-qa.NSJuly.NS_lkp_service_type_allocated

- num_rows: 0

- schema:
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - allocateReservices: BOOLEAN NULLABLE - 
  - isRervice: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - is_valid_non_labor_charge: BOOLEAN NULLABLE - 
  - expiredCode: BOOLEAN NULLABLE - 
  - visitOverride: FLOAT NULLABLE - 
  - allocation_custom1: FLOAT NULLABLE - 
  - allocation_custom2: FLOAT NULLABLE - 
  - allocation_custom3: FLOAT NULLABLE - 
  - allocation_rev_GP_Resi: FLOAT NULLABLE - 
  - allocation_rev_GP_Comm: FLOAT NULLABLE - 
  - allocation_rev_WD: FLOAT NULLABLE - 
  - allocation_rev_Mosq: FLOAT NULLABLE - 
  - allocation_rev_Snake: FLOAT NULLABLE - 
  - allocation_rev_Other: FLOAT NULLABLE - 
  - allocation_rev_None: FLOAT NULLABLE - 

## pco-qa.NSJuly.NS_lkp_service_type_allocated_unpivoted

- num_rows: 0

- schema:
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - allocateReservices: BOOLEAN NULLABLE - 
  - isRervice: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - is_valid_non_labor_charge: BOOLEAN NULLABLE - 
  - expiredCode: BOOLEAN NULLABLE - 
  - visitOverride: FLOAT NULLABLE - 
  - allocation_custom1: FLOAT NULLABLE - 
  - allocation_custom2: FLOAT NULLABLE - 
  - allocation_custom3: FLOAT NULLABLE - 
  - allocated_bucket_name: STRING NULLABLE - 
  - allocation_percentage: FLOAT NULLABLE - 
  - allocated_bucket_type: STRING NULLABLE - 

## pco-qa.NSJuly.base_subscription_metrics

- num_rows: 27595

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - revenue_recurring: FLOAT NULLABLE - 
  - revenue_one_time: INTEGER NULLABLE - 
  - LaborHours: FLOAT NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - onSiteMin: FLOAT NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - balanceTime: FLOAT NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - keepIn: BOOLEAN NULLABLE - 
  - metric_date: DATE NULLABLE - 
  - Calculated_Hourly_Cost: FLOAT NULLABLE - 
  - is_allocation_defined: BOOLEAN NULLABLE - 
  - is_valid_non_labor_charge: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.client_hourly_costs

- num_rows: 6

- schema:
  - ClientID: STRING NULLABLE - 
  - Reference_Date: DATE NULLABLE - 
  - Total_TTM_COGS: INTEGER NULLABLE - 
  - Total_TTM_Revenue: INTEGER NULLABLE - 
  - Total_TTM_GeneralAdmin: FLOAT NULLABLE - 
  - Total_TTM_OnSite_Labor_Hours: FLOAT NULLABLE - 
  - Calculated_Hourly_Cost: FLOAT NULLABLE - 
  - TTM_GA_Percent: FLOAT NULLABLE - 
  - Number_of_Months_Included: INTEGER NULLABLE - 

## pco-qa.NSJuly.customer_bundle_flags

- num_rows: 21271

- schema:
  - customerID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - has_wd_service: BOOLEAN NULLABLE - 
  - has_mosq_service: BOOLEAN NULLABLE - 
  - has_gp_resi_service: BOOLEAN NULLABLE - 
  - has_gp_comm_service: BOOLEAN NULLABLE - 
  - has_snake_service: BOOLEAN NULLABLE - 
  - has_other_service: BOOLEAN NULLABLE - 
  - is_triple_play_bundle: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.customer_bundle_flags_master_diag

- num_rows: 21679

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - has_wd_service: BOOLEAN NULLABLE - 
  - has_mosq_service: BOOLEAN NULLABLE - 
  - has_gp_resi_service: BOOLEAN NULLABLE - 
  - has_gp_comm_service: BOOLEAN NULLABLE - 
  - has_snake_service: BOOLEAN NULLABLE - 
  - has_other_service: BOOLEAN NULLABLE - 
  - is_triple_play_bundle: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.customer_bundle_status

- num_rows: 24594

- schema:
  - customerID: INTEGER NULLABLE - 
  - is_wd_bundle_customer: BOOLEAN NULLABLE - 
  - bundle_type_derived: STRING NULLABLE - 

## pco-qa.NSJuly.customer_deviation_summary

- num_rows: 20353

- schema:
  - metric_date: DATE NULLABLE - 
  - clientId: STRING NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - sum_actual_revenue: FLOAT NULLABLE - 
  - sum_expected_revenue: FLOAT NULLABLE - 
  - customer_revenue_deviation_pct: FLOAT NULLABLE - 
  - sum_actual_onsite_min: FLOAT NULLABLE - 
  - sum_expected_onsite_min: FLOAT NULLABLE - 
  - customer_onsite_min_deviation_pct: FLOAT NULLABLE - 
  - num_subscriptions_in_scope: INTEGER NULLABLE - 
  - is_price_deviation_alert: BOOLEAN NULLABLE - 
  - is_time_deviation_alert: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.customer_last_appointment_date

- num_rows: 19877

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - last_appointment_date: DATE NULLABLE - 
  - last_recurring_appointment_date: DATE NULLABLE - 

## pco-qa.NSJuly.customer_names

- num_rows: 245270

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - location_name: STRING NULLABLE - 
  - master_account_name: STRING NULLABLE - 

## pco-qa.NSJuly.diag_pisgah_tripleplay_customer_counts

- num_rows: 1

- schema:
  - clientID_examined: STRING NULLABLE - 
  - total_distinct_triple_play_master_accounts: INTEGER NULLABLE - 
  - total_distinct_triple_play_customer_ids: INTEGER NULLABLE - 
  - master_accounts_with_multiple_customer_ids_in_triple_play: INTEGER NULLABLE - 

## pco-qa.NSJuly.enriched_subscription_detail

- num_rows: 28954

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - serviceType_ID: INTEGER NULLABLE - 
  - Revenue: FLOAT NULLABLE - 
  - LaborHours: FLOAT NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - allocated_majorBucket: STRING NULLABLE - 
  - majorBucket_allocation_pct: FLOAT NULLABLE - 
  - is_wd_allocated_service: BOOLEAN NULLABLE - 
  - is_mosq_allocated_service: BOOLEAN NULLABLE - 
  - is_gp_resi_allocated_service: BOOLEAN NULLABLE - 
  - is_gp_comm_allocated_service: BOOLEAN NULLABLE - 
  - is_snake_allocated_service: BOOLEAN NULLABLE - 
  - is_other_allocated_service: BOOLEAN NULLABLE - 
  - onSiteMin: FLOAT NULLABLE - 
  - totalMin: FLOAT NULLABLE - 
  - balanceTime: FLOAT NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - keepIn: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.large_customer_health_report

- num_rows: 2086

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - subAccounts: STRING NULLABLE - 
  - total_revenue_recurring: FLOAT NULLABLE - 
  - total_revenue_one_time: FLOAT NULLABLE - 
  - cm_ntm_dollar: FLOAT NULLABLE - 
  - cm_ntm_perc: FLOAT NULLABLE - 
  - ltm_reservice_rate: FLOAT NULLABLE - 
  - ptm_reservice_rate: FLOAT NULLABLE - 
  - reservice_trend_abs: FLOAT NULLABLE - 
  - ltm_avg_recurring_visit_length: FLOAT NULLABLE - 
  - ptm_avg_recurring_visit_length: FLOAT NULLABLE - 
  - visit_length_trend_pct: FLOAT NULLABLE - 

## pco-qa.NSJuly.lkp_PnL

- num_rows: 0

- schema:
  - Year: INTEGER NULLABLE - 
  - Month: INTEGER NULLABLE - 
  - BoM: DATE NULLABLE - %m/%d/%E4Y
  - EOM: DATE NULLABLE - %m/%d/%E4Y
  - Data_Available: BOOLEAN NULLABLE - 
  - Revenue: INTEGER NULLABLE - 
  - COGS: INTEGER NULLABLE - 
  - Marketing: FLOAT NULLABLE - 
  - GeneralAdmin: FLOAT NULLABLE - 
  - ClientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.NSJuly.lkp_employee_names

- num_rows: 0

- schema:
  - employeeID: INTEGER NULLABLE - 
  - fname: STRING NULLABLE - 
  - lname: STRING NULLABLE - 
  - active: INTEGER NULLABLE - 
  - type: INTEGER NULLABLE - 
  - Long_Visit_Threshold: FLOAT NULLABLE - 
  - ClientID: STRING NULLABLE - 

## pco-qa.NSJuly.lkp_price_recs_by_segment

- num_rows: 0

- schema:
  - ClientID: STRING NULLABLE - 
  - Ref_Date: DATE NULLABLE - %E4Y-%m-%d
  - Margin_Segment_and_SubSegment: STRING NULLABLE - 
  - Rec_PriceIncrease_Perc: FLOAT NULLABLE - 

## pco-qa.NSJuly.lkp_service_list_prices

- num_rows: 0

- schema:
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - list_price: FLOAT NULLABLE - 
  - billing_per_year: INTEGER NULLABLE - 
  - use_list_price: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.lkp_user_list

- num_rows: 0

- schema:
  - userID: INTEGER NULLABLE - 
  - clientID: STRING NULLABLE - 
  - userName: STRING NULLABLE - 
  - userRole: STRING NULLABLE - 
  - userEmail: STRING NULLABLE - 

## pco-qa.NSJuly.lkp_zeroPayers_validity

- num_rows: 0

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - customer_first_name: STRING NULLABLE - 
  - customer_last_name: STRING NULLABLE - 
  - customerName: STRING NULLABLE - 
  - validFreeService: STRING NULLABLE - 
  - dateReviewed: DATE NULLABLE - %m/%d/%E4Y
  - stillGettingFreeService: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.location_to_employee_mapping

- num_rows: 33074

- schema:
  - clientId: STRING NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - primaryEmployeeID: INTEGER NULLABLE - 
  - primary_employee_name: STRING NULLABLE - 
  - total_qualifying_visits: INTEGER NULLABLE - 
  - primary_employee_visit_count: INTEGER NULLABLE - 
  - at_least_3_qualifying_visits: BOOLEAN NULLABLE - 
  - latest_reservice_appointmentID: INTEGER NULLABLE - 
  - latest_reservice_date: DATE NULLABLE - 
  - latest_reservice_employeeID: INTEGER NULLABLE - 
  - latest_reservice_employeeName: STRING NULLABLE - 
  - causal_appointmentID: INTEGER NULLABLE - 
  - causal_appointment_date: DATE NULLABLE - 
  - causal_employeeID: INTEGER NULLABLE - 
  - causal_employee_name: STRING NULLABLE - 

## pco-qa.NSJuly.margin_by_ServiceTypeGroup_allocated

- num_rows: 20

- schema:
  - clientId: STRING NULLABLE - 
  - ServiceTypeGroup: STRING NULLABLE - 
  - customer_count_contributing_to_group: INTEGER NULLABLE - 
  - master_account_count_contributing_to_group: INTEGER NULLABLE - 
  - subscription_count_contributing_to_group: INTEGER NULLABLE - 
  - allocated_subscription_units: FLOAT NULLABLE - 
  - allocated_customer_units: FLOAT NULLABLE - 
  - allocated_master_account_units: FLOAT NULLABLE - 
  - total_allocated_revenue: FLOAT NULLABLE - 
  - total_allocated_labor_hours: FLOAT NULLABLE - 
  - total_allocated_contribution_margin: FLOAT NULLABLE - 
  - avg_allocated_price_per_unit: FLOAT NULLABLE - 
  - avg_allocated_cm_per_unit: FLOAT NULLABLE - 
  - avg_allocated_labor_hours_per_unit: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_ServiceTypeGroup_nonmece

- num_rows: 20

- schema:
  - clientId: STRING NULLABLE - 
  - grouping_type: STRING NULLABLE - 
  - customer_count: INTEGER NULLABLE - 
  - master_account_count: INTEGER NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - total_revenue: FLOAT NULLABLE - 
  - total_labor_hours: FLOAT NULLABLE - 
  - total_contribution_margin: FLOAT NULLABLE - 
  - avg_price_per_customer: FLOAT NULLABLE - 
  - avg_cm_per_customer: FLOAT NULLABLE - 
  - avg_labor_hours_per_customer: FLOAT NULLABLE - 
  - avg_price_per_subscription: FLOAT NULLABLE - 
  - avg_cm_per_subscription: FLOAT NULLABLE - 
  - avg_labor_hours_per_subscription: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_atomic_service

- num_rows: 109

- schema:
  - clientId: STRING NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - customer_count: INTEGER NULLABLE - 
  - master_account_count: INTEGER NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - total_revenue: FLOAT NULLABLE - 
  - total_labor_hours: FLOAT NULLABLE - 
  - total_contribution_margin: FLOAT NULLABLE - 
  - avg_price_per_subscription: FLOAT NULLABLE - 
  - avg_cm_per_subscription: FLOAT NULLABLE - 
  - avg_labor_hours_per_subscription: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_bundle_summary

- num_rows: 19

- schema:
  - clientId: STRING NULLABLE - 
  - grouping_type: STRING NULLABLE - 
  - customer_count: INTEGER NULLABLE - 
  - master_account_count: INTEGER NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - total_revenue: FLOAT NULLABLE - 
  - total_labor_hours: FLOAT NULLABLE - 
  - total_contribution_margin: FLOAT NULLABLE - 
  - avg_price_per_customer: FLOAT NULLABLE - 
  - avg_cm_per_customer: FLOAT NULLABLE - 
  - avg_labor_hours_per_customer: FLOAT NULLABLE - 
  - avg_price_per_subscription: FLOAT NULLABLE - 
  - avg_cm_per_subscription: FLOAT NULLABLE - 
  - avg_labor_hours_per_subscription: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_bundle_summary_master_diag

- num_rows: 29150

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - grouping_type: STRING NULLABLE - 
  - master_account_count: INTEGER NULLABLE - 
  - individual_account_count: INTEGER NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - total_revenue: FLOAT NULLABLE - 
  - total_labor_hours: FLOAT NULLABLE - 
  - total_contribution_margin: FLOAT NULLABLE - 
  - avg_price_per_master_account: FLOAT NULLABLE - 
  - avg_cm_per_master_account: FLOAT NULLABLE - 
  - avg_labor_hours_per_master_account: FLOAT NULLABLE - 
  - avg_price_per_subscription: FLOAT NULLABLE - 
  - avg_cm_per_subscription: FLOAT NULLABLE - 
  - avg_labor_hours_per_subscription: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_category_allocated

- num_rows: 114

- schema:
  - clientId: STRING NULLABLE - 
  - grouping_level: STRING NULLABLE - 
  - grouping_value: STRING NULLABLE - 
  - customer_count: INTEGER NULLABLE - 
  - allocated_subscription_parts_count: INTEGER NULLABLE - 
  - total_allocated_revenue: FLOAT NULLABLE - 
  - total_allocated_labor_hours: FLOAT NULLABLE - 
  - total_allocated_contribution_margin: FLOAT NULLABLE - 
  - avg_allocated_price_per_part: FLOAT NULLABLE - 
  - avg_allocated_cm_per_part: FLOAT NULLABLE - 
  - avg_allocated_labor_hours_per_part: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_category_unbundled

- num_rows: 10

- schema:
  - clientId: STRING NULLABLE - 
  - grouping_level: STRING NULLABLE - 
  - grouping_value: STRING NULLABLE - 
  - customer_count: INTEGER NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - total_revenue: FLOAT NULLABLE - 
  - total_labor_hours: FLOAT NULLABLE - 
  - total_contribution_margin: FLOAT NULLABLE - 
  - avg_price_per_customer: FLOAT NULLABLE - 
  - avg_cm_per_customer: FLOAT NULLABLE - 
  - avg_labor_hours_per_customer: FLOAT NULLABLE - 
  - avg_price_per_subscription: FLOAT NULLABLE - 
  - avg_cm_per_subscription: FLOAT NULLABLE - 
  - avg_labor_hours_per_subscription: FLOAT NULLABLE - 

## pco-qa.NSJuly.margin_by_master_account

- num_rows: 24155

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - metric_date: DATE NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - margin_sub_segment: STRING NULLABLE - 
  - total_revenue_recurring: FLOAT NULLABLE - 
  - total_revenue_one_time: FLOAT NULLABLE - 
  - total_revenue_unfiltered: FLOAT NULLABLE - 
  - labor_hours_forecast_annual: FLOAT NULLABLE - 
  - hourly_cost_ttm: FLOAT NULLABLE - 
  - hourly_cost_ntm: FLOAT NULLABLE - 
  - cogs_ttm_dollar: FLOAT NULLABLE - 
  - cogs_ntm_dollar: FLOAT NULLABLE - 
  - cm_ttm_dollar: FLOAT NULLABLE - 
  - cm_ntm_dollar: FLOAT NULLABLE - 
  - cm_ttm_perc: FLOAT NULLABLE - 
  - cm_ntm_perc: FLOAT NULLABLE - 
  - subsidized_threshold: FLOAT NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - location_count: INTEGER NULLABLE - 
  - master_account_is_excluded: BOOLEAN NULLABLE - 
  - earliest_subscription_date_for_master: TIMESTAMP NULLABLE - 
  - latest_contract_change_date_for_master: TIMESTAMP NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.masterAccount_to_employee_mapping

- num_rows: 19765

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - primaryEmployeeID: INTEGER NULLABLE - 
  - primary_employee_name: STRING NULLABLE - 
  - latest_reservice_appointmentID: INTEGER NULLABLE - 
  - latest_reservice_date: DATE NULLABLE - 
  - latest_reservice_employeeID: INTEGER NULLABLE - 
  - latest_reservice_employeeName: STRING NULLABLE - 
  - prior_visit_appointmentID: INTEGER NULLABLE - 
  - prior_visit_date: DATE NULLABLE - 
  - prior_visit_employeeID: INTEGER NULLABLE - 
  - prior_visit_employee_name: STRING NULLABLE - 

## pco-qa.NSJuly.master_account_deviation_summary

- num_rows: 18979

- schema:
  - metric_date: DATE NULLABLE - 
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - sum_actual_revenue: FLOAT NULLABLE - 
  - sum_expected_revenue: FLOAT NULLABLE - 
  - master_revenue_deviation_pct: FLOAT NULLABLE - 
  - sum_actual_onsite_min: FLOAT NULLABLE - 
  - sum_expected_onsite_min: FLOAT NULLABLE - 
  - master_onsite_min_deviation_pct: FLOAT NULLABLE - 
  - num_subscriptions_in_scope: INTEGER NULLABLE - 
  - num_individual_accounts_in_scope: INTEGER NULLABLE - 
  - is_price_deviation_alert: BOOLEAN NULLABLE - 
  - is_time_deviation_alert: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.master_client_exclusion_flags

- num_rows: 24155

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - total_master_client_revenue: FLOAT NULLABLE - 
  - is_calculated_zero_paying_master_client: BOOLEAN NULLABLE - 
  - override_status: STRING NULLABLE - 
  - is_exclude_from_analysis: BOOLEAN NULLABLE - 

## pco-qa.NSJuly.monthly_bottom_100_cohort

- num_rows: 449

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 

## pco-qa.NSJuly.monthly_bottom_100_health_details

- num_rows: 449

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - subAccounts: STRING NULLABLE - 
  - total_revenue_recurring: FLOAT NULLABLE - 
  - total_revenue_one_time: FLOAT NULLABLE - 
  - cm_ntm_dollar: FLOAT NULLABLE - 
  - cm_ntm_perc: FLOAT NULLABLE - 
  - customer_price_realization_pct: FLOAT NULLABLE - 
  - historical_reservice_rate: FLOAT NULLABLE - 
  - has_consecutive_reservice_history: BOOLEAN NULLABLE - 
  - avg_visit_time_deviation_pct: FLOAT NULLABLE - 

## pco-qa.NSJuly.monthly_bottom_100_unprofitable

- num_rows: 869

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - profitability_rank_in_client: INTEGER NULLABLE - 

## pco-qa.NSJuly.monthly_pnl_analytics

- num_rows: 453

- schema:
  - ClientID: STRING NULLABLE - 
  - Office_Identifier: STRING NULLABLE - 
  - month_start: DATE NULLABLE - 
  - Revenue: BIGNUMERIC NULLABLE - 
  - COGS: BIGNUMERIC NULLABLE - 
  - Selling_Advertising: BIGNUMERIC NULLABLE - 
  - General_Admin_Expenses: BIGNUMERIC NULLABLE - 
  - rows_with_cast_error: INTEGER NULLABLE - 
  - ignored_amount: BIGNUMERIC NULLABLE - 
  - total_amount_all_rows: BIGNUMERIC NULLABLE - 

## pco-qa.NSJuly.monthly_pricing_review

- num_rows: 198

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - total_revenue_recurring: FLOAT NULLABLE - 
  - total_expected_revenue: FLOAT NULLABLE - 
  - customer_price_realization_pct: FLOAT NULLABLE - 
  - cm_ntm_dollar: FLOAT NULLABLE - 
  - cm_ntm_perc: FLOAT NULLABLE - 

## pco-qa.NSJuly.pnl_mapping_config

- num_rows: 73

- schema:
  - client_id: STRING REQUIRED - 
  - header_1_pattern: STRING NULLABLE - 
  - header_2_pattern: STRING NULLABLE - 
  - header_3_pattern: STRING NULLABLE - 
  - header_4_pattern: STRING NULLABLE - 
  - aggregate_line_item: STRING REQUIRED - 
  - is_negate: BOOLEAN REQUIRED - 
  - priority: INTEGER REQUIRED - 

## pco-qa.NSJuly.subscription_analytics_base

- num_rows: 27595

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - revenue_recurring: FLOAT NULLABLE - 
  - revenue_one_time: INTEGER NULLABLE - 
  - LaborHours: FLOAT NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - onSiteMin: FLOAT NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - balanceTime: FLOAT NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - keepIn: BOOLEAN NULLABLE - 
  - metric_date: DATE NULLABLE - 
  - Calculated_Hourly_Cost: FLOAT NULLABLE - 
  - is_allocation_defined: BOOLEAN NULLABLE - 
  - is_valid_non_labor_charge: BOOLEAN NULLABLE - 
  - has_wd_service: BOOLEAN NULLABLE - 
  - has_mosq_service: BOOLEAN NULLABLE - 
  - has_gp_resi_service: BOOLEAN NULLABLE - 
  - has_gp_comm_service: BOOLEAN NULLABLE - 
  - has_snake_service: BOOLEAN NULLABLE - 
  - has_other_service: BOOLEAN NULLABLE - 
  - is_triple_play_bundle: BOOLEAN NULLABLE - 
  - actual_annual_revenue: FLOAT NULLABLE - 
  - expected_revenue_peer_average: FLOAT NULLABLE - 
  - expected_revenue_list_price: FLOAT NULLABLE - 
  - revenue_deviation_pct: FLOAT NULLABLE - 
  - actual_avg_visit_min: FLOAT NULLABLE - 
  - expected_avg_visit_min: FLOAT NULLABLE - 
  - avg_visit_min_deviation_pct: FLOAT NULLABLE - 
  - expected_peer_group_size: INTEGER NULLABLE - 

## pco-qa.NSJuly.subscription_deviation_metrics

- num_rows: 22590

- schema:
  - metric_date: DATE NULLABLE - 
  - clientId: STRING NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - actual_annual_revenue: FLOAT NULLABLE - 
  - expected_revenue_peer_average: FLOAT NULLABLE - 
  - expected_revenue_list_price: FLOAT NULLABLE - 
  - revenue_deviation_pct: FLOAT NULLABLE - 
  - actual_avg_visit_min: FLOAT NULLABLE - 
  - expected_avg_visit_min: FLOAT NULLABLE - 
  - avg_visit_min_deviation_pct: FLOAT NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - expected_peer_group_size: INTEGER NULLABLE - 

## pco-qa.NSJuly.temp_llm_pnl_mappings

- num_rows: 14

- schema:
  - client_id: STRING NULLABLE - 
  - header_1_pattern: STRING NULLABLE - 
  - header_2_pattern: STRING NULLABLE - 
  - header_3_pattern: STRING NULLABLE - 
  - header_4_pattern: INTEGER NULLABLE - 
  - aggregate_line_item: STRING NULLABLE - 
  - is_negate: BOOLEAN NULLABLE - 
  - priority: INTEGER NULLABLE - 

## pco-qa.NSMarch.AccelPayments

- num_rows: 119338

- schema:
  - APPLIED_PAYMENT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - PAYMENT_ID: STRING NULLABLE - 
  - TICKET_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - DATE_APPLIED: STRING NULLABLE - 
  - APPLIED_BY: STRING NULLABLE - 
  - APPLIED_AMOUNT: STRING NULLABLE - 
  - TAX_COLLECTED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 
  - dupePayment: BOOLEAN NULLABLE - 
  - isFirst: BOOLEAN NULLABLE - 

## pco-qa.NSMarch.ChatGPT_Accel_Mismatch Correction

- num_rows: 4972

- schema:
  - ticketID: STRING NULLABLE - 
  - clientID: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - sumItems: FLOAT NULLABLE - 
  - incorrectPushedCharge: FLOAT NULLABLE - 
  - correctedServiceCharge: FLOAT NULLABLE - 
  - currentServiceCharge: FLOAT NULLABLE - 
  - status: STRING NULLABLE - 

## pco-qa.NSMarch.Gemini_AccelFix_staging

- num_rows: 4972

- schema:
  - ticketID: STRING NULLABLE - 
  - clientID: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - sumItems: FLOAT NULLABLE - 
  - originalServiceCharge: FLOAT NULLABLE - 
  - incorrectPushedCharge_rounded: FLOAT NULLABLE - 
  - correctedServiceCharge: FLOAT NULLABLE - 
  - currentServiceCharge_rounded: FLOAT NULLABLE - 
  - status: STRING NULLABLE - 

## pco-qa.NSMarch.NS_Customer_Detail

- num_rows: 28285

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - ntmProductionForecast: FLOAT NULLABLE - 
  - ntmScheduledVisits: INTEGER NULLABLE - 
  - revGreaterThanNormal: FLOAT NULLABLE - 
  - dollarMarketingBenefit: FLOAT NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - avgOnsiteMinPerRecurringVisit: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - ntmForecastUnpaidVisits: FLOAT NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - estMinPerUnpaidVisits: INTEGER NULLABLE - 
  - ntmPaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmUnpaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmOnsiteMin: FLOAT NULLABLE - 
  - totalOnSiteHours: FLOAT NULLABLE - 
  - driveMinPaidVisit: FLOAT NULLABLE - 
  - driveMinUnpaidVisit: FLOAT NULLABLE - 
  - totalDriveTime: FLOAT NULLABLE - 
  - constrainedScheduleStops: FLOAT NULLABLE - 
  - minPenaltyPerConstrainedSchedule: FLOAT NULLABLE - 
  - totalConstrainedSchedulePenaltyMin: FLOAT NULLABLE - 
  - balanceAllocationPerVisitMin: FLOAT NULLABLE - 
  - totalBalanceAllocation: FLOAT NULLABLE - 
  - nonSiteMinForecast: FLOAT NULLABLE - 
  - laborMinForecast: FLOAT NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 
  - activeSubscriptions: INTEGER NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - eligibleSubAccounts: INTEGER NULLABLE - 
  - ntmDuration: INTEGER NULLABLE - 
  - customerDurationRatio: FLOAT NULLABLE - 
  - visitsinDurationRatio: INTEGER NULLABLE - 
  - fromForecastTable: BOOLEAN NULLABLE - 
  - fromDurationTable: BOOLEAN NULLABLE - 
  - hasNullClientId: BOOLEAN NULLABLE - 
  - hasNullMasterAccountId: BOOLEAN NULLABLE - 

## pco-qa.NSMarch.NS_Customer_Detail_v2

- num_rows: 21535

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - ntmProductionForecast: FLOAT NULLABLE - 
  - ntmScheduledVisits: INTEGER NULLABLE - 
  - revGreaterThanNormal: FLOAT NULLABLE - 
  - dollarMarketingBenefit: FLOAT NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - avgOnsiteMinPerRecurringVisit: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - ntmForecastUnpaidVisits: FLOAT NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - estMinPerUnpaidVisits: INTEGER NULLABLE - 
  - ntmPaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmUnpaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmOnsiteMin: FLOAT NULLABLE - 
  - totalOnSiteHours: FLOAT NULLABLE - 
  - driveMinPaidVisit: FLOAT NULLABLE - 
  - driveMinUnpaidVisit: FLOAT NULLABLE - 
  - totalDriveTime: FLOAT NULLABLE - 
  - constrainedScheduleStops: FLOAT NULLABLE - 
  - minPenaltyPerConstrainedSchedule: FLOAT NULLABLE - 
  - totalConstrainedSchedulePenaltyMin: FLOAT NULLABLE - 
  - balanceAllocationPerVisitMin: FLOAT NULLABLE - 
  - totalBalanceAllocation: FLOAT NULLABLE - 
  - nonSiteMinForecast: FLOAT NULLABLE - 
  - laborMinForecast: FLOAT NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 
  - activeSubscriptions: INTEGER NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - eligibleSubAccounts: INTEGER NULLABLE - 
  - ntmDuration: INTEGER NULLABLE - 
  - customerDurationRatio: FLOAT NULLABLE - 
  - visitsinDurationRatio: INTEGER NULLABLE - 
  - fromForecastTable: BOOLEAN NULLABLE - 
  - fromDurationTable: BOOLEAN NULLABLE - 
  - hasNullClientId: BOOLEAN NULLABLE - 
  - hasNullMasterAccountId: BOOLEAN NULLABLE - 

## pco-qa.NSMarch.NS_subscription_forecast

- num_rows: 52610

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - active: INTEGER NULLABLE - 
  - recurringCharge: FLOAT NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 
  - preferredDays: INTEGER NULLABLE - 
  - preferredStart: TIME NULLABLE - 
  - preferredEnd: TIME NULLABLE - 
  - recurringTicket: STRING NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - default_length: FLOAT NULLABLE - 
  - effective_duration: FLOAT NULLABLE - 
  - scheduled_duration: FLOAT NULLABLE - 
  - customer_ratio: FLOAT NULLABLE - 
  - service_ratio: FLOAT NULLABLE - 
  - final_ratio: FLOAT NULLABLE - 
  - forecast: FLOAT NULLABLE - 
  - fDuration: INTEGER NULLABLE - 
  - nsDuration: FLOAT NULLABLE - 

## pco-qa.NSMarch.WTFL_filled

- num_rows: 1062839

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - month_start: DATETIME NULLABLE - 
  - month_end: DATE NULLABLE - 
  - active_start: INTEGER NULLABLE - 
  - active_end: INTEGER NULLABLE - 
  - arv_start: FLOAT NULLABLE - 
  - arv_end: FLOAT NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 

## pco-qa.NSMarch.WTFL_month_dim

- num_rows: 13

- schema:
  - month_start_date: DATETIME NULLABLE - 
  - month_end_date: DATE NULLABLE - 
  - month_start_ts: TIMESTAMP NULLABLE - 
  - month_end_ts: TIMESTAMP NULLABLE - 

## pco-qa.NSMarch.WTFL_monthly_roll

- num_rows: 13

- schema:
  - month_start: DATETIME NULLABLE - 
  - n_active_start: INTEGER NULLABLE - 
  - n_active_end: INTEGER NULLABLE - 
  - start_of_month_arv: FLOAT NULLABLE - 
  - n_new_starts: INTEGER NULLABLE - 
  - arv_new_starts: FLOAT NULLABLE - 
  - n_churners: INTEGER NULLABLE - 
  - arv_churners: FLOAT NULLABLE - 
  - n_price_changers: INTEGER NULLABLE - 
  - delta_arv_price_chg: FLOAT NULLABLE - 
  - end_of_month_arv: FLOAT NULLABLE - 

## pco-qa.NSMarch.WTFL_roles

- num_rows: 1062839

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - month_start: DATETIME NULLABLE - 
  - month_end: DATE NULLABLE - 
  - active_start: INTEGER NULLABLE - 
  - active_end: INTEGER NULLABLE - 
  - arv_start: FLOAT NULLABLE - 
  - arv_end: FLOAT NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 
  - is_new_start: INTEGER NULLABLE - 
  - is_churner: INTEGER NULLABLE - 
  - is_price_changer: INTEGER NULLABLE - 

## pco-qa.NSMarch.WTFL_state_boundary

- num_rows: 1062839

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - month_start_date: DATETIME NULLABLE - 
  - month_end_date: DATE NULLABLE - 
  - active_start_snap: INTEGER NULLABLE - 
  - arv_start_snap: FLOAT NULLABLE - 
  - active_end_snap: INTEGER NULLABLE - 
  - arv_end_snap: FLOAT NULLABLE - 

## pco-qa.NSMarch.WTFL_sub_history

- num_rows: 86331

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 
  - active: INTEGER NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - snapshot_ts: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 

## pco-qa.NSMarch.WTFL_virtual_status

- num_rows: 1062839

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - month_start: DATETIME NULLABLE - 
  - month_end: DATE NULLABLE - 
  - interval_active_start: INTEGER NULLABLE - 
  - interval_active_end: INTEGER NULLABLE - 
  - active_start_snap: INTEGER NULLABLE - 
  - active_end_snap: INTEGER NULLABLE - 
  - arv_start_snap: FLOAT NULLABLE - 
  - arv_end_snap: FLOAT NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 

## pco-qa.NSMarch.historicalCustomerHours

- num_rows: 337721

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - totalOnSiteMinutes_since_start_date: FLOAT NULLABLE - 
  - totalVisits_since_start_date: INTEGER NULLABLE - 
  - totalRecurringVisits_since_start_date: INTEGER NULLABLE - 
  - totalRecurringOnSiteMinutes_since_start_date: FLOAT NULLABLE - 
  - totalReserviceVisits_since_start_date: INTEGER NULLABLE - 
  - totalReserviceOnSiteMinutes_since_start_date: FLOAT NULLABLE - 

## pco-qa.NSMarch.indRatio

- num_rows: 45506

- schema:
  - clientId: STRING NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - averageDurationRatio: FLOAT NULLABLE - 

## pco-qa.NSMarch.ind_acc_elig_inputs

- num_rows: 106075

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - clientID: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - hasProduct: BOOLEAN NULLABLE - 
  - mostRecentChange: TIMESTAMP NULLABLE - 
  - earliestContractDate: TIMESTAMP NULLABLE - 

## pco-qa.NSMarch.ind_acc_elig_outputs

- num_rows: 106075

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - clientID: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - hasProduct: BOOLEAN NULLABLE - 
  - mostRecentChange: TIMESTAMP NULLABLE - 
  - earliestContractDate: TIMESTAMP NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - enoughTimeSinceChange: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 

## pco-qa.NSMarch.masterRatio

- num_rows: 44523

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - averageDurationRatio: FLOAT NULLABLE - 
  - numberOfVisits: INTEGER NULLABLE - 

## pco-qa.NSMarch.mergedAppliedPayments

- num_rows: 444732

- schema:
  - APPLIED_PAYMENT_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - APPLIED_AMOUNT: FLOAT NULLABLE - 
  - TAX_COLLECTED: FLOAT NULLABLE - 
  - DATE_UPDATED: TIMESTAMP NULLABLE - 
  - DATE_APPLIED: DATE NULLABLE - 
  - CLIENT: STRING NULLABLE - 

## pco-qa.NSMarch.ntmDurationEstimator

- num_rows: 430600

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - effective_duration_minutes: INTEGER NULLABLE - 
  - annualRecurringServices_coalesced: INTEGER NULLABLE - 
  - active_coalesced: INTEGER NULLABLE - 
  - ntmDurationEstimateMinutes: INTEGER NULLABLE - 

## pco-qa.NSMarch.ntmDurationEstimator2

- num_rows: 430600

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - ntmDurationEstimateMinutes: INTEGER NULLABLE - 

## pco-qa.NSMarch.price_push_2025_05_14

- num_rows: 0

- schema:
  - ticketID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - ticketProductionValue: FLOAT NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 

## pco-qa.NSMarch.temp_AccelPricePush

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: STRING NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: FLOAT NULLABLE - 

## pco-qa.NSMarch.temp_AccelReversePush

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: FLOAT NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 

## pco-qa.NSMarch.temp_GreenCountryPricePush

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: STRING NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - oldCharge: INTEGER NULLABLE - 
  - newCharge: INTEGER NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 

## pco-qa.NS_ForLooker.vw_customer_profitability_dashboard

- num_rows: 0

- schema:
  - userEmail: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - locationID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - location_name: STRING NULLABLE - 
  - forecasted_annualized_revenue: FLOAT NULLABLE - 
  - forecasted_annualized_cost: FLOAT NULLABLE - 
  - forecasted_annualized_labor_hours: FLOAT NULLABLE - 
  - total_subscriptions_master: INTEGER NULLABLE - 
  - forecasted_contribution_margin_perc: FLOAT NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - master_primary_employee_name: STRING NULLABLE - 
  - location_primary_employee_name: STRING NULLABLE - 
  - latest_reservice_date: DATE NULLABLE - 
  - latest_reservice_employeeName: STRING NULLABLE - 
  - prior_visit_date: DATE NULLABLE - 
  - prior_visit_employee_name: STRING NULLABLE - 
  - last_appointment_date: DATE NULLABLE - 
  - last_recurring_appointment_date: DATE NULLABLE - 
  - has_no_recurring_appt_in_last_12_months: BOOLEAN NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - subscription_revenue: FLOAT NULLABLE - 
  - subscription_expected_revenue: FLOAT NULLABLE - 
  - subscription_price_realization_perc: FLOAT NULLABLE - 
  - subscription_forecasted_annual_labor_hours: FLOAT NULLABLE - 
  - subscription_planned_visits: INTEGER NULLABLE - 
  - subscription_cm_ntm_dollar: FLOAT NULLABLE - 
  - subscription_avg_length_of_visit: FLOAT NULLABLE - 
  - subscription_expected_avg_visit_min: FLOAT NULLABLE - 
  - subscription_visit_time_deviation_perc: FLOAT NULLABLE - 

## pco-qa.NS_ForLooker.vw_customer_profitability_dashboard_demo

- num_rows: 0

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - locationID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - location_name: STRING NULLABLE - 
  - forecasted_annualized_revenue: FLOAT NULLABLE - 
  - forecasted_annualized_cost: FLOAT NULLABLE - 
  - forecasted_annualized_labor_hours: FLOAT NULLABLE - 
  - total_subscriptions_master: INTEGER NULLABLE - 
  - forecasted_contribution_margin_perc: FLOAT NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - master_primary_employee_name: STRING NULLABLE - 
  - location_primary_employee_name: STRING NULLABLE - 
  - latest_reservice_date: DATE NULLABLE - 
  - latest_reservice_employeeName: STRING NULLABLE - 
  - prior_visit_date: DATE NULLABLE - 
  - prior_visit_employee_name: STRING NULLABLE - 
  - last_appointment_date: DATE NULLABLE - 
  - last_recurring_appointment_date: DATE NULLABLE - 
  - has_no_recurring_appt_in_last_12_months: BOOLEAN NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - subscription_revenue: FLOAT NULLABLE - 
  - subscription_expected_revenue: FLOAT NULLABLE - 
  - subscription_price_realization_perc: FLOAT NULLABLE - 
  - subscription_forecasted_annual_labor_hours: FLOAT NULLABLE - 
  - subscription_planned_visits: INTEGER NULLABLE - 
  - subscription_cm_ntm_dollar: FLOAT NULLABLE - 
  - subscription_avg_length_of_visit: FLOAT NULLABLE - 
  - subscription_expected_avg_visit_min: FLOAT NULLABLE - 
  - subscription_visit_time_deviation_perc: FLOAT NULLABLE - 

## pco-qa.NS_ForLooker.vw_hierarchical_price_report

- num_rows: 0

- schema:
  - clientId: STRING NULLABLE - 
  - level_name: STRING NULLABLE - 
  - parent_id: STRING NULLABLE - 
  - entity_id: STRING NULLABLE - 
  - entity_name: STRING NULLABLE - 
  - sub_entity_count: INTEGER NULLABLE - 
  - actual_revenue: FLOAT NULLABLE - 
  - expected_revenue: FLOAT NULLABLE - 
  - actual_as_pct_of_expected: FLOAT NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - master_actual_vs_expected_pct: FLOAT NULLABLE - 

## pco-qa.NS_ForLooker.vw_master_account_recs

- num_rows: 0

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - customer_first_name: STRING NULLABLE - 
  - customer_last_name: STRING NULLABLE - 
  - total_revenue: FLOAT NULLABLE - 
  - total_labor_hours: FLOAT NULLABLE - 
  - contribution_margin_dollar_ntm_hourly_rate: FLOAT NULLABLE - 
  - contribution_margin_perc_ntm_hourly_rate: FLOAT NULLABLE - 
  - contribution_margin_perc_ntm_hourly_rate_display: FLOAT NULLABLE - 
  - CM_Overhead_Threshold: FLOAT NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - margin_sub_segment: STRING NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - last_product_change_date: TIMESTAMP NULLABLE - 
  - gap_to_profitability_percent: FLOAT NULLABLE - 
  - countActiveLocations: INTEGER NULLABLE - 
  - recommended_price_increase_percent: FLOAT NULLABLE - 
  - recommended_price_increase_percent_display: FLOAT NULLABLE - 

## pco-qa.NS_HistoricalDive.historical_service_health

- num_rows: 234

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - master_account_name: STRING NULLABLE - 
  - subAccounts: STRING NULLABLE - 
  - total_revenue_recurring: FLOAT NULLABLE - 
  - total_revenue_one_time: FLOAT NULLABLE - 
  - cm_ntm_dollar: FLOAT NULLABLE - 
  - cm_ntm_perc: FLOAT NULLABLE - 
  - customer_price_realization_pct: FLOAT NULLABLE - 
  - historical_reservice_rate: FLOAT NULLABLE - 
  - has_consecutive_reservice_history: BOOLEAN NULLABLE - 
  - avg_visit_time_deviation_pct: FLOAT NULLABLE - 

## pco-qa.NS_HistoricalDive.service_time_historical_summary

- num_rows: 290

- schema:
  - clientId: STRING NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - total_appointments_in_ttm: INTEGER NULLABLE - 
  - avg_visit_minutes: FLOAT NULLABLE - 
  - median_visit_minutes: FLOAT NULLABLE - 
  - max_visit_minutes: FLOAT NULLABLE - 

## pco-qa.NS_HistoricalDive.technician_historical_summary

- num_rows: 34

- schema:
  - clientId: STRING NULLABLE - 
  - employeeID: INTEGER NULLABLE - 
  - technician_name: STRING NULLABLE - 
  - total_appointments_in_ttm: INTEGER NULLABLE - 
  - technician_causal_reservice_rate: FLOAT NULLABLE - 
  - pct_of_visits_very_long: FLOAT NULLABLE - 
  - pct_of_visits_very_short: FLOAT NULLABLE - 

## pco-qa.NS_HistoricalDive.unprofitable_cohort

- num_rows: 234

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 

## pco-qa.NS_PriceRecs.NSPriceRecs_Step1

- num_rows: 3883

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - customer_first_name: STRING NULLABLE - 
  - customer_last_name: STRING NULLABLE - 
  - CurrentRevenue: FLOAT NULLABLE - 
  - CurrentLaborHours: FLOAT NULLABLE - 
  - calculated_hourly_cost: FLOAT NULLABLE - 
  - ntm_forecast_hourly_rate: FLOAT NULLABLE - 
  - CurrentCM_Dollar: FLOAT NULLABLE - 
  - CurrentCM_Percent: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - last_product_change_date: TIMESTAMP NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 
  - Astar: BIGNUMERIC NULLABLE - 
  - flexible_upper_bound_percent: BIGNUMERIC NULLABLE - 
  - outperformer_increase_setting: STRING NULLABLE - 
  - CM_Overhead_Threshold: FLOAT NULLABLE - 
  - target_revenue_per_hour: BIGNUMERIC NULLABLE - 
  - customer_segment: STRING NULLABLE - 
  - gap_to_profitability_percent: FLOAT NULLABLE - 

## pco-qa.NS_PriceRecs.NSPriceRecs_Step2

- num_rows: 3883

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - customer_first_name: STRING NULLABLE - 
  - customer_last_name: STRING NULLABLE - 
  - CurrentRevenue: FLOAT NULLABLE - 
  - CurrentLaborHours: FLOAT NULLABLE - 
  - calculated_hourly_cost: FLOAT NULLABLE - 
  - ntm_forecast_hourly_rate: FLOAT NULLABLE - 
  - CurrentCM_Dollar: FLOAT NULLABLE - 
  - CurrentCM_Percent: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - last_product_change_date: TIMESTAMP NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 
  - Astar: BIGNUMERIC NULLABLE - 
  - flexible_upper_bound_percent: BIGNUMERIC NULLABLE - 
  - outperformer_increase_setting: STRING NULLABLE - 
  - CM_Overhead_Threshold: FLOAT NULLABLE - 
  - target_revenue_per_hour: BIGNUMERIC NULLABLE - 
  - customer_segment: STRING NULLABLE - 
  - gap_to_profitability_percent: FLOAT NULLABLE - 
  - initial_proposed_increase_percent: FLOAT NULLABLE - 

## pco-qa.NS_PriceRecs.NSPriceRecs_Step3

- num_rows: 3883

- schema:
  - clientId: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - customer_first_name: STRING NULLABLE - 
  - customer_last_name: STRING NULLABLE - 
  - CurrentRevenue: FLOAT NULLABLE - 
  - CurrentLaborHours: FLOAT NULLABLE - 
  - calculated_hourly_cost: FLOAT NULLABLE - 
  - ntm_forecast_hourly_rate: FLOAT NULLABLE - 
  - CurrentCM_Dollar: FLOAT NULLABLE - 
  - CurrentCM_Percent: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - last_product_change_date: TIMESTAMP NULLABLE - 
  - is_eligible_for_price_increase: BOOLEAN NULLABLE - 
  - Astar: BIGNUMERIC NULLABLE - 
  - flexible_upper_bound_percent: BIGNUMERIC NULLABLE - 
  - outperformer_increase_setting: STRING NULLABLE - 
  - CM_Overhead_Threshold: FLOAT NULLABLE - 
  - target_revenue_per_hour: BIGNUMERIC NULLABLE - 
  - customer_segment: STRING NULLABLE - 
  - gap_to_profitability_percent: FLOAT NULLABLE - 
  - initial_proposed_increase_percent: FLOAT NULLABLE - 
  - final_proposed_increase_percent_pass1: FLOAT NULLABLE - 

## pco-qa.NS_WeeklyCoaching.all_coaching_opportunities

- num_rows: 1614

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - appointmentDate: DATE NULLABLE - 
  - employeeID: INTEGER NULLABLE - 
  - prev_visit_employeeID: INTEGER NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - coaching_topic: STRING NULLABLE - 
  - key_metric_details: STRING NULLABLE - 
  - priority: INTEGER NULLABLE - 

## pco-qa.NS_WeeklyCoaching.interim_long_visit_issues

- num_rows: 0

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - location_name: STRING NULLABLE - 
  - technician_name: STRING NULLABLE - 
  - appointmentDate: DATE NULLABLE - 
  - coaching_topic: STRING NULLABLE - 
  - key_metric_details: STRING NULLABLE - 

## pco-qa.NS_WeeklyCoaching.weekly_coaching_agenda_full

- num_rows: 1614

- schema:
  - unique_key: STRING NULLABLE - 
  - issue_rank: INTEGER NULLABLE - 
  - base_priority_rank: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - location_name: STRING NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - technician_latest: STRING NULLABLE - 
  - technician_prior: STRING NULLABLE - 
  - appointmentDate: DATE NULLABLE - 
  - coaching_topic: STRING NULLABLE - 
  - key_metric_details: STRING NULLABLE - 

## pco-qa.NS_WeeklyCoaching.weekly_coaching_agenda_top7

- num_rows: 544

- schema:
  - unique_key: STRING NULLABLE - 
  - base_priority_rank: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - location_name: STRING NULLABLE - 
  - margin_segment: STRING NULLABLE - 
  - technician_latest: STRING NULLABLE - 
  - technician_prior: STRING NULLABLE - 
  - appointmentDate: DATE NULLABLE - 
  - coaching_topic: STRING NULLABLE - 
  - key_metric_details: STRING NULLABLE - 

## pco-qa.PricePush.InvalidData

- num_rows: 3791

- schema:
  - ticketID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - hasItems: BOOLEAN NULLABLE - 
  - items: RECORD REPEATED - 
    - itemID: STRING NULLABLE - 
    - ticketID: STRING NULLABLE - 
    - officeID: STRING NULLABLE - 
    - description: STRING NULLABLE - 
    - quantity: STRING NULLABLE - 
    - amount: STRING NULLABLE - 
    - productID: STRING NULLABLE - 
    - serviceID: STRING NULLABLE - 
    - taxable: STRING NULLABLE - 
    - creditTo: STRING NULLABLE - 
    - unitID: STRING NULLABLE - 
    - glNumber: STRING NULLABLE - 
    - measurementSF: STRING NULLABLE - 
    - measurementLF: STRING NULLABLE - 
    - prepaymentAmount: STRING NULLABLE - 
    - category: STRING NULLABLE - 
    - code: STRING NULLABLE - 
    - dateCreated: STRING NULLABLE - 
    - dateUpdated: STRING NULLABLE - 
  - sumItems: FLOAT NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - subTotal: FLOAT NULLABLE - 

## pco-qa.PricePush.PricePushQA

- num_rows: 18777

- schema:
  - clientId: STRING NULLABLE - 
  - ticketId: STRING NULLABLE - 
  - subscriptionId: STRING NULLABLE - 
  - status: STRING NULLABLE - 
  - detail: STRING NULLABLE - 
  - loadedAt: TIMESTAMP NULLABLE - 
  - apiCallParams: STRING NULLABLE - 
  - pushed_serviceCharge: STRING NULLABLE - 
  - pushed_productionValue: STRING NULLABLE - 
  - inputSheet_oldAnnualRecurringValue: STRING NULLABLE - 
  - recordCreatedAt: STRING NULLABLE - 
  - inputSheet_oldServiceCharge: STRING NULLABLE - 
  - rdate_oldProductionValue: STRING NULLABLE - 
  - dataSource_date: STRING NULLABLE - 
  - plannedIncrease: STRING NULLABLE - 
  - runID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - officeId: STRING NULLABLE - 

## pco-qa.PricePush.PricePushQA_backup_20250611

- num_rows: 17765

- schema:
  - clientId: STRING NULLABLE - 
  - ticketId: STRING NULLABLE - 
  - subscriptionId: STRING NULLABLE - 
  - status: STRING NULLABLE - 
  - detail: STRING NULLABLE - 
  - loadedAt: TIMESTAMP NULLABLE - 
  - apiCallParams: STRING NULLABLE - 
  - serviceCharge: STRING NULLABLE - 
  - productionValue: STRING NULLABLE - 
  - oldAnnualRecurringValue: STRING NULLABLE - 
  - recordCreatedAt: STRING NULLABLE - 
  - oldServiceCharge: STRING NULLABLE - 
  - oldProductionValue: STRING NULLABLE - 
  - dataSource_date: STRING NULLABLE - 
  - plannedIncrease: STRING NULLABLE - 

## pco-qa.PricePush.PricePushQA_backup_20250620

- num_rows: 18255

- schema:
  - clientId: STRING NULLABLE - 
  - ticketId: STRING NULLABLE - 
  - subscriptionId: STRING NULLABLE - 
  - status: STRING NULLABLE - 
  - detail: STRING NULLABLE - 
  - loadedAt: TIMESTAMP NULLABLE - 
  - apiCallParams: STRING NULLABLE - 
  - serviceCharge: STRING NULLABLE - 
  - productionValue: STRING NULLABLE - 
  - oldAnnualRecurringValue: STRING NULLABLE - 
  - recordCreatedAt: STRING NULLABLE - 
  - oldServiceCharge: STRING NULLABLE - 
  - oldProductionValue: STRING NULLABLE - 
  - dataSource_date: STRING NULLABLE - 
  - plannedIncrease: STRING NULLABLE - 

## pco-qa.PricePush.accel

- num_rows: 13

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - client: STRING NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - productionValue: FLOAT NULLABLE - 

## pco-qa.PricePush.accel_2

- num_rows: 13

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - client: STRING NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - productionValue: FLOAT NULLABLE - 

## pco-qa.PricePush.accel_3

- num_rows: 13

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - client: STRING NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - productionValue: FLOAT NULLABLE - 

## pco-qa.PricePush.pricePush_decider

- num_rows: 3791

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - hasItems: BOOLEAN NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - status: STRING NULLABLE - 
  - oldProductionValue: FLOAT NULLABLE - 
  - newProductionValue: FLOAT NULLABLE - 
  - accelOldPrice: FLOAT NULLABLE - 
  - mergedRecurringCharge: FLOAT NULLABLE - 

## pco-qa.PricePush.price_push

- num_rows: 18

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: STRING NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: STRING NULLABLE - 

## pco-qa.PricePush.price_push_acc

- num_rows: 3

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: STRING NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: STRING NULLABLE - 

## pco-qa.PricePush.price_push_accel

- num_rows: 4973

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: STRING NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: STRING NULLABLE - 
  - productionValue: INTEGER NULLABLE - 

## pco-qa.PricePush.price_push_final_push

- num_rows: 3790

- schema:
  - ticketID: STRING NULLABLE - 
  - clientID: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - sumItems: FLOAT NULLABLE - 
  - incorrectPushedCharge: FLOAT NULLABLE - 
  - correctedServiceCharge: FLOAT NULLABLE - 
  - currentServiceCharge: FLOAT NULLABLE - 
  - status: STRING NULLABLE - 
  - raw_ticketID: STRING NULLABLE - 
  - raw_client: STRING NULLABLE - 
  - mt_ticketID: INTEGER NULLABLE - 
  - oldProductionValue: FLOAT NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - plannedProductionValue: FLOAT NULLABLE - 

## pco-qa.PricePush.staging_price_push

- num_rows: 3791

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - hasItems: BOOLEAN NULLABLE - 
  - sumItems: FLOAT NULLABLE - 
  - recurringCharge: FLOAT NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - productionValue: FLOAT NULLABLE - 
  - oldsubTotalv1: FLOAT NULLABLE - 
  - oldServiceCharge: FLOAT NULLABLE - 
  - pricePushCharge: FLOAT NULLABLE - 
  - newServiceCharge: FLOAT NULLABLE - 
  - newSubtotal: FLOAT NULLABLE - 
  - apiServiceCharge: FLOAT NULLABLE - 
  - intendedCharge: FLOAT NULLABLE - 
  - status: STRING NULLABLE - 

## pco-qa.QBO_PL.PL_GREENCOUNTRY

- num_rows: 24961

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.QBO_PL.PL_PISGAH

- num_rows: 31788

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.QBO_PL.PL_PISGAH_CLEANED_VIEW

- num_rows: 0

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 
  - cleaned_amount_numeric: BIGNUMERIC NULLABLE - 

## pco-qa.QBO_PL.PL_PROFEXT

- num_rows: 21681

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.QBO_PL.PL_QUELL

- num_rows: 20519

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.QBO_PL.PL_STB

- num_rows: 16526

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.QBO_PL.STB_Predefinedreport_accounts

- num_rows: 11

- schema:
  - account: STRING NULLABLE - 
  - type: STRING NULLABLE - 
  - detail_type: STRING NULLABLE - 
  - create_date: STRING NULLABLE - 
  - created_by: STRING NULLABLE - 
  - last_modified: STRING NULLABLE - 
  - last_modified_by: STRING NULLABLE - 
  - description: STRING NULLABLE - 
  - balance: STRING NULLABLE - 

## pco-qa.QBO_PL.categorized_transactions

- num_rows: 123761

- schema:
  - ClientID: STRING NULLABLE - 
  - Office_Identifier: STRING NULLABLE - 
  - transaction_date: DATE NULLABLE - 
  - cleaned_amount_numeric: BIGNUMERIC NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 
  - amount_cast_error: BOOLEAN NULLABLE - 
  - assigned_pnl_category: STRING NULLABLE - 

## pco-qa.QBO_PL.master_cleaned_data

- num_rows: 146423

- schema:
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - date: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - cleaned_amount_numeric: BIGNUMERIC NULLABLE - 
  - ClientID: STRING NULLABLE - 
  - Office_Identifier: STRING NULLABLE - 

## pco-qa.analytics_layer.allowed_discounts

- num_rows: 1

- schema:
  - Number: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - excludedBy: STRING NULLABLE - 
  - exclusionReason: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 
  - lastVerified: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 

## pco-qa.analytics_layer.cohort_confirmation_list

- num_rows: 13680

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - fullName: STRING NULLABLE - 
  - oldARR: FLOAT NULLABLE - 
  - sinceStart: FLOAT NULLABLE - 
  - sinceProductChange: FLOAT NULLABLE - 

## pco-qa.analytics_layer.cohort_history

- num_rows: 500

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - cohortCreationDate: TIMESTAMP NULLABLE - 
  - cohortAnalyzed: BOOLEAN NULLABLE - 
  - isRepriced: BOOLEAN NULLABLE - 

## pco-qa.analytics_layer.customer_categorize

- num_rows: 25677

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dollarValueForecast: FLOAT NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 
  - forecastDollarValuePerHour: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isSubsidized: STRING NULLABLE - 

## pco-qa.analytics_layer.customer_forecast

- num_rows: 25677

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - ntmProductionForecast: FLOAT NULLABLE - 
  - ntmScheduledVisits: INTEGER NULLABLE - 
  - revGreaterThanNormal: FLOAT NULLABLE - 
  - dollarMarketingBenefit: FLOAT NULLABLE - 
  - reOccurringProductionForecast: INTEGER NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - avgOnsiteMinPerRecurringVisit: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - ntmForecastUnpaidVisits: FLOAT NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - estMinPerUnpaidVisits: INTEGER NULLABLE - 
  - ntmPaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmUnpaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmOnsiteMin: FLOAT NULLABLE - 
  - totalOnSiteHours: FLOAT NULLABLE - 
  - driveMinPaidVisit: INTEGER NULLABLE - 
  - driveMinUnpaidVisit: INTEGER NULLABLE - 
  - totalDriveTime: FLOAT NULLABLE - 
  - constrainedScheduleStops: INTEGER NULLABLE - 
  - minPenaltyPerConstrainedSchedule: INTEGER NULLABLE - 
  - totalConstrainedSchedulePenaltyMin: INTEGER NULLABLE - 
  - balanceAllocationPerVisitMin: INTEGER NULLABLE - 
  - totalBalanceAllocation: FLOAT NULLABLE - 
  - nonSiteMinForecast: FLOAT NULLABLE - 
  - laborMinForecast: FLOAT NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 

## pco-qa.analytics_layer.customer_history

- num_rows: 25677

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - earliestContractDate: TIMESTAMP NULLABLE - 
  - reserviceableDate: TIMESTAMP NULLABLE - 
  - earliestDateToCountReservice: TIMESTAMP NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - hasReserviceableContract: BOOLEAN NULLABLE - 
  - latestProductAddition: TIMESTAMP NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 
  - lastDateAtWhichCusHasInclContract: TIMESTAMP NULLABLE - 
  - totalVisits: FLOAT NULLABLE - 
  - prodFromTicket_total: FLOAT NULLABLE - 
  - prodFromTicket_included: FLOAT NULLABLE - 
  - oneTimeProduction: FLOAT NULLABLE - 
  - recurringVisits: FLOAT NULLABLE - 
  - reserviceableVisits: FLOAT NULLABLE - 
  - isReservice: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - avgMinutesPerRecurringVisit: FLOAT NULLABLE - 

## pco-qa.analytics_layer.customer_level_priceup

- num_rows: 6860

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - hasActiveProduct: BOOLEAN NULLABLE - 
  - referenceDate: TIMESTAMP NULLABLE - 
  - latestProductChange: TIMESTAMP NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - daysSinceFirstSubscription: FLOAT NULLABLE - 
  - noRecentProductChange: BOOLEAN NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - completedVisits: FLOAT NULLABLE - 
  - enoughVisits: BOOLEAN NULLABLE - 
  - manualExclusionRecentPriceChange: BOOLEAN NULLABLE - 
  - manualExclusionAllowedDiscounts: BOOLEAN NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - oldARR: FLOAT NULLABLE - 
  - newARR: FLOAT NULLABLE - 
  - increasePercentage: FLOAT NULLABLE - 
  - Increase: FLOAT NULLABLE - 
  - isSubsidized: STRING NULLABLE - 

## pco-qa.analytics_layer.eligibility

- num_rows: 40753

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - hasActiveProduct: BOOLEAN NULLABLE - 
  - referenceDate: TIMESTAMP NULLABLE - 
  - latestProductChange: TIMESTAMP NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - daysSinceFirstSubscription: FLOAT NULLABLE - 
  - noRecentProductChange: BOOLEAN NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - completedVisits: FLOAT NULLABLE - 
  - enoughVisits: BOOLEAN NULLABLE - 
  - manualExclusionRecentPriceChange: BOOLEAN NULLABLE - 
  - manualExclusionAllowedDiscounts: BOOLEAN NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 

## pco-qa.analytics_layer.individual_account_eligibility

- num_rows: 41831

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - hasActiveProduct: BOOLEAN NULLABLE - 
  - referenceDate: TIMESTAMP NULLABLE - 
  - latestProductChange: TIMESTAMP NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - daysSinceFirstSubscription: FLOAT NULLABLE - 
  - noRecentProductChange: BOOLEAN NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - completedVisits: FLOAT NULLABLE - 
  - enoughVisits: BOOLEAN NULLABLE - 
  - manualExclusionRecentPriceChange: BOOLEAN NULLABLE - 
  - manualExclusionAllowedDiscounts: BOOLEAN NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - randomNumbers: FLOAT NULLABLE - 
  - inCohort: BOOLEAN NULLABLE - 
  - fullName: STRING NULLABLE - 

## pco-qa.analytics_layer.make_floors

- num_rows: 4

- schema:
  - customerLevelCategorizationInputs: STRING NULLABLE - 
  - isSubsidized: BOOLEAN NULLABLE - 
  - name: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - floorValue: FLOAT NULLABLE - 
  - ttmCogs: FLOAT NULLABLE - 
  - cogsPlusInflation: FLOAT NULLABLE - 
  - floorDollar: FLOAT NULLABLE - 
  - check: BOOLEAN NULLABLE - 

## pco-qa.analytics_layer.master_account_eligibility

- num_rows: 606

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - fullName: STRING NULLABLE - 
  - noOfEligibleSubAccounts: INTEGER NULLABLE - 
  - countSubAccounts: INTEGER NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - anyEligibleSubAccount: BOOLEAN NULLABLE - 
  - allEligibleSubAccount: BOOLEAN NULLABLE - 

## pco-qa.analytics_layer.multisite

- num_rows: 4505

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - fullName: STRING NULLABLE - 
  - noOfEligibleSubAccounts: INTEGER NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - totalContractSize: FLOAT NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - totalForecastedHours: FLOAT NULLABLE - 
  - dollarContractPerHour: FLOAT NULLABLE - 
  - dollarValuePerHour: FLOAT NULLABLE - 
  - performerTargetDollarValuePerHour: FLOAT NULLABLE - 
  - gapToPerformer: FLOAT NULLABLE - 

## pco-qa.analytics_layer.multisite_aggregated

- num_rows: 4243

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - fullName: STRING NULLABLE - 
  - noOfEligibleSubAccounts: INTEGER NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - totalContractSize: FLOAT NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - totalForecastedHours: FLOAT NULLABLE - 
  - dollarContractPerHour: FLOAT NULLABLE - 
  - dollarValuePerHour: FLOAT NULLABLE - 
  - performerTargetDollarValuePerHour: FLOAT NULLABLE - 
  - gapToPerformer: FLOAT NULLABLE - 

## pco-qa.analytics_layer.output_list_advanced

- num_rows: 4593

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - totalForecastNTMValue: FLOAT NULLABLE - 
  - forecastHours: FLOAT NULLABLE - 
  - forecastDollarProductionPerHour: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - increasePercentage: FLOAT NULLABLE - 
  - dollarSubsidized: FLOAT NULLABLE - 
  - recommendContractReview: BOOLEAN NULLABLE - 
  - fullName: STRING NULLABLE - 

## pco-qa.analytics_layer.output_list_basic

- num_rows: 135

- schema:
  - Number: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - name: STRING NULLABLE - 
  - annualRecurringProduction: FLOAT NULLABLE - 
  - forecastHoursSubscriptionOnSite: FLOAT NULLABLE - 
  - forecastRerserviceHoursUnpaidOnSite: FLOAT NULLABLE - 
  - onSiteDollarPerHourForecast: FLOAT NULLABLE - 
  - onSitePercentile: FLOAT NULLABLE - 
  - onSiteCategory: STRING NULLABLE - 
  - freqReservice: STRING NULLABLE - 
  - minutesBetween30And45: STRING NULLABLE - 
  - minVisitsGreaterThan45: STRING NULLABLE - 

## pco-qa.analytics_layer.price_pushup

- num_rows: 7256

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: STRING NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: STRING NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: STRING NULLABLE - 

## pco-qa.analytics_layer.subscription_level_priceup

- num_rows: 6957

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - active: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - majorBucket: STRING NULLABLE - 
  - minorBucket: STRING NULLABLE - 
  - increaseIfPerformer: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isPerformer: BOOLEAN NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - Increase: FLOAT NULLABLE - 

## pco-qa.analytics_layer.tech_notes

- num_rows: 135

- schema:
  - Number: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - isSubsidized: STRING NULLABLE - 
  - freqReservice: STRING NULLABLE - 
  - minutesBetween30And45: STRING NULLABLE - 
  - minVisitsGreaterThan45: STRING NULLABLE - 
  - freqReschedule: STRING NULLABLE - 
  - constrainedSchedule: STRING NULLABLE - 
  - techContacted: BOOLEAN NULLABLE - 
  - deferSixMonths: BOOLEAN NULLABLE - 
  - techText: STRING NULLABLE - 

## pco-qa.analytics_reporting.price_push_baseline_subscriptions

- num_rows: 2

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - runID: STRING NULLABLE - 
  - subscriptionsIDs: STRING NULLABLE - 
  - pricePushDate: DATE NULLABLE - 
  - recordUpdateAt: TIMESTAMP NULLABLE - 

## pco-qa.analytics_reporting.price_push_impact_sub_level

- num_rows: 35

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - runID: STRING NULLABLE - 
  - pricePushDate: DATE NULLABLE - 
  - majorServiceBucket: STRING NULLABLE - 
  - horizonDays: INTEGER NULLABLE - 
  - countExpBaseline: INTEGER NULLABLE - 
  - countCtrlBaseline: INTEGER NULLABLE - 
  - countExpActive: INTEGER NULLABLE - 
  - countCtrlActive: INTEGER NULLABLE - 
  - cancelRateExp: FLOAT NULLABLE - 
  - cancelRateCtrl: FLOAT NULLABLE - 
  - deltaCancelRate: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.analytics_reporting.price_push_tagged_subscriptions

- num_rows: 41613

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - groupType: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - sinceLastProductChange: FLOAT NULLABLE - 
  - dateOfLastPriceChange: DATE NULLABLE - 
  - runID: STRING NULLABLE - 
  - pricePushDate: DATE NULLABLE - 
  - majorServiceBucket: STRING NULLABLE - 
  - minTenureSinceStart: FLOAT NULLABLE - 
  - minTenureSinceProductChange: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - active: INTEGER NULLABLE - 

## pco-qa.analytics_reporting.subscription_scd

- num_rows: 303384

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - active: INTEGER NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - validFrom: TIMESTAMP NULLABLE - 
  - validTo: TIMESTAMP NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 

## pco-qa.computation_layer.PricePushQA

- num_rows: 18

- schema:
  - apiCallParams: STRING NULLABLE - 
  - status: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - detail: STRING NULLABLE - 
  - loadedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 

## pco-qa.computation_layer.cohort_confirmation_list

- num_rows: 17378

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - fullName: STRING NULLABLE - 
  - oldARR: FLOAT NULLABLE - 
  - sinceStart: FLOAT NULLABLE - 
  - sinceProductChange: FLOAT NULLABLE - 

## pco-qa.computation_layer.cohort_history

- num_rows: 21775

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - cohortCreationDate: TIMESTAMP NULLABLE - 
  - cohortAnalyzed: BOOLEAN NULLABLE - 
  - isRepriced: BOOLEAN NULLABLE - 

## pco-qa.computation_layer.customer_categorize

- num_rows: 107229

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dollarValueForecast: FLOAT NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 
  - forecastDollarValuePerHour: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isSubsidized: STRING NULLABLE - 

## pco-qa.computation_layer.customer_forecast

- num_rows: 107229

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - ntmProductionForecast: FLOAT NULLABLE - 
  - ntmScheduledVisits: INTEGER NULLABLE - 
  - revGreaterThanNormal: FLOAT NULLABLE - 
  - dollarMarketingBenefit: FLOAT NULLABLE - 
  - reOccurringProductionForecast: INTEGER NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - avgOnsiteMinPerRecurringVisit: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - ntmForecastUnpaidVisits: FLOAT NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - estMinPerUnpaidVisits: INTEGER NULLABLE - 
  - ntmPaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmUnpaidOnsiteMinutes: FLOAT NULLABLE - 
  - ntmOnsiteMin: FLOAT NULLABLE - 
  - totalOnSiteHours: FLOAT NULLABLE - 
  - driveMinPaidVisit: FLOAT NULLABLE - 
  - driveMinUnpaidVisit: FLOAT NULLABLE - 
  - totalDriveTime: FLOAT NULLABLE - 
  - constrainedScheduleStops: FLOAT NULLABLE - 
  - minPenaltyPerConstrainedSchedule: FLOAT NULLABLE - 
  - totalConstrainedSchedulePenaltyMin: FLOAT NULLABLE - 
  - balanceAllocationPerVisitMin: FLOAT NULLABLE - 
  - totalBalanceAllocation: FLOAT NULLABLE - 
  - nonSiteMinForecast: FLOAT NULLABLE - 
  - laborMinForecast: FLOAT NULLABLE - 
  - laborHoursForecast: FLOAT NULLABLE - 

## pco-qa.computation_layer.customer_history

- num_rows: 108650

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - earliestContractDate: TIMESTAMP NULLABLE - 
  - reserviceableDate: TIMESTAMP NULLABLE - 
  - earliestDateToCountReservice: TIMESTAMP NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - hasReserviceableContract: BOOLEAN NULLABLE - 
  - latestProductAddition: TIMESTAMP NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 
  - lastDateAtWhichCusHasInclContract: TIMESTAMP NULLABLE - 
  - totalVisits: FLOAT NULLABLE - 
  - prodFromTicket_total: FLOAT NULLABLE - 
  - prodFromTicket_included: FLOAT NULLABLE - 
  - oneTimeProduction: FLOAT NULLABLE - 
  - recurringVisits: FLOAT NULLABLE - 
  - reserviceableVisits: FLOAT NULLABLE - 
  - isReservice: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - avgMinutesPerRecurringVisit: FLOAT NULLABLE - 

## pco-qa.computation_layer.customer_level_priceup

- num_rows: 17786

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - hasActiveProduct: BOOLEAN NULLABLE - 
  - referenceDate: TIMESTAMP NULLABLE - 
  - latestProductChange: TIMESTAMP NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - daysSinceFirstSubscription: FLOAT NULLABLE - 
  - noRecentProductChange: BOOLEAN NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - completedVisits: FLOAT NULLABLE - 
  - enoughVisits: BOOLEAN NULLABLE - 
  - manualExclusionRecentPriceChange: BOOLEAN NULLABLE - 
  - manualExclusionAllowedDiscounts: BOOLEAN NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - oldARR: FLOAT NULLABLE - 
  - newARR: FLOAT NULLABLE - 
  - increasePercentage: FLOAT NULLABLE - 
  - Increase: FLOAT NULLABLE - 
  - isSubsidized: STRING NULLABLE - 

## pco-qa.computation_layer.eligibility

- num_rows: 108655

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - hasActiveProduct: BOOLEAN NULLABLE - 
  - referenceDate: TIMESTAMP NULLABLE - 
  - latestProductChange: TIMESTAMP NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - daysSinceFirstSubscription: FLOAT NULLABLE - 
  - noRecentProductChange: BOOLEAN NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - completedVisits: FLOAT NULLABLE - 
  - enoughVisits: BOOLEAN NULLABLE - 
  - manualExclusionRecentPriceChange: BOOLEAN NULLABLE - 
  - manualExclusionAllowedDiscounts: BOOLEAN NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 

## pco-qa.computation_layer.individual_account_eligibility

- num_rows: 110794

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - hasActiveProduct: BOOLEAN NULLABLE - 
  - referenceDate: TIMESTAMP NULLABLE - 
  - latestProductChange: TIMESTAMP NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - firstSubsDate: TIMESTAMP NULLABLE - 
  - daysSinceFirstSubscription: FLOAT NULLABLE - 
  - noRecentProductChange: BOOLEAN NULLABLE - 
  - enoughTenure: BOOLEAN NULLABLE - 
  - completedVisits: FLOAT NULLABLE - 
  - enoughVisits: BOOLEAN NULLABLE - 
  - manualExclusionRecentPriceChange: BOOLEAN NULLABLE - 
  - manualExclusionAllowedDiscounts: BOOLEAN NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - randomNumbers: FLOAT NULLABLE - 
  - inCohort: BOOLEAN NULLABLE - 
  - fullName: STRING NULLABLE - 

## pco-qa.computation_layer.individual_customer_location

- num_rows: 25794

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - DollarPerHour: FLOAT NULLABLE - 
  - plannedVisit: FLOAT NULLABLE - 
  - unplannedVisit: FLOAT NULLABLE - 
  - onSiteHour: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - includedRecurringSubscriptions: INTEGER NULLABLE - 
  - subscriptionOldEnough: INTEGER NULLABLE - 
  - activeSub: INTEGER NULLABLE - 
  - countZeroVisitTime: INTEGER NULLABLE - 
  - countIsRecurring: INTEGER NULLABLE - 
  - countNullAppointmentHistory: INTEGER NULLABLE - 
  - countNtmPlannedVisitsGreaterThanZero: INTEGER NULLABLE - 
  - countHoursButNoRevenue: INTEGER NULLABLE - 
  - countRevButNoHours: INTEGER NULLABLE - 
  - hoursWithoutRevenue: BOOLEAN NULLABLE - 
  - revenueWithotHours: BOOLEAN NULLABLE - 
  - yearlyVisitOverride: INTEGER NULLABLE - 
  - countIncludedSubscription: INTEGER NULLABLE - 
  - countExcludedSubscription: INTEGER NULLABLE - 
  - includedSubscriptionPercentage: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.computation_layer.make_floors

- num_rows: 24

- schema:
  - customerLevelCategorizationInputs: STRING NULLABLE - 
  - isSubsidized: BOOLEAN NULLABLE - 
  - name: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - floorValue: FLOAT NULLABLE - 
  - ttmCogs: FLOAT NULLABLE - 
  - cogsPlusInflation: FLOAT NULLABLE - 
  - floorDollar: FLOAT NULLABLE - 
  - check: BOOLEAN NULLABLE - 

## pco-qa.computation_layer.master_account_eligibility

- num_rows: 1134

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - fullName: STRING NULLABLE - 
  - noOfEligibleSubAccounts: INTEGER NULLABLE - 
  - countSubAccounts: INTEGER NULLABLE - 
  - anyManualExclusion: BOOLEAN NULLABLE - 
  - anyEligibleSubAccount: BOOLEAN NULLABLE - 
  - allEligibleSubAccount: BOOLEAN NULLABLE - 

## pco-qa.computation_layer.master_customer_location

- num_rows: 24155

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - DollarPerHour: FLOAT NULLABLE - 
  - plannedVisit: FLOAT NULLABLE - 
  - unplannedVisit: FLOAT NULLABLE - 
  - onSiteHour: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - includedRecurringSubscriptions: INTEGER NULLABLE - 
  - subscriptionOldEnough: INTEGER NULLABLE - 
  - afterDeductible: FLOAT NULLABLE - 
  - Benefit: FLOAT NULLABLE - 
  - effectiveDollarPerHour: FLOAT NULLABLE - 
  - activeSub: INTEGER NULLABLE - 
  - countZeroVisitTime: INTEGER NULLABLE - 
  - countIsRecurring: INTEGER NULLABLE - 
  - countNullAppointmentHistory: INTEGER NULLABLE - 
  - countNtmPlannedVisitsGreaterThanZero: INTEGER NULLABLE - 
  - countHoursButNoRevenue: INTEGER NULLABLE - 
  - countRevButNoHours: INTEGER NULLABLE - 
  - hoursWithoutRevenue: BOOLEAN NULLABLE - 
  - revenueWithotHours: BOOLEAN NULLABLE - 
  - yearlyVisitOverride: INTEGER NULLABLE - 
  - countIncludedSubscription: INTEGER NULLABLE - 
  - countExcludedSubscription: INTEGER NULLABLE - 
  - includedSubscriptionPercentage: FLOAT NULLABLE - 
  - allSubscriptionOldEnough: BOOLEAN NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.computation_layer.multisite

- num_rows: 13195

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - daysSinceProductChange: FLOAT NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - fullName: STRING NULLABLE - 
  - noOfEligibleSubAccounts: INTEGER NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - totalContractSize: FLOAT NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - totalForecastedHours: FLOAT NULLABLE - 
  - dollarContractPerHour: FLOAT NULLABLE - 
  - dollarValuePerHour: FLOAT NULLABLE - 
  - performerTargetDollarValuePerHour: FLOAT NULLABLE - 
  - gapToPerformer: FLOAT NULLABLE - 

## pco-qa.computation_layer.multisite_aggregated

- num_rows: 12290

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - activeSubAccounts: INTEGER NULLABLE - 
  - fullName: STRING NULLABLE - 
  - noOfEligibleSubAccounts: INTEGER NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - totalContractSize: FLOAT NULLABLE - 
  - ntmValueForecast: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - totalForecastedHours: FLOAT NULLABLE - 
  - dollarContractPerHour: FLOAT NULLABLE - 
  - dollarValuePerHour: FLOAT NULLABLE - 
  - performerTargetDollarValuePerHour: FLOAT NULLABLE - 
  - gapToPerformer: FLOAT NULLABLE - 

## pco-qa.computation_layer.output_list_advanced

- num_rows: 10762

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - totalForecastNTMValue: FLOAT NULLABLE - 
  - forecastHours: FLOAT NULLABLE - 
  - forecastDollarProductionPerHour: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - increasePercentage: FLOAT NULLABLE - 
  - dollarSubsidized: FLOAT NULLABLE - 
  - recommendContractReview: BOOLEAN NULLABLE - 
  - fullName: STRING NULLABLE - 

## pco-qa.computation_layer.price_pushup

- num_rows: 18486

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: STRING NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: STRING NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: STRING NULLABLE - 

## pco-qa.computation_layer.price_pushup_new

- num_rows: 2402

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: STRING NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - newCharge: STRING NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 

## pco-qa.computation_layer.subscription_detail

- num_rows: 42286

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - fname: STRING NULLABLE - 
  - lname: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - yearlyVisitOverride: FLOAT NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - filterOutFlag: BOOLEAN NULLABLE - 
  - countFrom: TIMESTAMP NULLABLE - 
  - agreementLengthDay: FLOAT NULLABLE - 
  - ntmPlannedVisits: FLOAT NULLABLE - 
  - daySubOldEnough: TIMESTAMP NULLABLE - 
  - subOldEnough: BOOLEAN NULLABLE - 
  - nullAppointmentHistory: BOOLEAN NULLABLE - 
  - zeroVisitTimeButMinutes: BOOLEAN NULLABLE - 
  - arsLessThanZero: BOOLEAN NULLABLE - 
  - arsGreaterThan24: BOOLEAN NULLABLE - 
  - hoursButNoRevenue: BOOLEAN NULLABLE - 
  - revButNoHours: BOOLEAN NULLABLE - 
  - keepIn: BOOLEAN NULLABLE - 
  - forecastUnplannedVisits: FLOAT NULLABLE - 
  - minutesPerUnplannedVisits: INTEGER NULLABLE - 
  - unplannedVisitMinOnSite: FLOAT NULLABLE - 
  - plannedVisitMinOnSite: FLOAT NULLABLE - 
  - onSiteMin: FLOAT NULLABLE - 
  - avgDriveTimePlanned: FLOAT NULLABLE - 
  - avgDriveTimeUnplanned: FLOAT NULLABLE - 
  - totalPlannedDriveTime: FLOAT NULLABLE - 
  - totalUnplannedDriveTime: FLOAT NULLABLE - 
  - totalDriveTime: FLOAT NULLABLE - 
  - balanceTime: FLOAT NULLABLE - 
  - totalMin: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.computation_layer.subscription_level_labour_forecast

- num_rows: 21437

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - earliestContractDate: TIMESTAMP NULLABLE - 
  - sumDuration: INTEGER NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - calculationTimestamp: TIMESTAMP NULLABLE - 
  - sumPlannedDuration: INTEGER NULLABLE - 
  - sumOnsiteMinutes: FLOAT NULLABLE - 
  - avgDurationRatioSubOnly: FLOAT NULLABLE - 
  - isReservice: FLOAT NULLABLE - 
  - reserviceableVisits: FLOAT NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - filterOutFlag: BOOLEAN NULLABLE - 

## pco-qa.computation_layer.subscription_level_priceup

- num_rows: 17786

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - active: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - majorBucket: STRING NULLABLE - 
  - minorBucket: STRING NULLABLE - 
  - increaseIfPerformer: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isPerformer: BOOLEAN NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - Increase: FLOAT NULLABLE - 

## pco-qa.computation_layer.subscription_level_priceup_new

- num_rows: 37897

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - active: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - eligible: BOOLEAN NULLABLE - 
  - majorBucket: STRING NULLABLE - 
  - minorBucket: STRING NULLABLE - 
  - increaseIfPerformer: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isPerformer: BOOLEAN NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - Increase: FLOAT NULLABLE - 

## pco-qa.kulti_test.PISGAH_CUS_DETAIL

- num_rows: 5820

- schema:
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - DollarPerHour: FLOAT NULLABLE - 
  - plannedVisit: FLOAT NULLABLE - 
  - unplannedVisit: FLOAT NULLABLE - 
  - onSiteHour: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - includedRecurringSubscriptions: INTEGER NULLABLE - 
  - subscriptionOldEnough: INTEGER NULLABLE - 
  - afterDeductible: FLOAT NULLABLE - 
  - Benefit: FLOAT NULLABLE - 
  - effectiveDollarPerHour: FLOAT NULLABLE - 
  - activeSub: INTEGER NULLABLE - 
  - countZeroVisitTime: INTEGER NULLABLE - 
  - countIsRecurring: INTEGER NULLABLE - 
  - countNullAppointmentHistory: INTEGER NULLABLE - 
  - countNtmPlannedVisitsGreaterThanZero: INTEGER NULLABLE - 
  - countHoursButNoRevenue: INTEGER NULLABLE - 
  - countRevButNoHours: INTEGER NULLABLE - 
  - hoursWithoutRevenue: BOOLEAN NULLABLE - 
  - revenueWithotHours: BOOLEAN NULLABLE - 
  - yearlyVisitOverride: INTEGER NULLABLE - 
  - countIncludedSubscription: INTEGER NULLABLE - 
  - countExcludedSubscription: INTEGER NULLABLE - 
  - includedSubscriptionPercentage: FLOAT NULLABLE - 
  - allSubscriptionOldEnough: BOOLEAN NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.PISGAH_LOC_DETAIL

- num_rows: 6480

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - DollarPerHour: FLOAT NULLABLE - 
  - plannedVisit: FLOAT NULLABLE - 
  - unplannedVisit: FLOAT NULLABLE - 
  - onSiteHour: FLOAT NULLABLE - 
  - tenure: FLOAT NULLABLE - 
  - includedRecurringSubscriptions: INTEGER NULLABLE - 
  - subscriptionOldEnough: INTEGER NULLABLE - 
  - activeSub: INTEGER NULLABLE - 
  - countZeroVisitTime: INTEGER NULLABLE - 
  - countIsRecurring: INTEGER NULLABLE - 
  - countNullAppointmentHistory: INTEGER NULLABLE - 
  - countNtmPlannedVisitsGreaterThanZero: INTEGER NULLABLE - 
  - countHoursButNoRevenue: INTEGER NULLABLE - 
  - countRevButNoHours: INTEGER NULLABLE - 
  - hoursWithoutRevenue: BOOLEAN NULLABLE - 
  - revenueWithotHours: BOOLEAN NULLABLE - 
  - yearlyVisitOverride: INTEGER NULLABLE - 
  - countIncludedSubscription: INTEGER NULLABLE - 
  - countExcludedSubscription: INTEGER NULLABLE - 
  - includedSubscriptionPercentage: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.PISGAH_SUB_DETAIL

- num_rows: 15428

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - fname: STRING NULLABLE - 
  - lname: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - yearlyVisitOverride: FLOAT NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - filterOutFlag: BOOLEAN NULLABLE - 
  - countFrom: TIMESTAMP NULLABLE - 
  - agreementLengthDay: FLOAT NULLABLE - 
  - ntmPlannedVisits: FLOAT NULLABLE - 
  - daySubOldEnough: TIMESTAMP NULLABLE - 
  - subOldEnough: BOOLEAN NULLABLE - 
  - nullAppointmentHistory: BOOLEAN NULLABLE - 
  - zeroVisitTimeButMinutes: BOOLEAN NULLABLE - 
  - arsLessThanZero: BOOLEAN NULLABLE - 
  - arsGreaterThan24: BOOLEAN NULLABLE - 
  - hoursButNoRevenue: BOOLEAN NULLABLE - 
  - revButNoHours: BOOLEAN NULLABLE - 
  - keepIn: BOOLEAN NULLABLE - 
  - forecastUnplannedVisits: FLOAT NULLABLE - 
  - minutesPerUnplannedVisits: INTEGER NULLABLE - 
  - unplannedVisitMinOnSite: FLOAT NULLABLE - 
  - plannedVisitMinOnSite: FLOAT NULLABLE - 
  - onSiteMin: FLOAT NULLABLE - 
  - avgDriveTimePlanned: FLOAT NULLABLE - 
  - avgDriveTimeUnplanned: FLOAT NULLABLE - 
  - totalPlannedDriveTime: FLOAT NULLABLE - 
  - totalUnplannedDriveTime: FLOAT NULLABLE - 
  - totalDriveTime: FLOAT NULLABLE - 
  - balanceTime: FLOAT NULLABLE - 
  - totalMin: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.ask_client_flags

- num_rows: 43

- schema:
  - TYPE_ID: INTEGER NULLABLE - 
  - DESCRIPTION: STRING NULLABLE - 
  - Recurrence: INTEGER NULLABLE - 
  - hasReservice: BOOLEAN NULLABLE - 
  - isRervice: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - clientId: STRING NULLABLE - 

## pco-qa.kulti_test.check_type_lkp

- num_rows: 18

- schema:
  - checkTypeID: INTEGER NULLABLE - 
  - checkTypeName: STRING NULLABLE - 
  - description: STRING NULLABLE - 
  - flagTypeIDs: STRING NULLABLE - 
  - targetTablesNames: STRING NULLABLE - 
  - severity: INTEGER NULLABLE - 
  - category: STRING NULLABLE - 
  - defaultOwner: STRING NULLABLE - 
  - createdAt: DATE NULLABLE - 
  - lastUpdatedAt: DATE NULLABLE - 
  - updates: STRING NULLABLE - 
  - documentationUrl: STRING NULLABLE - 
  - codeUrl: STRING NULLABLE - 
  - scheduleType: INTEGER NULLABLE - 
  - lastCompleted: TIMESTAMP NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 

## pco-qa.kulti_test.customer_forecast

- num_rows: 29533

- schema:
  - individualAccountID: INTEGER NULLABLE - 
  - subscription_count: INTEGER NULLABLE - 
  - total_scheduled_duration: FLOAT NULLABLE - 
  - avg_scheduled_duration: FLOAT NULLABLE - 
  - avg_customer_ratio: FLOAT NULLABLE - 
  - avg_service_ratio: FLOAT NULLABLE - 
  - avg_final_ratio: FLOAT NULLABLE - 
  - total_ntmPaidOnSite: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 

## pco-qa.kulti_test.flag_log

- num_rows: 20404

- schema:
  - flagID: STRING NULLABLE - 
  - flagDate: STRING NULLABLE - 
  - runID: STRING NULLABLE - 
  - forIDField: STRING NULLABLE - 
  - forID: STRING NULLABLE - 
  - forClient: STRING NULLABLE - 
  - flagTypeName: STRING NULLABLE - 
  - checkTypeName: STRING NULLABLE - 
  - overrideTypeID: STRING NULLABLE - 
  - overrideTypeName: STRING NULLABLE - 
  - isResolved: STRING NULLABLE - 
  - resolvedDate: STRING NULLABLE - 

## pco-qa.kulti_test.flag_type_lkp

- num_rows: 12

- schema:
  - flagTypeID: INTEGER NULLABLE - 
  - flagTypeName: STRING NULLABLE - 
  - owner: STRING NULLABLE - 
  - overrideTypeIDs: STRING NULLABLE - 
  - checkTypeIDs: STRING NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 

## pco-qa.kulti_test.full_service_type_logic

- num_rows: 62

- schema:
  - TYPE_ID: INTEGER NULLABLE - 
  - DESCRIPTION: STRING NULLABLE - 
  - API RESERVICE FLAG: INTEGER NULLABLE - 
  - API REGULAR_SERVICE FLAG: INTEGER NULLABLE - 
  - API FREQUENCY FLAG: INTEGER NULLABLE - 
  - API DEFAULT_LENGTH FLAG: INTEGER NULLABLE - 
  - hasVisitsInPast2Years: BOOLEAN NULLABLE - 
  - hasActiveSubscription: BOOLEAN NULLABLE - 
  - Repeated Name: BOOLEAN NULLABLE - 
  - API Reservice: BOOLEAN NULLABLE - 
  - API Recurring: BOOLEAN NULLABLE - 
  - API Zero Time: BOOLEAN NULLABLE - 
  - API Has Reservice: BOOLEAN NULLABLE - 
  - Word Signal Reservice: BOOLEAN NULLABLE - 
  - Word Signal Recurring: BOOLEAN NULLABLE - 
  - Word Signal Zero Time: BOOLEAN NULLABLE - 
  - Word Signal Has Reservice: BOOLEAN NULLABLE - 
  - Expired Code: BOOLEAN NULLABLE - 
  - AskClient Reservice - Reason: STRING NULLABLE - 
  - AskClient Recurring - Reason: STRING NULLABLE - 
  - AskClient Zero Time - Reason: STRING NULLABLE - 
  - AskClient Has Reservice - Reason: STRING NULLABLE - 
  - AskClient: BOOLEAN NULLABLE - 
  - Client: STRING NULLABLE - 

## pco-qa.kulti_test.inactive_subs

- num_rows: 10434

- schema:
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - CONTRACT_ADDED: STRING NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - ACTIVE_TEXT: STRING NULLABLE - 
  - INITIAL_QUOTE: STRING NULLABLE - 
  - INITIAL_DISCOUNT: STRING NULLABLE - 
  - INITIAL_SERVICE_TOTAL: STRING NULLABLE - 
  - YIF_DISCOUNT: STRING NULLABLE - 
  - RECURRING_CHARGE: STRING NULLABLE - 
  - CONTRACT_VALUE: STRING NULLABLE - 
  - ANNUAL_RECURRING_VALUE: STRING NULLABLE - 
  - BILLING_FREQUENCY: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - FOLLOWUP_SERVICE: STRING NULLABLE - 
  - AGREEMENT_LENGTH: STRING NULLABLE - 
  - NEXT_SERVICE: STRING NULLABLE - 
  - LAST_COMPLETED: STRING NULLABLE - 
  - SERVICE_ID: STRING NULLABLE - 
  - SERVICE_TYPE: STRING NULLABLE - 
  - SOLD_BY: STRING NULLABLE - 
  - SOLD_BY2: STRING NULLABLE - 
  - SOLD_BY3: STRING NULLABLE - 
  - PREFERRED_TECH: STRING NULLABLE - 
  - ADDED_BY: STRING NULLABLE - 
  - INITIAL_APPOINTMENT_ID: STRING NULLABLE - 
  - INITIAL_STATUS: STRING NULLABLE - 
  - INITIAL_STATUS_TEXT: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CXL_NOTES: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - SUBSCRIPTION_LINK: STRING NULLABLE - 
  - PO_NUMBER: STRING NULLABLE - 
  - APPOINTMENT_IDS: STRING NULLABLE - 
  - COMPLETED_APPOINTMENT_IDS: STRING NULLABLE - 
  - LEAD_ID: STRING NULLABLE - 
  - LEAD_DATE_ADDED: STRING NULLABLE - 
  - LEAD_UPDATED: STRING NULLABLE - 
  - LEAD_ADDED_BY: STRING NULLABLE - 
  - LEAD_SOURCE_ID: STRING NULLABLE - 
  - LEAD_SOURCE: STRING NULLABLE - 
  - LEAD_STATUS: STRING NULLABLE - 
  - LEAD_STATUS_TEXT: STRING NULLABLE - 
  - LEAD_STAGE_ID: STRING NULLABLE - 
  - LEAD_STAGE: STRING NULLABLE - 
  - LEAD_ASSIGNED_TO: STRING NULLABLE - 
  - LEAD_DATE_ASSIGNED: STRING NULLABLE - 
  - LEAD_VALUE: STRING NULLABLE - 
  - LEAD_DATE_CLOSED: STRING NULLABLE - 
  - LEAD_LOST_REASON: STRING NULLABLE - 
  - LEAD_LOST_REASON_TEXT: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - ANNUAL_RECURRING_SERVICES: STRING NULLABLE - 
  - REGION_ID: STRING NULLABLE - 
  - INITIAL_INVOICE: STRING NULLABLE - 
  - INITIAL_BILLING_DATE: STRING NULLABLE - 
  - RENEWAL_FREQUENCY: STRING NULLABLE - 
  - RENEWAL_DATE: STRING NULLABLE - 
  - CUSTOM_DATE: STRING NULLABLE - 
  - SENTRICON_CONNECTED: STRING NULLABLE - 
  - SENTRICON_SITE_ID: STRING NULLABLE - 
  - SEASONAL_START: STRING NULLABLE - 
  - SEASONAL_END: STRING NULLABLE - 
  - NEXT_BILLING_DATE: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - EXPIRATION_DATE: STRING NULLABLE - 
  - LAST_APPOINTMENT: STRING NULLABLE - 
  - TEMPLATE_TYPE: STRING NULLABLE - 
  - PARENT_ID: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - PREFERRED_DAYS: STRING NULLABLE - 
  - PREFERRED_START: STRING NULLABLE - 
  - PREFERRED_END: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - AUTOPAY_PAYMENT_PROFILE_ID: STRING NULLABLE - 
  - BILLING_TERMS_DAYS: STRING NULLABLE - 
  - ON_HOLD: STRING NULLABLE - 
  - CUSTOM_SCHEDULE_ID: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - RECURRING_TICKET: STRING NULLABLE - 
  - ADD_ONS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.kulti_service_types

- num_rows: 1751

- schema:
  - TYPE_ID: NUMERIC NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DESCRIPTION: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - DEFAULT_CHARGE: STRING NULLABLE - 
  - CATEGORY: STRING NULLABLE - 
  - RESERVICE: NUMERIC NULLABLE - 
  - DEFAULT_LENGTH: STRING NULLABLE - 
  - DEFAULT_INITIAL_CHARGE: STRING NULLABLE - 
  - INITIAL_ID: NUMERIC NULLABLE - 
  - MINIMUM_RECURRING_CHARGE: STRING NULLABLE - 
  - MINIMUM_INITIAL_CHARGE: STRING NULLABLE - 
  - REGULAR_SERVICE: NUMERIC NULLABLE - 
  - INITIAL: NUMERIC NULLABLE - 
  - SEASON_START: STRING NULLABLE - 
  - SEASON_END: STRING NULLABLE - 
  - GL_ACCOUNT_ID: NUMERIC NULLABLE - 
  - SENTRICON: NUMERIC NULLABLE - 
  - VISIBLE: STRING NULLABLE - 
  - DEFAULT_FOLLOWUP_DELAY: NUMERIC NULLABLE - 
  - SALES_VISIBLE: NUMERIC NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.lkp_service_type

- num_rows: 554

- schema:
  - serviceType: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - majorRecurringBucket: STRING NULLABLE - 
  - minorRecurringBucket: STRING NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - allocateReservices: BOOLEAN NULLABLE - 
  - isRervice: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - visitOverride: FLOAT NULLABLE - 
  - expiredCode: BOOLEAN NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.kulti_test.merged_appointment

- num_rows: 89016

- schema:
  - appointmentID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - appointmentDate: DATE NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - type: INTEGER NULLABLE - 
  - status: INTEGER NULLABLE - 
  - isInitial: INTEGER NULLABLE - 
  - servicedBy: INTEGER NULLABLE - 
  - timeIn: TIMESTAMP NULLABLE - 
  - timeOut: TIMESTAMP NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - productionValue: INTEGER NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.merged_customer

- num_rows: 24012

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - fname: STRING NULLABLE - 
  - lname: STRING NULLABLE - 
  - status: INTEGER NULLABLE - 
  - billToAccount: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 
  - companyName: STRING NULLABLE - 
  - masterNumber: STRING NULLABLE - 
  - email: STRING NULLABLE - 
  - phone1: STRING NULLABLE - 
  - phone2: STRING NULLABLE - 
  - customerNumber: STRING NULLABLE - 

## pco-qa.kulti_test.merged_service_type

- num_rows: 175

- schema:
  - typeId: INTEGER NULLABLE - 
  - officeId: INTEGER NULLABLE - 
  - description: STRING NULLABLE - 
  - frequency: INTEGER NULLABLE - 
  - defaultCharge: FLOAT NULLABLE - 
  - category: STRING NULLABLE - 
  - reservice: INTEGER NULLABLE - 
  - defaultLength: INTEGER NULLABLE - 
  - defaultInitialCharge: FLOAT NULLABLE - 
  - initialId: INTEGER NULLABLE - 
  - minimumRecurringCharge: FLOAT NULLABLE - 
  - minimumInitialCharge: FLOAT NULLABLE - 
  - regularService: INTEGER NULLABLE - 
  - initial: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.merged_subscription

- num_rows: 16848

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - active: INTEGER NULLABLE - 
  - recurringCharge: FLOAT NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 
  - preferredDays: INTEGER NULLABLE - 
  - preferredStart: TIME NULLABLE - 
  - preferredEnd: TIME NULLABLE - 
  - recurringTicket: STRING NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - seasonalStart: TIMESTAMP NULLABLE - 
  - seasonalEnd: TIMESTAMP NULLABLE - 
  - yifDiscount: FLOAT NULLABLE - 

## pco-qa.kulti_test.merged_ticket

- num_rows: 1173206

- schema:
  - ticketID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - prodFromTicket: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 
  - ticketProductionValue: FLOAT NULLABLE - 
  - active: INTEGER NULLABLE - 
  - taxAmount: FLOAT NULLABLE - 
  - total: FLOAT NULLABLE - 
  - serviceCharge: FLOAT NULLABLE - 
  - serviceTaxable: INTEGER NULLABLE - 
  - balance: FLOAT NULLABLE - 

## pco-qa.kulti_test.override_log

- num_rows: 6

- schema:
  - flagID: STRING NULLABLE - 
  - overrideTypeID: STRING NULLABLE - 
  - appliedDate: TIMESTAMP NULLABLE - 
  - isAuto: BOOLEAN NULLABLE - 
  - appliedBy: STRING NULLABLE - 

## pco-qa.kulti_test.override_type_lkp

- num_rows: 10

- schema:
  - overrideTypeID: INTEGER NULLABLE - 
  - overrideTypeName: STRING NULLABLE - 
  - flagID: INTEGER NULLABLE - 
  - overrideTable: STRING NULLABLE - 
  - overrideField: STRING NULLABLE - 
  - appliesToQuery: STRING NULLABLE - 
  - appliesToClient: STRING NULLABLE - 
  - oldValue: STRING NULLABLE - 
  - newValue: STRING NULLABLE - 
  - recurrence: INTEGER NULLABLE - 
  - lastCompleted: TIMESTAMP NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - notes: STRING NULLABLE - 

## pco-qa.kulti_test.subscription_forecast

- num_rows: 46174

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - dateAdded: DATETIME NULLABLE - 
  - active: INTEGER NULLABLE - 
  - recurringCharge: FLOAT NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - default_length: FLOAT NULLABLE - 
  - effective_duration: FLOAT NULLABLE - 
  - scheduled_duration: FLOAT NULLABLE - 
  - customer_ratio: FLOAT NULLABLE - 
  - service_ratio: FLOAT NULLABLE - 
  - final_ratio: FLOAT NULLABLE - 
  - forecast: FLOAT NULLABLE - 

## pco-qa.kulti_test.t_appointment

- num_rows: 3082125

- schema:
  - appointmentID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - appointmentDate: DATE NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - type: INTEGER NULLABLE - 
  - status: INTEGER NULLABLE - 
  - isInitial: INTEGER NULLABLE - 
  - servicedBy: INTEGER NULLABLE - 
  - timeIn: TIMESTAMP NULLABLE - 
  - timeOut: TIMESTAMP NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - productionValue: INTEGER NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.t_service_type

- num_rows: 3991

- schema:
  - typeId: INTEGER NULLABLE - 
  - officeId: INTEGER NULLABLE - 
  - description: STRING NULLABLE - 
  - frequency: INTEGER NULLABLE - 
  - defaultCharge: FLOAT NULLABLE - 
  - category: STRING NULLABLE - 
  - reservice: INTEGER NULLABLE - 
  - defaultLength: INTEGER NULLABLE - 
  - defaultInitialCharge: FLOAT NULLABLE - 
  - initialId: INTEGER NULLABLE - 
  - minimumRecurringCharge: FLOAT NULLABLE - 
  - minimumInitialCharge: FLOAT NULLABLE - 
  - regularService: INTEGER NULLABLE - 
  - initial: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.kulti_test.t_subscription

- num_rows: 376005

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - active: INTEGER NULLABLE - 
  - recurringCharge: FLOAT NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - dateCancelled: TIMESTAMP NULLABLE - 
  - preferredDays: INTEGER NULLABLE - 
  - preferredStart: TIME NULLABLE - 
  - preferredEnd: TIME NULLABLE - 
  - recurringTicket: STRING NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - dateUpdated: TIMESTAMP NULLABLE - 
  - contractAdded: TIMESTAMP NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - agreementLength: INTEGER NULLABLE - 
  - seasonalStart: TIMESTAMP NULLABLE - 
  - seasonalEnd: TIMESTAMP NULLABLE - 
  - yifDiscount: FLOAT NULLABLE - 

## pco-qa.poc_layer.FR_APPOINTMENT

- num_rows: 2072863

- schema:
  - APPOINTMENT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - SUBSCRIPTION_REGION_ID: STRING NULLABLE - 
  - ROUTE_ID: STRING NULLABLE - 
  - SPOT_ID: STRING NULLABLE - 
  - DATE: STRING NULLABLE - 
  - START_C: STRING NULLABLE - 
  - END_C: STRING NULLABLE - 
  - TIME_WINDOW: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - TYPE: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - EMPLOYEE_ID: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - IS_INITIAL: STRING NULLABLE - 
  - SUBSCRIPTION_PREFERRED_TECH: STRING NULLABLE - 
  - COMPLETED_BY: STRING NULLABLE - 
  - SERVICED_BY: STRING NULLABLE - 
  - DATE_COMPLETED: STRING NULLABLE - 
  - SIGNED_BY_CUSTOMER: STRING NULLABLE - 
  - SIGNED_BY_TECH: STRING NULLABLE - 
  - NOTES: STRING NULLABLE - 
  - OFFICE_NOTES: STRING NULLABLE - 
  - TIME_IN: STRING NULLABLE - 
  - TIME_OUT: STRING NULLABLE - 
  - CHECK_IN: STRING NULLABLE - 
  - CHECK_OUT: STRING NULLABLE - 
  - WIND_SPEED: STRING NULLABLE - 
  - WIND_DIRECTION: STRING NULLABLE - 
  - TEMPERATURE: STRING NULLABLE - 
  - AMOUNT_COLLECTED: STRING NULLABLE - 
  - PAYMENT_METHOD: STRING NULLABLE - 
  - SERVICED_INTERIOR: STRING NULLABLE - 
  - TICKET_ID: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - ADDITIONAL_TECHS: STRING NULLABLE - 
  - CANCELLATION_REASON_ID: STRING NULLABLE - 
  - RESCHEDULE_REASON_ID: STRING NULLABLE - 
  - RESERVICE_REASON_ID: STRING NULLABLE - 
  - APPOINTMENT_NOTES: STRING NULLABLE - 
  - DO_INTERIOR: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - ASSIGNED_TECH: STRING NULLABLE - 
  - LAT_IN: STRING NULLABLE - 
  - LAT_OUT: STRING NULLABLE - 
  - LONG_IN: STRING NULLABLE - 
  - LONG_OUT: STRING NULLABLE - 
  - SEQUENCE: STRING NULLABLE - 
  - LOCKED_BY: STRING NULLABLE - 
  - ORIGINAL_APPOINTMENT_ID: STRING NULLABLE - 
  - PRODUCTION_VALUE: STRING NULLABLE - 
  - DUE_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.poc_layer.FR_INCREMENTAL_SYNC_METADATA

- num_rows: 7

- schema:
  - CLIENT: STRING NULLABLE - 
  - ENDPOINT: STRING NULLABLE - 
  - REACHED_RATE_LIMIT: BOOLEAN NULLABLE - 
  - PREVIOUS_DATE_LOADED: TIMESTAMP NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.poc_layer.FR_SUBSCRIPTION

- num_rows: 180464

- schema:
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - CONTRACT_ADDED: STRING NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - ACTIVE_TEXT: STRING NULLABLE - 
  - INITIAL_QUOTE: STRING NULLABLE - 
  - INITIAL_DISCOUNT: STRING NULLABLE - 
  - INITIAL_SERVICE_TOTAL: STRING NULLABLE - 
  - YIF_DISCOUNT: STRING NULLABLE - 
  - RECURRING_CHARGE: STRING NULLABLE - 
  - CONTRACT_VALUE: STRING NULLABLE - 
  - ANNUAL_RECURRING_VALUE: STRING NULLABLE - 
  - BILLING_FREQUENCY: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - FOLLOWUP_SERVICE: STRING NULLABLE - 
  - AGREEMENT_LENGTH: STRING NULLABLE - 
  - NEXT_SERVICE: STRING NULLABLE - 
  - LAST_COMPLETED: STRING NULLABLE - 
  - SERVICE_ID: STRING NULLABLE - 
  - SERVICE_TYPE: STRING NULLABLE - 
  - SOLD_BY: STRING NULLABLE - 
  - SOLD_BY2: STRING NULLABLE - 
  - SOLD_BY3: STRING NULLABLE - 
  - PREFERRED_TECH: STRING NULLABLE - 
  - ADDED_BY: STRING NULLABLE - 
  - INITIAL_APPOINTMENT_ID: STRING NULLABLE - 
  - INITIAL_STATUS: STRING NULLABLE - 
  - INITIAL_STATUS_TEXT: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CXL_NOTES: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - SUBSCRIPTION_LINK: STRING NULLABLE - 
  - PO_NUMBER: STRING NULLABLE - 
  - APPOINTMENT_IDS: STRING NULLABLE - 
  - COMPLETED_APPOINTMENT_IDS: STRING NULLABLE - 
  - LEAD_ID: STRING NULLABLE - 
  - LEAD_DATE_ADDED: STRING NULLABLE - 
  - LEAD_UPDATED: STRING NULLABLE - 
  - LEAD_ADDED_BY: STRING NULLABLE - 
  - LEAD_SOURCE_ID: STRING NULLABLE - 
  - LEAD_SOURCE: STRING NULLABLE - 
  - LEAD_STATUS: STRING NULLABLE - 
  - LEAD_STATUS_TEXT: STRING NULLABLE - 
  - LEAD_STAGE_ID: STRING NULLABLE - 
  - LEAD_STAGE: STRING NULLABLE - 
  - LEAD_ASSIGNED_TO: STRING NULLABLE - 
  - LEAD_DATE_ASSIGNED: STRING NULLABLE - 
  - LEAD_VALUE: STRING NULLABLE - 
  - LEAD_DATE_CLOSED: STRING NULLABLE - 
  - LEAD_LOST_REASON: STRING NULLABLE - 
  - LEAD_LOST_REASON_TEXT: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - ANNUAL_RECURRING_SERVICES: STRING NULLABLE - 
  - REGION_ID: STRING NULLABLE - 
  - INITIAL_INVOICE: STRING NULLABLE - 
  - INITIAL_BILLING_DATE: STRING NULLABLE - 
  - RENEWAL_FREQUENCY: STRING NULLABLE - 
  - RENEWAL_DATE: STRING NULLABLE - 
  - CUSTOM_DATE: STRING NULLABLE - 
  - SENTRICON_CONNECTED: STRING NULLABLE - 
  - SENTRICON_SITE_ID: STRING NULLABLE - 
  - SEASONAL_START: STRING NULLABLE - 
  - SEASONAL_END: STRING NULLABLE - 
  - NEXT_BILLING_DATE: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - EXPIRATION_DATE: STRING NULLABLE - 
  - LAST_APPOINTMENT: STRING NULLABLE - 
  - TEMPLATE_TYPE: STRING NULLABLE - 
  - PARENT_ID: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - PREFERRED_DAYS: STRING NULLABLE - 
  - PREFERRED_START: STRING NULLABLE - 
  - PREFERRED_END: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - AUTOPAY_PAYMENT_PROFILE_ID: STRING NULLABLE - 
  - BILLING_TERMS_DAYS: STRING NULLABLE - 
  - ON_HOLD: STRING NULLABLE - 
  - CUSTOM_SCHEDULE_ID: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - RECURRING_TICKET: STRING NULLABLE - 
  - ADD_ONS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.qbo_central.PL_CashQuell

- num_rows: 19643

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.qbo_central.PL_GreenCountry

- num_rows: 24567

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.qbo_central.PL_PISGAH

- num_rows: 31680

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - location: STRING NULLABLE - 
  - class: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.qbo_central.PL_Pisgah

- num_rows: 31680

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.qbo_central.PL_Quell

- num_rows: 20041

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.qbo_central.PL_STB

- num_rows: 15979

- schema:
  - date: STRING NULLABLE - 
  - transaction_type: STRING NULLABLE - 
  - num: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - memo_description: STRING NULLABLE - 
  - split: STRING NULLABLE - 
  - amount: STRING NULLABLE - 
  - balance: STRING NULLABLE - 
  - header_1: STRING NULLABLE - 
  - header_2: STRING NULLABLE - 
  - header_3: STRING NULLABLE - 
  - header_4: STRING NULLABLE - 
  - header_5: STRING NULLABLE - 

## pco-qa.raw_layer.FR_APPLIED_PAYMENT

- num_rows: 2334171

- schema:
  - APPLIED_PAYMENT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - PAYMENT_ID: STRING NULLABLE - 
  - TICKET_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - DATE_APPLIED: STRING NULLABLE - 
  - APPLIED_BY: STRING NULLABLE - 
  - APPLIED_AMOUNT: STRING NULLABLE - 
  - TAX_COLLECTED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_APPLIED_PAYMENT_OLD

- num_rows: 1199705

- schema:
  - APPLIED_PAYMENT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - PAYMENT_ID: STRING NULLABLE - 
  - TICKET_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - DATE_APPLIED: STRING NULLABLE - 
  - APPLIED_BY: STRING NULLABLE - 
  - APPLIED_AMOUNT: STRING NULLABLE - 
  - TAX_COLLECTED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_APPOINTMENT

- num_rows: 3634253

- schema:
  - APPOINTMENT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - SUBSCRIPTION_REGION_ID: STRING NULLABLE - 
  - ROUTE_ID: STRING NULLABLE - 
  - SPOT_ID: STRING NULLABLE - 
  - DATE: STRING NULLABLE - 
  - START_C: STRING NULLABLE - 
  - END_C: STRING NULLABLE - 
  - TIME_WINDOW: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - TYPE: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - EMPLOYEE_ID: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - IS_INITIAL: STRING NULLABLE - 
  - SUBSCRIPTION_PREFERRED_TECH: STRING NULLABLE - 
  - COMPLETED_BY: STRING NULLABLE - 
  - SERVICED_BY: STRING NULLABLE - 
  - DATE_COMPLETED: STRING NULLABLE - 
  - SIGNED_BY_CUSTOMER: STRING NULLABLE - 
  - SIGNED_BY_TECH: STRING NULLABLE - 
  - NOTES: STRING NULLABLE - 
  - OFFICE_NOTES: STRING NULLABLE - 
  - TIME_IN: STRING NULLABLE - 
  - TIME_OUT: STRING NULLABLE - 
  - CHECK_IN: STRING NULLABLE - 
  - CHECK_OUT: STRING NULLABLE - 
  - WIND_SPEED: STRING NULLABLE - 
  - WIND_DIRECTION: STRING NULLABLE - 
  - TEMPERATURE: STRING NULLABLE - 
  - AMOUNT_COLLECTED: STRING NULLABLE - 
  - PAYMENT_METHOD: STRING NULLABLE - 
  - SERVICED_INTERIOR: STRING NULLABLE - 
  - TICKET_ID: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - ADDITIONAL_TECHS: STRING NULLABLE - 
  - CANCELLATION_REASON_ID: STRING NULLABLE - 
  - RESCHEDULE_REASON_ID: STRING NULLABLE - 
  - RESERVICE_REASON_ID: STRING NULLABLE - 
  - APPOINTMENT_NOTES: STRING NULLABLE - 
  - DO_INTERIOR: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - ASSIGNED_TECH: STRING NULLABLE - 
  - LAT_IN: STRING NULLABLE - 
  - LAT_OUT: STRING NULLABLE - 
  - LONG_IN: STRING NULLABLE - 
  - LONG_OUT: STRING NULLABLE - 
  - SEQUENCE: STRING NULLABLE - 
  - LOCKED_BY: STRING NULLABLE - 
  - ORIGINAL_APPOINTMENT_ID: STRING NULLABLE - 
  - PRODUCTION_VALUE: STRING NULLABLE - 
  - DUE_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_APPOINTMENT_OLD

- num_rows: 49220

- schema:
  - APPOINTMENT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - SUBSCRIPTION_REGION_ID: STRING NULLABLE - 
  - ROUTE_ID: STRING NULLABLE - 
  - SPOT_ID: STRING NULLABLE - 
  - DATE: STRING NULLABLE - 
  - START_C: STRING NULLABLE - 
  - END_C: STRING NULLABLE - 
  - TIME_WINDOW: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - TYPE: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - EMPLOYEE_ID: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - IS_INITIAL: STRING NULLABLE - 
  - SUBSCRIPTION_PREFERRED_TECH: STRING NULLABLE - 
  - COMPLETED_BY: STRING NULLABLE - 
  - SERVICED_BY: STRING NULLABLE - 
  - DATE_COMPLETED: STRING NULLABLE - 
  - SIGNED_BY_CUSTOMER: STRING NULLABLE - 
  - SIGNED_BY_TECH: STRING NULLABLE - 
  - NOTES: STRING NULLABLE - 
  - OFFICE_NOTES: STRING NULLABLE - 
  - TIME_IN: STRING NULLABLE - 
  - TIME_OUT: STRING NULLABLE - 
  - CHECK_IN: STRING NULLABLE - 
  - CHECK_OUT: STRING NULLABLE - 
  - WIND_SPEED: STRING NULLABLE - 
  - WIND_DIRECTION: STRING NULLABLE - 
  - TEMPERATURE: STRING NULLABLE - 
  - AMOUNT_COLLECTED: STRING NULLABLE - 
  - PAYMENT_METHOD: STRING NULLABLE - 
  - SERVICED_INTERIOR: STRING NULLABLE - 
  - TICKET_ID: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - ADDITIONAL_TECHS: STRING NULLABLE - 
  - CANCELLATION_REASON_ID: STRING NULLABLE - 
  - RESCHEDULE_REASON_ID: STRING NULLABLE - 
  - RESERVICE_REASON_ID: STRING NULLABLE - 
  - APPOINTMENT_NOTES: STRING NULLABLE - 
  - DO_INTERIOR: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - ASSIGNED_TECH: STRING NULLABLE - 
  - LAT_IN: STRING NULLABLE - 
  - LAT_OUT: STRING NULLABLE - 
  - LONG_IN: STRING NULLABLE - 
  - LONG_OUT: STRING NULLABLE - 
  - SEQUENCE: STRING NULLABLE - 
  - LOCKED_BY: STRING NULLABLE - 
  - ORIGINAL_APPOINTMENT_ID: STRING NULLABLE - 
  - PRODUCTION_VALUE: STRING NULLABLE - 
  - DUE_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_CUSTOMER

- num_rows: 432295

- schema:
  - CUSTOMER_ID: STRING NULLABLE - 
  - FNAME: STRING NULLABLE - 
  - LNAME: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - COMMERCIAL_ACCOUNT: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - COMPANY_NAME: STRING NULLABLE - 
  - BILLING_COMPANY_NAME: STRING NULLABLE - 
  - LAT: STRING NULLABLE - 
  - LNG: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - A_PAY: STRING NULLABLE - 
  - PREFERRED_TECH_ID: STRING NULLABLE - 
  - PAID_IN_FULL: STRING NULLABLE - 
  - SUBSCRIPTION_IDS: STRING NULLABLE - 
  - BALANCE: STRING NULLABLE - 
  - BALANCE_AGE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE_AGE: STRING NULLABLE - 
  - CUSTOMER_NUMBER: STRING NULLABLE - 
  - MASTER_NUMBER: STRING NULLABLE - 
  - PAYMENT_HOLD_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - SPECIAL_SCHEDULING: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - USE_STRUCTURES: STRING NULLABLE - 
  - IS_MULTI_UNIT: STRING NULLABLE - 
  - AGING_DATE: STRING NULLABLE - 
  - RESPONSIBLE_AGING_DATE: STRING NULLABLE - 
  - EMAIL: STRING NULLABLE - 
  - PHONE1: STRING NULLABLE - 
  - PHONE2: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_CUSTOMER_ACCEL

- num_rows: 70507

- schema:
  - CUSTOMER_ID: STRING NULLABLE - 
  - FNAME: STRING NULLABLE - 
  - LNAME: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - COMMERCIAL_ACCOUNT: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - COMPANY_NAME: STRING NULLABLE - 
  - BILLING_COMPANY_NAME: STRING NULLABLE - 
  - LAT: STRING NULLABLE - 
  - LNG: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - A_PAY: STRING NULLABLE - 
  - PREFERRED_TECH_ID: STRING NULLABLE - 
  - PAID_IN_FULL: STRING NULLABLE - 
  - SUBSCRIPTION_IDS: STRING NULLABLE - 
  - BALANCE: STRING NULLABLE - 
  - BALANCE_AGE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE_AGE: STRING NULLABLE - 
  - CUSTOMER_NUMBER: STRING NULLABLE - 
  - MASTER_NUMBER: STRING NULLABLE - 
  - PAYMENT_HOLD_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - SPECIAL_SCHEDULING: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - USE_STRUCTURES: STRING NULLABLE - 
  - IS_MULTI_UNIT: STRING NULLABLE - 
  - AGING_DATE: STRING NULLABLE - 
  - RESPONSIBLE_AGING_DATE: STRING NULLABLE - 
  - EMAIL: STRING NULLABLE - 
  - PHONE1: STRING NULLABLE - 
  - PHONE2: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_CUSTOMER_GREENCOUNTRY

- num_rows: 0

- schema:
  - CUSTOMER_ID: STRING NULLABLE - 
  - FNAME: STRING NULLABLE - 
  - LNAME: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - COMMERCIAL_ACCOUNT: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - COMPANY_NAME: STRING NULLABLE - 
  - BILLING_COMPANY_NAME: STRING NULLABLE - 
  - LAT: STRING NULLABLE - 
  - LNG: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - A_PAY: STRING NULLABLE - 
  - PREFERRED_TECH_ID: STRING NULLABLE - 
  - PAID_IN_FULL: STRING NULLABLE - 
  - SUBSCRIPTION_IDS: STRING NULLABLE - 
  - BALANCE: STRING NULLABLE - 
  - BALANCE_AGE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE_AGE: STRING NULLABLE - 
  - CUSTOMER_NUMBER: STRING NULLABLE - 
  - MASTER_NUMBER: STRING NULLABLE - 
  - PAYMENT_HOLD_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - SPECIAL_SCHEDULING: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - USE_STRUCTURES: STRING NULLABLE - 
  - IS_MULTI_UNIT: STRING NULLABLE - 
  - AGING_DATE: STRING NULLABLE - 
  - RESPONSIBLE_AGING_DATE: STRING NULLABLE - 
  - EMAIL: STRING NULLABLE - 
  - PHONE1: STRING NULLABLE - 
  - PHONE2: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_CUSTOMER_OLD

- num_rows: 44692

- schema:
  - CUSTOMER_ID: STRING NULLABLE - 
  - FNAME: STRING NULLABLE - 
  - LNAME: STRING NULLABLE - 
  - STATUS: STRING NULLABLE - 
  - STATUS_TEXT: STRING NULLABLE - 
  - COMMERCIAL_ACCOUNT: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - COMPANY_NAME: STRING NULLABLE - 
  - BILLING_COMPANY_NAME: STRING NULLABLE - 
  - LAT: STRING NULLABLE - 
  - LNG: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - A_PAY: STRING NULLABLE - 
  - PREFERRED_TECH_ID: STRING NULLABLE - 
  - PAID_IN_FULL: STRING NULLABLE - 
  - SUBSCRIPTION_IDS: STRING NULLABLE - 
  - BALANCE: STRING NULLABLE - 
  - BALANCE_AGE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE: STRING NULLABLE - 
  - RESPONSIBLE_BALANCE_AGE: STRING NULLABLE - 
  - MASTER_ACCOUNT: STRING NULLABLE - 
  - PAYMENT_HOLD_DATE: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - SPECIAL_SCHEDULING: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - USE_STRUCTURES: STRING NULLABLE - 
  - IS_MULTI_UNIT: STRING NULLABLE - 
  - AGING_DATE: STRING NULLABLE - 
  - RESPONSIBLE_AGING_DATE: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_INCREMENTAL_SYNC_METADATA

- num_rows: 65

- schema:
  - CLIENT: STRING NULLABLE - 
  - ENDPOINT: STRING NULLABLE - 
  - REACHED_RATE_LIMIT: BOOLEAN NULLABLE - 
  - PREVIOUS_DATE_LOADED: TIMESTAMP NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_INCREMENTAL_SYNC_METADATA_BACKUP_20250612

- num_rows: 65

- schema:
  - CLIENT: STRING NULLABLE - 
  - ENDPOINT: STRING NULLABLE - 
  - REACHED_RATE_LIMIT: BOOLEAN NULLABLE - 
  - PREVIOUS_DATE_LOADED: TIMESTAMP NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_INCREMENTAL_SYNC_METADATA_BACKUP_20250616

- num_rows: 65

- schema:
  - CLIENT: STRING NULLABLE - 
  - ENDPOINT: STRING NULLABLE - 
  - REACHED_RATE_LIMIT: BOOLEAN NULLABLE - 
  - PREVIOUS_DATE_LOADED: TIMESTAMP NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_INCREMENTAL_SYNC_METADATA_BACKUP_20250629

- num_rows: 65

- schema:
  - CLIENT: STRING NULLABLE - 
  - ENDPOINT: STRING NULLABLE - 
  - REACHED_RATE_LIMIT: BOOLEAN NULLABLE - 
  - PREVIOUS_DATE_LOADED: TIMESTAMP NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_SERVICE_TYPE

- num_rows: 125111

- schema:
  - TYPE_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DESCRIPTION: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - DEFAULT_CHARGE: STRING NULLABLE - 
  - CATEGORY: STRING NULLABLE - 
  - RESERVICE: STRING NULLABLE - 
  - DEFAULT_LENGTH: STRING NULLABLE - 
  - DEFAULT_INITIAL_CHARGE: STRING NULLABLE - 
  - INITIAL_ID: STRING NULLABLE - 
  - MINIMUM_RECURRING_CHARGE: STRING NULLABLE - 
  - MINIMUM_INITIAL_CHARGE: STRING NULLABLE - 
  - REGULAR_SERVICE: STRING NULLABLE - 
  - INITIAL: STRING NULLABLE - 
  - SEASON_START: STRING NULLABLE - 
  - SEASON_END: STRING NULLABLE - 
  - GL_ACCOUNT_ID: STRING NULLABLE - 
  - SENTRICON: STRING NULLABLE - 
  - VISIBLE: STRING NULLABLE - 
  - DEFAULT_FOLLOWUP_DELAY: STRING NULLABLE - 
  - SALES_VISIBLE: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: STRING NULLABLE - 

## pco-qa.raw_layer.FR_SUBSCRIPTION

- num_rows: 579911

- schema:
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - CONTRACT_ADDED: STRING NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - ACTIVE_TEXT: STRING NULLABLE - 
  - INITIAL_QUOTE: STRING NULLABLE - 
  - INITIAL_DISCOUNT: STRING NULLABLE - 
  - INITIAL_SERVICE_TOTAL: STRING NULLABLE - 
  - YIF_DISCOUNT: STRING NULLABLE - 
  - RECURRING_CHARGE: STRING NULLABLE - 
  - CONTRACT_VALUE: STRING NULLABLE - 
  - ANNUAL_RECURRING_VALUE: STRING NULLABLE - 
  - BILLING_FREQUENCY: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - FOLLOWUP_SERVICE: STRING NULLABLE - 
  - AGREEMENT_LENGTH: STRING NULLABLE - 
  - NEXT_SERVICE: STRING NULLABLE - 
  - LAST_COMPLETED: STRING NULLABLE - 
  - SERVICE_ID: STRING NULLABLE - 
  - SERVICE_TYPE: STRING NULLABLE - 
  - SOLD_BY: STRING NULLABLE - 
  - SOLD_BY2: STRING NULLABLE - 
  - SOLD_BY3: STRING NULLABLE - 
  - PREFERRED_TECH: STRING NULLABLE - 
  - ADDED_BY: STRING NULLABLE - 
  - INITIAL_APPOINTMENT_ID: STRING NULLABLE - 
  - INITIAL_STATUS: STRING NULLABLE - 
  - INITIAL_STATUS_TEXT: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CXL_NOTES: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - SUBSCRIPTION_LINK: STRING NULLABLE - 
  - PO_NUMBER: STRING NULLABLE - 
  - APPOINTMENT_IDS: STRING NULLABLE - 
  - COMPLETED_APPOINTMENT_IDS: STRING NULLABLE - 
  - LEAD_ID: STRING NULLABLE - 
  - LEAD_DATE_ADDED: STRING NULLABLE - 
  - LEAD_UPDATED: STRING NULLABLE - 
  - LEAD_ADDED_BY: STRING NULLABLE - 
  - LEAD_SOURCE_ID: STRING NULLABLE - 
  - LEAD_SOURCE: STRING NULLABLE - 
  - LEAD_STATUS: STRING NULLABLE - 
  - LEAD_STATUS_TEXT: STRING NULLABLE - 
  - LEAD_STAGE_ID: STRING NULLABLE - 
  - LEAD_STAGE: STRING NULLABLE - 
  - LEAD_ASSIGNED_TO: STRING NULLABLE - 
  - LEAD_DATE_ASSIGNED: STRING NULLABLE - 
  - LEAD_VALUE: STRING NULLABLE - 
  - LEAD_DATE_CLOSED: STRING NULLABLE - 
  - LEAD_LOST_REASON: STRING NULLABLE - 
  - LEAD_LOST_REASON_TEXT: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - ANNUAL_RECURRING_SERVICES: STRING NULLABLE - 
  - REGION_ID: STRING NULLABLE - 
  - INITIAL_INVOICE: STRING NULLABLE - 
  - INITIAL_BILLING_DATE: STRING NULLABLE - 
  - RENEWAL_FREQUENCY: STRING NULLABLE - 
  - RENEWAL_DATE: STRING NULLABLE - 
  - CUSTOM_DATE: STRING NULLABLE - 
  - SENTRICON_CONNECTED: STRING NULLABLE - 
  - SENTRICON_SITE_ID: STRING NULLABLE - 
  - SEASONAL_START: STRING NULLABLE - 
  - SEASONAL_END: STRING NULLABLE - 
  - NEXT_BILLING_DATE: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - EXPIRATION_DATE: STRING NULLABLE - 
  - LAST_APPOINTMENT: STRING NULLABLE - 
  - TEMPLATE_TYPE: STRING NULLABLE - 
  - PARENT_ID: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - PREFERRED_DAYS: STRING NULLABLE - 
  - PREFERRED_START: STRING NULLABLE - 
  - PREFERRED_END: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - AUTOPAY_PAYMENT_PROFILE_ID: STRING NULLABLE - 
  - BILLING_TERMS_DAYS: STRING NULLABLE - 
  - ON_HOLD: STRING NULLABLE - 
  - CUSTOM_SCHEDULE_ID: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - RECURRING_TICKET: STRING NULLABLE - 
  - ADD_ONS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_SUBSCRIPTION_ACCEL

- num_rows: 86646

- schema:
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - CONTRACT_ADDED: STRING NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - ACTIVE_TEXT: STRING NULLABLE - 
  - INITIAL_QUOTE: STRING NULLABLE - 
  - INITIAL_DISCOUNT: STRING NULLABLE - 
  - INITIAL_SERVICE_TOTAL: STRING NULLABLE - 
  - YIF_DISCOUNT: STRING NULLABLE - 
  - RECURRING_CHARGE: STRING NULLABLE - 
  - CONTRACT_VALUE: STRING NULLABLE - 
  - ANNUAL_RECURRING_VALUE: STRING NULLABLE - 
  - BILLING_FREQUENCY: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - FOLLOWUP_SERVICE: STRING NULLABLE - 
  - AGREEMENT_LENGTH: STRING NULLABLE - 
  - NEXT_SERVICE: STRING NULLABLE - 
  - LAST_COMPLETED: STRING NULLABLE - 
  - SERVICE_ID: STRING NULLABLE - 
  - SERVICE_TYPE: STRING NULLABLE - 
  - SOLD_BY: STRING NULLABLE - 
  - SOLD_BY2: STRING NULLABLE - 
  - SOLD_BY3: STRING NULLABLE - 
  - PREFERRED_TECH: STRING NULLABLE - 
  - ADDED_BY: STRING NULLABLE - 
  - INITIAL_APPOINTMENT_ID: STRING NULLABLE - 
  - INITIAL_STATUS: STRING NULLABLE - 
  - INITIAL_STATUS_TEXT: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CXL_NOTES: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - SUBSCRIPTION_LINK: STRING NULLABLE - 
  - PO_NUMBER: STRING NULLABLE - 
  - APPOINTMENT_IDS: STRING NULLABLE - 
  - COMPLETED_APPOINTMENT_IDS: STRING NULLABLE - 
  - LEAD_ID: STRING NULLABLE - 
  - LEAD_DATE_ADDED: STRING NULLABLE - 
  - LEAD_UPDATED: STRING NULLABLE - 
  - LEAD_ADDED_BY: STRING NULLABLE - 
  - LEAD_SOURCE_ID: STRING NULLABLE - 
  - LEAD_SOURCE: STRING NULLABLE - 
  - LEAD_STATUS: STRING NULLABLE - 
  - LEAD_STATUS_TEXT: STRING NULLABLE - 
  - LEAD_STAGE_ID: STRING NULLABLE - 
  - LEAD_STAGE: STRING NULLABLE - 
  - LEAD_ASSIGNED_TO: STRING NULLABLE - 
  - LEAD_DATE_ASSIGNED: STRING NULLABLE - 
  - LEAD_VALUE: STRING NULLABLE - 
  - LEAD_DATE_CLOSED: STRING NULLABLE - 
  - LEAD_LOST_REASON: STRING NULLABLE - 
  - LEAD_LOST_REASON_TEXT: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - ANNUAL_RECURRING_SERVICES: STRING NULLABLE - 
  - REGION_ID: STRING NULLABLE - 
  - INITIAL_INVOICE: STRING NULLABLE - 
  - INITIAL_BILLING_DATE: STRING NULLABLE - 
  - RENEWAL_FREQUENCY: STRING NULLABLE - 
  - RENEWAL_DATE: STRING NULLABLE - 
  - CUSTOM_DATE: STRING NULLABLE - 
  - SENTRICON_CONNECTED: STRING NULLABLE - 
  - SENTRICON_SITE_ID: STRING NULLABLE - 
  - SEASONAL_START: STRING NULLABLE - 
  - SEASONAL_END: STRING NULLABLE - 
  - NEXT_BILLING_DATE: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - EXPIRATION_DATE: STRING NULLABLE - 
  - LAST_APPOINTMENT: STRING NULLABLE - 
  - TEMPLATE_TYPE: STRING NULLABLE - 
  - PARENT_ID: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - PREFERRED_DAYS: STRING NULLABLE - 
  - PREFERRED_START: STRING NULLABLE - 
  - PREFERRED_END: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - AUTOPAY_PAYMENT_PROFILE_ID: STRING NULLABLE - 
  - BILLING_TERMS_DAYS: STRING NULLABLE - 
  - ON_HOLD: STRING NULLABLE - 
  - CUSTOM_SCHEDULE_ID: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - RECURRING_TICKET: STRING NULLABLE - 
  - ADD_ONS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_SUBSCRIPTION_GREENCOUNTRY

- num_rows: 16429

- schema:
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - CONTRACT_ADDED: STRING NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - ACTIVE_TEXT: STRING NULLABLE - 
  - INITIAL_QUOTE: STRING NULLABLE - 
  - INITIAL_DISCOUNT: STRING NULLABLE - 
  - INITIAL_SERVICE_TOTAL: STRING NULLABLE - 
  - YIF_DISCOUNT: STRING NULLABLE - 
  - RECURRING_CHARGE: STRING NULLABLE - 
  - CONTRACT_VALUE: STRING NULLABLE - 
  - ANNUAL_RECURRING_VALUE: STRING NULLABLE - 
  - BILLING_FREQUENCY: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - FOLLOWUP_SERVICE: STRING NULLABLE - 
  - AGREEMENT_LENGTH: STRING NULLABLE - 
  - NEXT_SERVICE: STRING NULLABLE - 
  - LAST_COMPLETED: STRING NULLABLE - 
  - SERVICE_ID: STRING NULLABLE - 
  - SERVICE_TYPE: STRING NULLABLE - 
  - SOLD_BY: STRING NULLABLE - 
  - SOLD_BY2: STRING NULLABLE - 
  - SOLD_BY3: STRING NULLABLE - 
  - PREFERRED_TECH: STRING NULLABLE - 
  - ADDED_BY: STRING NULLABLE - 
  - INITIAL_APPOINTMENT_ID: STRING NULLABLE - 
  - INITIAL_STATUS: STRING NULLABLE - 
  - INITIAL_STATUS_TEXT: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CXL_NOTES: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - SUBSCRIPTION_LINK: STRING NULLABLE - 
  - PO_NUMBER: STRING NULLABLE - 
  - APPOINTMENT_IDS: STRING NULLABLE - 
  - COMPLETED_APPOINTMENT_IDS: STRING NULLABLE - 
  - LEAD_ID: STRING NULLABLE - 
  - LEAD_DATE_ADDED: STRING NULLABLE - 
  - LEAD_UPDATED: STRING NULLABLE - 
  - LEAD_ADDED_BY: STRING NULLABLE - 
  - LEAD_SOURCE_ID: STRING NULLABLE - 
  - LEAD_SOURCE: STRING NULLABLE - 
  - LEAD_STATUS: STRING NULLABLE - 
  - LEAD_STATUS_TEXT: STRING NULLABLE - 
  - LEAD_STAGE_ID: STRING NULLABLE - 
  - LEAD_STAGE: STRING NULLABLE - 
  - LEAD_ASSIGNED_TO: STRING NULLABLE - 
  - LEAD_DATE_ASSIGNED: STRING NULLABLE - 
  - LEAD_VALUE: STRING NULLABLE - 
  - LEAD_DATE_CLOSED: STRING NULLABLE - 
  - LEAD_LOST_REASON: STRING NULLABLE - 
  - LEAD_LOST_REASON_TEXT: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - ANNUAL_RECURRING_SERVICES: STRING NULLABLE - 
  - REGION_ID: STRING NULLABLE - 
  - INITIAL_INVOICE: STRING NULLABLE - 
  - INITIAL_BILLING_DATE: STRING NULLABLE - 
  - RENEWAL_FREQUENCY: STRING NULLABLE - 
  - RENEWAL_DATE: STRING NULLABLE - 
  - CUSTOM_DATE: STRING NULLABLE - 
  - SENTRICON_CONNECTED: STRING NULLABLE - 
  - SENTRICON_SITE_ID: STRING NULLABLE - 
  - SEASONAL_START: STRING NULLABLE - 
  - SEASONAL_END: STRING NULLABLE - 
  - NEXT_BILLING_DATE: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - EXPIRATION_DATE: STRING NULLABLE - 
  - LAST_APPOINTMENT: STRING NULLABLE - 
  - TEMPLATE_TYPE: STRING NULLABLE - 
  - PARENT_ID: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - PREFERRED_DAYS: STRING NULLABLE - 
  - PREFERRED_START: STRING NULLABLE - 
  - PREFERRED_END: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - AUTOPAY_PAYMENT_PROFILE_ID: STRING NULLABLE - 
  - BILLING_TERMS_DAYS: STRING NULLABLE - 
  - ON_HOLD: STRING NULLABLE - 
  - CUSTOM_SCHEDULE_ID: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - RECURRING_TICKET: STRING NULLABLE - 
  - ADD_ONS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_SUBSCRIPTION_OLD

- num_rows: 93658

- schema:
  - SUBSCRIPTION_ID: STRING NULLABLE - 
  - CUSTOMER_ID: STRING NULLABLE - 
  - BILL_TO_ACCOUNT_ID: STRING NULLABLE - 
  - OFFICE_ID: STRING NULLABLE - 
  - DATE_ADDED: STRING NULLABLE - 
  - CONTRACT_ADDED: STRING NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - ACTIVE_TEXT: STRING NULLABLE - 
  - INITIAL_QUOTE: STRING NULLABLE - 
  - INITIAL_DISCOUNT: STRING NULLABLE - 
  - INITIAL_SERVICE_TOTAL: STRING NULLABLE - 
  - YIF_DISCOUNT: STRING NULLABLE - 
  - RECURRING_CHARGE: STRING NULLABLE - 
  - CONTRACT_VALUE: STRING NULLABLE - 
  - ANNUAL_RECURRING_VALUE: STRING NULLABLE - 
  - BILLING_FREQUENCY: STRING NULLABLE - 
  - FREQUENCY: STRING NULLABLE - 
  - FOLLOWUP_SERVICE: STRING NULLABLE - 
  - AGREEMENT_LENGTH: STRING NULLABLE - 
  - NEXT_SERVICE: STRING NULLABLE - 
  - LAST_COMPLETED: STRING NULLABLE - 
  - SERVICE_ID: STRING NULLABLE - 
  - SERVICE_TYPE: STRING NULLABLE - 
  - SOLD_BY: STRING NULLABLE - 
  - SOLD_BY2: STRING NULLABLE - 
  - SOLD_BY3: STRING NULLABLE - 
  - PREFERRED_TECH: STRING NULLABLE - 
  - ADDED_BY: STRING NULLABLE - 
  - INITIAL_APPOINTMENT_ID: STRING NULLABLE - 
  - INITIAL_STATUS: STRING NULLABLE - 
  - INITIAL_STATUS_TEXT: STRING NULLABLE - 
  - DATE_CANCELLED: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CXL_NOTES: STRING NULLABLE - 
  - CANCELLED_BY: STRING NULLABLE - 
  - SUBSCRIPTION_LINK: STRING NULLABLE - 
  - PO_NUMBER: STRING NULLABLE - 
  - APPOINTMENT_IDS: STRING NULLABLE - 
  - COMPLETED_APPOINTMENT_IDS: STRING NULLABLE - 
  - LEAD_ID: STRING NULLABLE - 
  - LEAD_DATE_ADDED: STRING NULLABLE - 
  - LEAD_UPDATED: STRING NULLABLE - 
  - LEAD_ADDED_BY: STRING NULLABLE - 
  - LEAD_SOURCE_ID: STRING NULLABLE - 
  - LEAD_SOURCE: STRING NULLABLE - 
  - LEAD_STATUS: STRING NULLABLE - 
  - LEAD_STATUS_TEXT: STRING NULLABLE - 
  - LEAD_STAGE_ID: STRING NULLABLE - 
  - LEAD_STAGE: STRING NULLABLE - 
  - LEAD_ASSIGNED_TO: STRING NULLABLE - 
  - LEAD_DATE_ASSIGNED: STRING NULLABLE - 
  - LEAD_VALUE: STRING NULLABLE - 
  - LEAD_DATE_CLOSED: STRING NULLABLE - 
  - LEAD_LOST_REASON: STRING NULLABLE - 
  - LEAD_LOST_REASON_TEXT: STRING NULLABLE - 
  - SOURCE_ID: STRING NULLABLE - 
  - SOURCE: STRING NULLABLE - 
  - ANNUAL_RECURRING_SERVICES: STRING NULLABLE - 
  - REGION_ID: STRING NULLABLE - 
  - INITIAL_INVOICE: STRING NULLABLE - 
  - INITIAL_BILLING_DATE: STRING NULLABLE - 
  - RENEWAL_FREQUENCY: STRING NULLABLE - 
  - RENEWAL_DATE: STRING NULLABLE - 
  - CUSTOM_DATE: STRING NULLABLE - 
  - SENTRICON_CONNECTED: STRING NULLABLE - 
  - SENTRICON_SITE_ID: STRING NULLABLE - 
  - SEASONAL_START: STRING NULLABLE - 
  - SEASONAL_END: STRING NULLABLE - 
  - NEXT_BILLING_DATE: STRING NULLABLE - 
  - MAX_MONTHLY_CHARGE: STRING NULLABLE - 
  - EXPIRATION_DATE: STRING NULLABLE - 
  - LAST_APPOINTMENT: STRING NULLABLE - 
  - TEMPLATE_TYPE: STRING NULLABLE - 
  - PARENT_ID: STRING NULLABLE - 
  - DURATION: STRING NULLABLE - 
  - PREFERRED_DAYS: STRING NULLABLE - 
  - PREFERRED_START: STRING NULLABLE - 
  - PREFERRED_END: STRING NULLABLE - 
  - CALL_AHEAD: STRING NULLABLE - 
  - AUTOPAY_PAYMENT_PROFILE_ID: STRING NULLABLE - 
  - BILLING_TERMS_DAYS: STRING NULLABLE - 
  - ON_HOLD: STRING NULLABLE - 
  - CUSTOM_SCHEDULE_ID: STRING NULLABLE - 
  - UNIT_IDS: STRING NULLABLE - 
  - RECURRING_TICKET: STRING NULLABLE - 
  - ADD_ONS: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 

## pco-qa.raw_layer.FR_TICKET

- num_rows: 4542976

- schema:
  - TICKET_ID: STRING NULLABLE - 
  - PRODUCTION_VALUE: STRING NULLABLE - 
  - SUB_TOTAL: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - TAX_AMOUNT: STRING NULLABLE - 
  - TOTAL: STRING NULLABLE - 
  - SERVICE_CHARGE: STRING NULLABLE - 
  - SERVICE_TAXABLE: STRING NULLABLE - 
  - BALANCE: STRING NULLABLE - 

## pco-qa.raw_layer.FR_TICKET_OLD

- num_rows: 71353

- schema:
  - TICKET_ID: STRING NULLABLE - 
  - PRODUCTION_VALUE: STRING NULLABLE - 
  - SUB_TOTAL: STRING NULLABLE - 
  - DATE_UPDATED: STRING NULLABLE - 
  - CLIENT: STRING NULLABLE - 
  - DATA_SOURCE: STRING NULLABLE - 
  - DATE_LOADED: TIMESTAMP NULLABLE - 
  - ACTIVE: STRING NULLABLE - 
  - TAX_AMOUNT: STRING NULLABLE - 
  - TOTAL: STRING NULLABLE - 
  - SERVICE_CHARGE: STRING NULLABLE - 
  - SERVICE_TAXABLE: STRING NULLABLE - 
  - BALANCE: STRING NULLABLE - 

## pco-qa.raw_layer.Input_PricePush

- num_rows: 0

- schema:
  - clientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldServiceCharge: FLOAT NULLABLE - 
  - oldProductionValue: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newServiceCharge: FLOAT NULLABLE - 
  - newProductionValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - recordCreatedAt: STRING NULLABLE - 
  - dataSource_date: DATE NULLABLE - %E4Y-%m-%d
  - isNotified: BOOLEAN NULLABLE - 

## pco-qa.raw_layer.lkp_cogs_hours

- num_rows: 0

- schema:
  - Year: INTEGER NULLABLE - 
  - Month: INTEGER NULLABLE - 
  - BoM: DATE NULLABLE - %m/%d/%E4Y
  - EOM: DATE NULLABLE - %m/%d/%E4Y
  - Data_Available: BOOLEAN NULLABLE - 
  - Revenue: INTEGER NULLABLE - 
  - COGS: INTEGER NULLABLE - 
  - Labor_Hours: INTEGER NULLABLE - 
  - clientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_cohort_counter

- num_rows: 0

- schema:
  - cohortCreationDate: DATE NULLABLE - %m/%d/%E4Y
  - cohortRandSize: INTEGER NULLABLE - 
  - ClientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_company_level_target_inputs

- num_rows: 0

- schema:
  - companyLevelTargetInputs: STRING NULLABLE - 
  - value: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_customer_category

- num_rows: 0

- schema:
  - customerCategory: STRING NULLABLE - 
  - priceIncrease: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_customer_level_categorization_inputs

- num_rows: 0

- schema:
  - customerLevelCategorizationInputs: STRING NULLABLE - 
  - isSubsidized: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - floor: STRING NULLABLE - 
  - ceiling: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_eligibility_criteria

- num_rows: 0

- schema:
  - eligibilityCriteria: STRING NULLABLE - 
  - value: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_make_floors

- num_rows: 11

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - isSubsidized: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - floor: FLOAT NULLABLE - 
  - TTMCogs: FLOAT NULLABLE - 
  - CogsPlusInflation: FLOAT NULLABLE - 
  - floorValue: FLOAT NULLABLE - 
  - check: BOOLEAN NULLABLE - 

## pco-qa.raw_layer.lkp_marketing_benefit

- num_rows: 0

- schema:
  - marketingBenefit: STRING NULLABLE - 
  - value: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_master_account_map

- num_rows: 0

- schema:
  - customerID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_never_push_increase

- num_rows: 0

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - level: STRING NULLABLE - 
  - rowID: INTEGER NULLABLE - 
  - dateEntered: DATE NULLABLE - %m/%d/%E4Y
  - Notes: STRING NULLABLE - 
  - requiresNotice: BOOLEAN NULLABLE - 

## pco-qa.raw_layer.lkp_performer_product

- num_rows: 0

- schema:
  - performerProduct: STRING NULLABLE - 
  - priceIncrease: INTEGER NULLABLE - 
  - priceIncreasePercentage: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_price_strategy_by_category

- num_rows: 0

- schema:
  - customerCategory: STRING NULLABLE - 
  - cohortCreationDate: DATE NULLABLE - %m/%d/%E4Y
  - priceIncrease: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_price_strategy_by_product

- num_rows: 0

- schema:
  - performerProduct: STRING NULLABLE - 
  - cohortCreationDate: DATE NULLABLE - %m/%d/%E4Y
  - priceIncrease: INTEGER NULLABLE - 
  - priceIncreasePercentage: FLOAT NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_re_occuring_production_settings

- num_rows: 0

- schema:
  - reoccuringProductionSettings: STRING NULLABLE - 
  - value: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_report_type

- num_rows: 0

- schema:
  - reportType: STRING NULLABLE - 
  - value: STRING NULLABLE - 
  - clientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_sales_mapping_recurring

- num_rows: 0

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - glNumber: STRING NULLABLE - 
  - salesMappingCode: STRING NULLABLE - 
  - isRecurring: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_service_type

- num_rows: 0

- schema:
  - serviceType: INTEGER NULLABLE - 
  - serviceTypeName: STRING NULLABLE - 
  - majorRecurringBucket: STRING NULLABLE - 
  - minorRecurringBucket: STRING NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - allocateReservices: BOOLEAN NULLABLE - 
  - isRervice: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - visitOverride: FLOAT NULLABLE - 
  - expiredCode: BOOLEAN NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.lkp_time_assumption

- num_rows: 0

- schema:
  - timeAssumption: STRING NULLABLE - 
  - value: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 

## pco-qa.raw_layer.raw_bill_to_account

- num_rows: 12072

- schema:
  - customerID: STRING NULLABLE - Unique identifier for the customer
  - lastName: STRING NULLABLE - Last name of the customer
  - firstName: STRING NULLABLE - First name of the customer
  - billToCustomerID: STRING NULLABLE - Customer ID to which the bill is sent

## pco-qa.test_layer.A1forAdi

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.accel_appointments

- num_rows: 61039

- schema:
  - OfficeName: STRING NULLABLE - 
  - TechID: INTEGER NULLABLE - 
  - TechName: STRING NULLABLE - 
  - CustomerID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - AppointmentDate: DATE NULLABLE - 
  - TimeIn: STRING NULLABLE - 
  - TimeOut: STRING NULLABLE - 
  - ServiceType: STRING NULLABLE - 
  - AppointmentDuration: INTEGER NULLABLE - 
  - ReportedTimeOnSite: FLOAT NULLABLE - 
  - ReportedDifference: FLOAT NULLABLE - 
  - ActualTimeOnSite: FLOAT NULLABLE - 
  - ActualDifference: FLOAT NULLABLE - 
  - ServiceNotes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.accel_customer

- num_rows: 47407

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 

## pco-qa.test_layer.accel_subscription

- num_rows: 75135

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - SubscriptionID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 

## pco-qa.test_layer.cohort_confirmation_list

- num_rows: 1994

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - eligible: STRING NULLABLE - 
  - firstSubsDate: DATE NULLABLE - 
  - fullName: STRING NULLABLE - 
  - oldARR: FLOAT NULLABLE - 
  - sinceStart: FLOAT NULLABLE - 
  - sinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.customer_forecast

- num_rows: 6582

- schema:
  - masterCustomerID: INTEGER NULLABLE - 
  - Eligible: BOOLEAN NULLABLE - 
  - TreatasCommercial: BOOLEAN NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - RevgreaterthanNormal: FLOAT NULLABLE - 
  - MarketingBenefit: FLOAT NULLABLE - 
  - ReoccuringProductionForecast: FLOAT NULLABLE - 
  - ForecastNTMValue: FLOAT NULLABLE - 
  - PaidVisits: INTEGER NULLABLE - 
  - UnpaidVisits: FLOAT NULLABLE - 
  - OnsiteMinPaidVisit: FLOAT NULLABLE - 
  - OnsiteMinUnpaidVisit: INTEGER NULLABLE - 
  - PaidOnsiteMinutes: FLOAT NULLABLE - 
  - UnpaidOnsiteMinutes: FLOAT NULLABLE - 
  - TotalOnSiteMin: FLOAT NULLABLE - 
  - TotalOnSiteHours: FLOAT NULLABLE - 
  - DriveMinPaidVisit: FLOAT NULLABLE - 
  - DriveMinUnpaidVisit: FLOAT NULLABLE - 
  - TotalDriveTime: FLOAT NULLABLE - 
  - ConstrainedScheduleExtraMinutes: INTEGER NULLABLE - 
  - FreqRescheduleExtraMin: FLOAT NULLABLE - 
  - TotalSchedulingMinutes: FLOAT NULLABLE - 
  - BalanceAllocationperVisitMin: FLOAT NULLABLE - 
  - TotalBalanceAllocationMin: FLOAT NULLABLE - 
  - LaborHoursForecast: FLOAT NULLABLE - 
  - NTMOnSitePH: STRING NULLABLE - 
  - RankPercentileOnSite: STRING NULLABLE - 
  - NTMTotalPH: STRING NULLABLE - 
  - RankPercentile: STRING NULLABLE - 
  - PercentileSpread: STRING NULLABLE - 
  - Rank: STRING NULLABLE - 
  - Check: STRING NULLABLE - 
  - Check_1: FLOAT NULLABLE - 

## pco-qa.test_layer.customer_history

- num_rows: 6582

- schema:
  - masterCustomerID: INTEGER NULLABLE - 
  - EarliestContractDate: STRING NULLABLE - 
  - EarliestReserviceableContractDate: STRING NULLABLE - 
  - EarliestDateCountReservice: TIMESTAMP NULLABLE - 
  - LatestProductAddition: STRING NULLABLE - 
  - TenureSinceStart: FLOAT NULLABLE - 
  - TenuresinceProductChange: FLOAT NULLABLE - 
  - LastDateCustomerIncludeContracts: STRING NULLABLE - 
  - Active: INTEGER NULLABLE - 
  - Total Production: INTEGER NULLABLE - 
  - IncludedServiceProd: INTEGER NULLABLE - 
  - IncludedProductionInitialexcluded: INTEGER NULLABLE - 
  - OneTimeProduction: INTEGER NULLABLE - 
  - RecurringMinutes: FLOAT NULLABLE - 
  - OneTimeMinutes: FLOAT NULLABLE - 
  - TotalVisits: INTEGER NULLABLE - 
  - ReserviceVisits: INTEGER NULLABLE - 
  - ReserviceableVisits: INTEGER NULLABLE - 
  - ReserviceVisitsPercentage: STRING NULLABLE - 
  - IncludedRecurringVisits: INTEGER NULLABLE - 
  - RescheduledVisits: INTEGER NULLABLE - 
  - RescheduledVisitPercentage: STRING NULLABLE - 
  - includedVisitsGreaterThan30: INTEGER NULLABLE - 
  - includedVisitsGreaterThan45: INTEGER NULLABLE - 
  - AvgMinutesperRecurringVisit: STRING NULLABLE - 
  - ExcludeNotEnoughVisits: BOOLEAN NULLABLE - 
  - ExcludeNotEnoughTenure: BOOLEAN NULLABLE - 
  - ExcludeNoActiveIncludedContracts: BOOLEAN NULLABLE - 
  - ExcludeRecentProductChange: BOOLEAN NULLABLE - 
  - ExcludeFriendsFamily: BOOLEAN NULLABLE - 
  - InCohortLastTwelveMonths: INTEGER NULLABLE - 
  - ManualExclusion: INTEGER NULLABLE - 
  - AnyExclusion: BOOLEAN NULLABLE - 
  - PermExclusions: BOOLEAN NULLABLE - 
  - PermExclusionCount: INTEGER NULLABLE - 
  - FreqReservice: STRING NULLABLE - 
  - minutesBetween30And45: STRING NULLABLE - 
  - minVisitsGreaterThan45: STRING NULLABLE - 
  - freqreschedule: STRING NULLABLE - 
  - constrainedschedule: STRING NULLABLE - 

## pco-qa.test_layer.customer_location

- num_rows: 0

- schema:
  - groupID: INTEGER NULLABLE - 
  - Name: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - Labor_Hours_2: FLOAT NULLABLE - 
  - __per_Hour: FLOAT NULLABLE - 
  - Category: STRING NULLABLE - 
  - Increase__: STRING NULLABLE - 
  - Planned_Visits: FLOAT NULLABLE - 
  - Unplanned_Visits: FLOAT NULLABLE - 
  - Overlap_Visits: FLOAT NULLABLE - 
  - OnSite_Hours: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - Tenure: FLOAT NULLABLE - 
  - Included_Subscriptions: FLOAT NULLABLE - 
  - Subscriptions_Old_Enough: FLOAT NULLABLE - 
  - InReview_From_15th: BOOLEAN NULLABLE - 
  - No_Increase_Flag: BOOLEAN NULLABLE - 
  - InReview_After_15th: BOOLEAN NULLABLE - 
  - Sign_Up_Anniversary: FLOAT NULLABLE - 
  - Random__: FLOAT NULLABLE - 
  - inReview_Random__: FLOAT NULLABLE - 
  - InReview_Rank: FLOAT NULLABLE - 
  - InCohort: BOOLEAN NULLABLE - 
  - dummy1: STRING NULLABLE - 
  - After_Deductible: FLOAT NULLABLE - 
  - Benefit: FLOAT NULLABLE - 
  - Eff____Hour: FLOAT NULLABLE - 
  - dummy: STRING NULLABLE - 
  - Active_Subs: FLOAT NULLABLE - 
  - zeroVisitTime: FLOAT NULLABLE - 
  - Non_Recurring: FLOAT NULLABLE - 
  - NULL_Appointments: FLOAT NULLABLE - 
  - ntmPlannedVisits_0: FLOAT NULLABLE - 
  - Hours_but_no_rev: FLOAT NULLABLE - 
  - Rev_but_no_hours: FLOAT NULLABLE - 
  - All_Subs_Old_Enough: BOOLEAN NULLABLE - 
  - Hours_without_Revenue: BOOLEAN NULLABLE - 
  - Revenue_without_Hours: BOOLEAN NULLABLE - 

## pco-qa.test_layer.delete_experiment

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.gc_cl_qa

- num_rows: 0

- schema:
  - groupID: INTEGER NULLABLE - 
  - Name: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - Labor_Hours2: FLOAT NULLABLE - 
  - __per_Hour: FLOAT NULLABLE - 
  - Category: STRING NULLABLE - 
  - Planned_Visits: FLOAT NULLABLE - 
  - Unplanned_Visits: FLOAT NULLABLE - 
  - Overlap_Visits: FLOAT NULLABLE - 
  - OnSite_Hours: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - Tenure: FLOAT NULLABLE - 
  - Included_Subscriptions: FLOAT NULLABLE - 
  - Subscriptions_Old_Enough: FLOAT NULLABLE - 
  - InReview: BOOLEAN NULLABLE - 
  - dummy: STRING NULLABLE - 
  - After_Deductible: FLOAT NULLABLE - 
  - Benefit: FLOAT NULLABLE - 
  - Eff____Hour: FLOAT NULLABLE - 
  - dummy2: STRING NULLABLE - 
  - Active_Subs: FLOAT NULLABLE - 
  - zeroVisitTime: FLOAT NULLABLE - 
  - Non_Recurring: FLOAT NULLABLE - 
  - NULL_Appointments: FLOAT NULLABLE - 
  - ntmPlannedVisits_0: FLOAT NULLABLE - 
  - Hours_but_no_rev: FLOAT NULLABLE - 
  - Rev_but_no_hours: FLOAT NULLABLE - 
  - All_Subs_Old_Enough: BOOLEAN NULLABLE - 
  - Hours_without_Revenue: BOOLEAN NULLABLE - 
  - Revenue_without_Hours: BOOLEAN NULLABLE - 

## pco-qa.test_layer.gc_lc_qa

- num_rows: 0

- schema:
  - customID: INTEGER NULLABLE - 
  - groupID: STRING NULLABLE - 
  - Name: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - Labor_Hours_2: FLOAT NULLABLE - 
  - __per_Hour: FLOAT NULLABLE - 
  - Planned_Visits: FLOAT NULLABLE - 
  - Unplanned_Visits: FLOAT NULLABLE - 
  - Overlap_Visits: FLOAT NULLABLE - 
  - OnSite_Hours: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - Tenure: FLOAT NULLABLE - 
  - Included_Recurring_Subscriptions: FLOAT NULLABLE - 
  - Old_Enough: FLOAT NULLABLE - 
  - DUMMY: STRING NULLABLE - 
  - Active_Subs: FLOAT NULLABLE - 
  - zeroVisitTime: FLOAT NULLABLE - 
  - Non_Recurring: FLOAT NULLABLE - 
  - NULL_Appointments: FLOAT NULLABLE - 
  - ntmPlannedVisits_0: FLOAT NULLABLE - 
  - Hours_but_no_rev: FLOAT NULLABLE - 
  - Rev_but_no_hours: FLOAT NULLABLE - 
  - Hours_without_Revenue: BOOLEAN NULLABLE - 
  - Revenue_without_Hours: BOOLEAN NULLABLE - 

## pco-qa.test_layer.gc_sd_qa

- num_rows: 0

- schema:
  - Name: STRING NULLABLE - 
  - Service_Type: STRING NULLABLE - 
  - All_Subscription_Revenue: FLOAT NULLABLE - 
  - Planned_Visits: INTEGER NULLABLE - 
  - Avg__Visit_Min: STRING NULLABLE - 
  - Customer_Reservice_Rate: FLOAT NULLABLE - 
  - DUMMY: STRING NULLABLE - 
  - DUMMY1: STRING NULLABLE - 
  - DUMMY2: STRING NULLABLE - 
  - DUMMY3: STRING NULLABLE - 
  - DUMMY4: STRING NULLABLE - 
  - DUMMY5: STRING NULLABLE - 
  - SID: INTEGER NULLABLE - 
  - CID: INTEGER NULLABLE - 
  - GID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - ntmPlannedVistis: INTEGER NULLABLE - 
  - dateSubOldEnough: FLOAT NULLABLE - 
  - subOldEnough: BOOLEAN NULLABLE - 
  - NULLAppointment_History: BOOLEAN NULLABLE - 
  - zeroVisitTime_but_Minutes: BOOLEAN NULLABLE - 
  - ARS_0: BOOLEAN NULLABLE - 
  - ARS_24: BOOLEAN NULLABLE - 
  - Hours_but_no_rev: BOOLEAN NULLABLE - 
  - Rev_but_no_hours: BOOLEAN NULLABLE - 
  - KeepIn: BOOLEAN NULLABLE - 
  - forecastUnplanned_Visits: FLOAT NULLABLE - 
  - Minutes_per_Unplanned_Visit: INTEGER NULLABLE - 
  - unplannedVisitMinOnSite: FLOAT NULLABLE - 
  - plannedVisitMinOnSite: STRING NULLABLE - 
  - onSiteMin: STRING NULLABLE - 
  - visitSize: STRING NULLABLE - 
  - CustomerNum3_S: INTEGER NULLABLE - 
  - First_3__: BOOLEAN NULLABLE - 
  - _3__Overlap_Level: FLOAT NULLABLE - 
  - Under_3_Overlap_Level: STRING NULLABLE - 
  - Overlap: FLOAT NULLABLE - 
  - Overlap_Visits: INTEGER NULLABLE - 
  - DUMMY6: STRING NULLABLE - 
  - avgdriveTimePlanned: INTEGER NULLABLE - 
  - avgdriveTimeUnplanned: INTEGER NULLABLE - 
  - Total_PlannedDriveTime: INTEGER NULLABLE - 
  - Total_Unplanned_Drive_Time: FLOAT NULLABLE - 
  - Total_Drive_TIme: FLOAT NULLABLE - 
  - BalanceTime: INTEGER NULLABLE - 
  - Total_Min: STRING NULLABLE - 
  - isActive: INTEGER NULLABLE - 

## pco-qa.test_layer.gui_accel_app

- num_rows: 123257

- schema:
  - Office Name: STRING NULLABLE - 
  - Tech ID: INTEGER NULLABLE - 
  - Tech Name: STRING NULLABLE - 
  - Customer ID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - Appointment Date: DATE NULLABLE - 
  - Time In: STRING NULLABLE - 
  - Time Out: STRING NULLABLE - 
  - Service Type: STRING NULLABLE - 
  - Appointment Duration: INTEGER NULLABLE - 
  - Reported Time On Site: FLOAT NULLABLE - 
  - Reported Difference: FLOAT NULLABLE - 
  - Actual Time On Site: FLOAT NULLABLE - 
  - Actual Difference: FLOAT NULLABLE - 
  - Service Notes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.gui_accel_appointments

- num_rows: 164857

- schema:
  - OfficeName: STRING NULLABLE - 
  - TechID: INTEGER NULLABLE - 
  - TechName: STRING NULLABLE - 
  - CustomerID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - AppointmentDate: DATE NULLABLE - 
  - TimeIn: STRING NULLABLE - 
  - TimeOut: STRING NULLABLE - 
  - ServiceType: STRING NULLABLE - 
  - AppointmentDuration: INTEGER NULLABLE - 
  - ReportedTimeOnSite: FLOAT NULLABLE - 
  - ReportedDifference: FLOAT NULLABLE - 
  - ActualTimeOnSite: FLOAT NULLABLE - 
  - ActualDifference: FLOAT NULLABLE - 
  - ServiceNotes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 

## pco-qa.test_layer.gui_accel_cus

- num_rows: 70605

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Customer Date Canceled: STRING NULLABLE - 
  - Customer Status: STRING NULLABLE - 

## pco-qa.test_layer.gui_accel_customer

- num_rows: 12838

- schema:
  - CustomerID: INTEGER NULLABLE - 

## pco-qa.test_layer.gui_accel_sub

- num_rows: 103935

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Subscription ID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Annual Recurring Value: FLOAT NULLABLE - 
  - Subscription Date Canceled: STRING NULLABLE - 
  - Subscription Status: STRING NULLABLE - 
  - Office Name: STRING NULLABLE - 

## pco-qa.test_layer.gui_accel_subscription

- num_rows: 19411

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - SubscriptionID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 
  - AnnualRecurringValue: FLOAT NULLABLE - 
  - RecurringTotal: FLOAT NULLABLE - 
  - BillingFrequency: STRING NULLABLE - 
  - SubscriptionStatus: STRING NULLABLE - 
  - InitialService: STRING NULLABLE - 
  - SubscriptionLastCompleted: STRING NULLABLE - 
  - OfficeName: STRING NULLABLE - 
  - SubscriptionContractValue: FLOAT NULLABLE - 
  - RecurringFrequency: STRING NULLABLE - 
  - RecurringPrice: STRING NULLABLE - 

## pco-qa.test_layer.gui_greencountry_appointment

- num_rows: 14707

- schema:
  - Office Name: STRING NULLABLE - 
  - Tech ID: INTEGER NULLABLE - 
  - Tech Name: STRING NULLABLE - 
  - Customer ID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - Appointment Date: DATE NULLABLE - 
  - Time In: STRING NULLABLE - 
  - Time Out: STRING NULLABLE - 
  - Service Type: STRING NULLABLE - 
  - Appointment Duration: INTEGER NULLABLE - 
  - Reported Time On Site: FLOAT NULLABLE - 
  - Reported Difference: FLOAT NULLABLE - 
  - Actual Time On Site: FLOAT NULLABLE - 
  - Actual Difference: FLOAT NULLABLE - 
  - Service Notes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.gui_greencountry_customers

- num_rows: 2820

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 
  - CustomerAdded: STRING NULLABLE - 
  - AccountType: STRING NULLABLE - 
  - CustomerDateCanceled: STRING NULLABLE - 
  - CustomerStatus: STRING NULLABLE - 
  - BillToCustomerID: INTEGER NULLABLE - 

## pco-qa.test_layer.gui_greencountry_subscription

- num_rows: 15358

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - SubscriptionID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 
  - AnnualRecurringValue: FLOAT NULLABLE - 
  - DateSigned: STRING NULLABLE - 
  - SubscriptionDateCanceled: STRING NULLABLE - 
  - SubscriptionStatus: STRING NULLABLE - 
  - SpecialScheduling: STRING NULLABLE - 
  - RecurringFrequency: STRING NULLABLE - 
  - RecurringPrice: FLOAT NULLABLE - 
  - BillingFrequency: STRING NULLABLE - 
  - string_field_13: STRING NULLABLE - 
  - string_field_14: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 

## pco-qa.test_layer.gui_pisgah_app

- num_rows: 65535

- schema:
  - Office Name: STRING NULLABLE - 
  - Tech ID: INTEGER NULLABLE - 
  - Tech Name: STRING NULLABLE - 
  - Customer ID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - Appointment Date: DATE NULLABLE - 
  - Time In: STRING NULLABLE - 
  - Time Out: STRING NULLABLE - 
  - Service Type: STRING NULLABLE - 
  - Appointment Duration: INTEGER NULLABLE - 
  - Reported Time On Site: FLOAT NULLABLE - 
  - Reported Difference: FLOAT NULLABLE - 
  - Actual Time On Site: FLOAT NULLABLE - 
  - Actual Difference: FLOAT NULLABLE - 
  - Service Notes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.gui_pisgah_cus

- num_rows: 22389

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Customer Date Canceled: STRING NULLABLE - 
  - Customer Status: STRING NULLABLE - 

## pco-qa.test_layer.gui_pisgah_sub

- num_rows: 49576

- schema:
  - Subscription ID: INTEGER NULLABLE - 

## pco-qa.test_layer.gui_profext_app

- num_rows: 23291

- schema:
  - Office Name: STRING NULLABLE - 
  - Tech ID: INTEGER NULLABLE - 
  - Tech Name: STRING NULLABLE - 
  - Customer ID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - Appointment Date: DATE NULLABLE - 
  - Time In: STRING NULLABLE - 
  - Time Out: STRING NULLABLE - 
  - Service Type: STRING NULLABLE - 
  - Appointment Duration: INTEGER NULLABLE - 
  - Reported Time On Site: FLOAT NULLABLE - 
  - Reported Difference: FLOAT NULLABLE - 
  - Actual Time On Site: FLOAT NULLABLE - 
  - Actual Difference: FLOAT NULLABLE - 
  - Service Notes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.gui_profext_cus

- num_rows: 17993

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Customer Date Canceled: STRING NULLABLE - 
  - Customer Status: STRING NULLABLE - 

## pco-qa.test_layer.gui_profext_sub

- num_rows: 7753

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Subscription ID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Annual Recurring Value: FLOAT NULLABLE - 
  - Recurring Total: STRING NULLABLE - 
  - Billing Frequency: STRING NULLABLE - 
  - Subscription Status: STRING NULLABLE - 
  - Recurring Frequency: STRING NULLABLE - 

## pco-qa.test_layer.gui_stb_appointment

- num_rows: 30286

- schema:
  - OfficeName: STRING NULLABLE - 
  - TechID: INTEGER NULLABLE - 
  - TechName: STRING NULLABLE - 
  - CustomerID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - AppointmentDate: DATE NULLABLE - 
  - TimeIn: STRING NULLABLE - 
  - TimeOut: STRING NULLABLE - 
  - ServiceType: STRING NULLABLE - 
  - AppointmentDuration: INTEGER NULLABLE - 
  - ReportedTimeOnSite: FLOAT NULLABLE - 
  - ReportedDifference: FLOAT NULLABLE - 
  - ActualTimeOnSite: FLOAT NULLABLE - 
  - ActualDifference: FLOAT NULLABLE - 
  - ServiceNotes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.gui_stb_customer

- num_rows: 12430

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 
  - CustomerAdded: STRING NULLABLE - 
  - AccountType: STRING NULLABLE - 
  - CustomerDateCanceled: STRING NULLABLE - 
  - CustomerStatus: STRING NULLABLE - 
  - int64_field_7: INTEGER NULLABLE - 

## pco-qa.test_layer.gui_stb_subscription

- num_rows: 16480

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - SubscriptionID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 
  - AnnualRecurringValue: FLOAT NULLABLE - 
  - RecurringTotal: FLOAT NULLABLE - 
  - BillingFrequency: STRING NULLABLE - 
  - DateSigned: STRING NULLABLE - 
  - SubscriptionDateCanceled: STRING NULLABLE - 
  - SubscriptionStatus: STRING NULLABLE - 
  - SpecialScheduling: STRING NULLABLE - 
  - RecurringFrequency: STRING NULLABLE - 

## pco-qa.test_layer.gui_trad_app

- num_rows: 96989

- schema:
  - Office Name: STRING NULLABLE - 
  - Tech ID: INTEGER NULLABLE - 
  - Tech Name: STRING NULLABLE - 
  - Customer ID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - Appointment Date: DATE NULLABLE - 
  - Time In: STRING NULLABLE - 
  - Time Out: STRING NULLABLE - 
  - Service Type: STRING NULLABLE - 
  - Appointment Duration: INTEGER NULLABLE - 
  - Reported Time On Site: FLOAT NULLABLE - 
  - Reported Difference: FLOAT NULLABLE - 
  - Actual Time On Site: FLOAT NULLABLE - 
  - Actual Difference: FLOAT NULLABLE - 
  - Service Notes: STRING NULLABLE - 

## pco-qa.test_layer.gui_trad_cus

- num_rows: 42266

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Customer Date Canceled: STRING NULLABLE - 
  - Customer Status: STRING NULLABLE - 
  - Office Name: STRING NULLABLE - 

## pco-qa.test_layer.gui_trad_sub

- num_rows: 79831

- schema:
  - Customer ID: INTEGER NULLABLE - 
  - Subscription ID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - Last Name: STRING NULLABLE - 
  - First Name: STRING NULLABLE - 
  - Annual Recurring Value: FLOAT NULLABLE - 
  - Subscription Date Canceled: STRING NULLABLE - 
  - Subscription Status: STRING NULLABLE - 
  - Office Name: STRING NULLABLE - 

## pco-qa.test_layer.live_quell_a4

- num_rows: 0

- schema:
  - fi: INTEGER NULLABLE - 
  - subsciptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - Planned_Increase: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: STRING NULLABLE - 
  - newAnnualRecurringValue: STRING NULLABLE - 
  - oldCharge: STRING NULLABLE - 
  - NewCharge: STRING NULLABLE - 

## pco-qa.test_layer.local_ID

- num_rows: 75446

- schema:
  - appointmentID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 

## pco-qa.test_layer.location_details

- num_rows: 0

- schema:
  - customID: INTEGER NULLABLE - 
  - groupID: STRING NULLABLE - 
  - Name: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - Labor_Hours_2: FLOAT NULLABLE - 
  - __per_Hour: FLOAT NULLABLE - 
  - Planned_Visits: FLOAT NULLABLE - 
  - Unplanned_Visits: FLOAT NULLABLE - 
  - Overlap_Visits: FLOAT NULLABLE - 
  - OnSite_Hours: FLOAT NULLABLE - 
  - Labor_Hours: FLOAT NULLABLE - 
  - Tenure: FLOAT NULLABLE - 
  - Included_Recurring_Subscriptions: FLOAT NULLABLE - 
  - Old_Enough: FLOAT NULLABLE - 
  - dummy3: STRING NULLABLE - 
  - Active_Subs: FLOAT NULLABLE - 
  - zeroVisitTime: FLOAT NULLABLE - 
  - Non_Recurring: FLOAT NULLABLE - 
  - NULL_Appointments: FLOAT NULLABLE - 
  - ntmPlannedVisits_0: FLOAT NULLABLE - 
  - Hours_but_no_rev: FLOAT NULLABLE - 
  - Rev_but_no_hours: FLOAT NULLABLE - 
  - Hours_without_Revenue: BOOLEAN NULLABLE - 
  - Revenue_without_Hours: BOOLEAN NULLABLE - 
  - No_Increase_Flag: BOOLEAN NULLABLE - 
  - Discounted_Increase_Flag: BOOLEAN NULLABLE - 
  - No_or_discounted_increase_flag: BOOLEAN NULLABLE - 

## pco-qa.test_layer.output_list_advanced

- num_rows: 1994

- schema:
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - totalForecastNTMValue: FLOAT NULLABLE - 
  - forecastHours: FLOAT NULLABLE - 
  - forecastDollarProductionPerHour: FLOAT NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - eligible: STRING NULLABLE - 
  - increasePercentage: STRING NULLABLE - 
  - dollarSubsidized: FLOAT NULLABLE - 
  - recommendContractReview: BOOLEAN NULLABLE - 
  - fullName: STRING NULLABLE - 

## pco-qa.test_layer.price_pushup

- num_rows: 2387

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: INTEGER NULLABLE - 
  - newCharge: FLOAT NULLABLE - 
  - eligible: STRING NULLABLE - 

## pco-qa.test_layer.pricepush_ForExecution

- num_rows: 674

- schema:
  - clientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldServiceCharge: FLOAT NULLABLE - 
  - oldProductionValue: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newServiceCharge: FLOAT NULLABLE - 
  - newProductionValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - recordCreatedAt: STRING NULLABLE - 
  - dataSource_date: DATE NULLABLE - 
  - isNotified: BOOLEAN NULLABLE - 
  - raw_ticketID: STRING NULLABLE - 
  - raw_client: STRING NULLABLE - 
  - raw_crm_source: STRING NULLABLE - 

## pco-qa.test_layer.qa_test_a1

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.qa_test_price_pushup

- num_rows: 0

- schema:
  - subsciptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - Planned_Increase: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: INTEGER NULLABLE - 
  - NewCharge: FLOAT NULLABLE - 

## pco-qa.test_layer.raw_customer

- num_rows: 11905

- schema:
  - customerID: INTEGER NULLABLE - 
  - fname: STRING NULLABLE - 
  - lname: STRING NULLABLE - 
  - status: INTEGER NULLABLE - 
  - statusText: STRING NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - commercialAccount: INTEGER NULLABLE - 
  - BilltoAccount: STRING NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - string_field_9: STRING NULLABLE - 
  - string_field_10: STRING NULLABLE - 
  - string_field_11: STRING NULLABLE - 

## pco-qa.test_layer.sub_details_accel

- num_rows: 0

- schema:
  - Name: STRING NULLABLE - 
  - Service_Type: STRING NULLABLE - 
  - All_Subscription_Revenue: INTEGER NULLABLE - 
  - Planned_Visits: FLOAT NULLABLE - 
  - Avg__Visit_Min: STRING NULLABLE - 
  - Customer_Reservice_Rate: FLOAT NULLABLE - 
  - dummy1: STRING NULLABLE - 
  - dummy2: STRING NULLABLE - 
  - dummy3: STRING NULLABLE - 
  - dummy4: STRING NULLABLE - 
  - dummy5: STRING NULLABLE - 
  - yearlyVisitOverride: FLOAT NULLABLE - 
  - SID: INTEGER NULLABLE - 
  - CID: INTEGER NULLABLE - 
  - GID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - isCommercial: BOOLEAN NULLABLE - 
  - reserviceRatio: FLOAT NULLABLE - 
  - avgSiteMinsCustandSub: FLOAT NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - ntmPlannedVistis: FLOAT NULLABLE - 
  - dateSubOldEnough: DATE NULLABLE - %m/%d/%E4Y
  - subOldEnough: BOOLEAN NULLABLE - 
  - NULLAppointment_History: BOOLEAN NULLABLE - 
  - zeroVisitTime_but_Minutes: BOOLEAN NULLABLE - 
  - ARS_0: BOOLEAN NULLABLE - 
  - ARS_24: BOOLEAN NULLABLE - 
  - Hours_but_no_rev: BOOLEAN NULLABLE - 
  - Rev_but_no_hours: BOOLEAN NULLABLE - 
  - KeepIn: BOOLEAN NULLABLE - 
  - forecastUnplanned_Visits: FLOAT NULLABLE - 
  - Minutes_per_Unplanned_Visit: INTEGER NULLABLE - 
  - unplannedVisitMinOnSite: FLOAT NULLABLE - 
  - plannedVisitMinOnSite: STRING NULLABLE - 
  - onSiteMin: STRING NULLABLE - 
  - visitSize: STRING NULLABLE - 
  - CustomerNum3_S: INTEGER NULLABLE - 
  - First_3__: BOOLEAN NULLABLE - 
  - _3__Overlap_Level: FLOAT NULLABLE - 
  - Under_3_Overlap_Level: STRING NULLABLE - 
  - Overlap: FLOAT NULLABLE - 
  - Overlap_Visits: INTEGER NULLABLE - 
  - Dummy: STRING NULLABLE - 
  - avgdriveTimePlanned: INTEGER NULLABLE - 
  - avgdriveTimeUnplanned: INTEGER NULLABLE - 
  - Total_PlannedDriveTime: FLOAT NULLABLE - 
  - Total_Unplanned_Drive_Time: FLOAT NULLABLE - 
  - Total_Drive_TIme: FLOAT NULLABLE - 
  - BalanceTime: INTEGER NULLABLE - 
  - Total_Min: STRING NULLABLE - 
  - isActive: INTEGER NULLABLE - 

## pco-qa.test_layer.subscription_level_price_up

- num_rows: 9179

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - Active: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - MajorBucket: STRING NULLABLE - 
  - MinorBucket: STRING NULLABLE - 
  - IncreaseifPerformer: FLOAT NULLABLE - 
  - Category: STRING NULLABLE - 
  - IsPerformer: BOOLEAN NULLABLE - 
  - PlannedIncrease: STRING NULLABLE - 
  - newAnnualRecurringValue: STRING NULLABLE - 
  - NewCharge: STRING NULLABLE - 
  - In Cohort: BOOLEAN NULLABLE - 
  - CohortCounter: INTEGER NULLABLE - 
  - Increase: STRING NULLABLE - 

## pco-qa.test_layer.t_appointment

- num_rows: 32200

- schema:
  - appointmentID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - appointmentDate: TIMESTAMP NULLABLE - 
  - duration: INTEGER NULLABLE - 
  - type: INTEGER NULLABLE - 
  - status: INTEGER NULLABLE - 
  - isInitial: INTEGER NULLABLE - 
  - servicedBy: INTEGER NULLABLE - 
  - timeIn: STRING NULLABLE - 
  - timeOut: STRING NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - productionValue: INTEGER NULLABLE - 
  - ProdFromTicket: INTEGER NULLABLE - 
  - serviceTypeText: STRING NULLABLE - 
  - isReservice: BOOLEAN NULLABLE - 
  - IncludedType: BOOLEAN NULLABLE - 
  - CountReservice: BOOLEAN NULLABLE - 
  - CommercialClient: BOOLEAN NULLABLE - 
  - MultiVisit: BOOLEAN NULLABLE - 
  - CRMMinutes: FLOAT NULLABLE - 
  - IsErr: BOOLEAN NULLABLE - 
  - MinutesOutliersOut: FLOAT NULLABLE - 
  - FillinErrors: STRING NULLABLE - 
  - MultivisitDuration: INTEGER NULLABLE - 
  - MutivisitCRMTime: STRING NULLABLE - 
  - MultivisitCount: INTEGER NULLABLE - 
  - MultivisitAdjustedMinutes: FLOAT NULLABLE - 
  - DriveTime: FLOAT NULLABLE - 
  - TotalTime: FLOAT NULLABLE - 
  - ResidentialGeneralPest: INTEGER NULLABLE - 
  - CommercialGeneralPest: INTEGER NULLABLE - 
  - WoodDestroying: INTEGER NULLABLE - 
  - Mosquito: INTEGER NULLABLE - 
  - Other: INTEGER NULLABLE - 
  - Residentialbasic: INTEGER NULLABLE - 
  - ResidentialPremium: INTEGER NULLABLE - 
  - ResidentialPlus: INTEGER NULLABLE - 
  - Excluded: INTEGER NULLABLE - 

## pco-qa.test_layer.t_subscription

- num_rows: 9179

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - dateAdded: TIMESTAMP NULLABLE - 
  - active: INTEGER NULLABLE - 
  - recurringCharge: FLOAT NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - dateCancelled: STRING NULLABLE - 
  - preferredDays: INTEGER NULLABLE - 
  - preferredStart: TIME NULLABLE - 
  - preferredEnd: TIME NULLABLE - 
  - recurringTicket: STRING NULLABLE - 
  - annualRecurringServices: INTEGER NULLABLE - 
  - TicketHelper: STRING NULLABLE - 
  - ConstrainedTime: BOOLEAN NULLABLE - 
  - Commercial: BOOLEAN NULLABLE - 
  - ReserviceableContract: BOOLEAN NULLABLE - 
  - IncludedContract: BOOLEAN NULLABLE - 
  - AdjEndDate: TIMESTAMP NULLABLE - 
  - MajorBucket: STRING NULLABLE - 
  - MinorBucket: STRING NULLABLE - 
  - ResidentialGeneralPest: BOOLEAN NULLABLE - 
  - CommercialGeneralPest: BOOLEAN NULLABLE - 
  - WoodDestroying: BOOLEAN NULLABLE - 
  - Mosquito: BOOLEAN NULLABLE - 
  - Other: BOOLEAN NULLABLE - 
  - TreatasCommercialCustomer: INTEGER NULLABLE - 
  - TicketID: INTEGER NULLABLE - 

## pco-qa.test_layer.test_accel_pricepush

- num_rows: 3870

- schema:
  - clientID: STRING NULLABLE - 
  - raw_ticketID: STRING NULLABLE - 
  - raw_client: STRING NULLABLE - 
  - newProductionValue: INTEGER NULLABLE - 
  - newServiceCharge: INTEGER NULLABLE - 
  - plannedIncrease: INTEGER NULLABLE - 
  - oldServiceCharge: FLOAT NULLABLE - 
  - oldProductionValue: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - dataSource_date: TIMESTAMP NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - annualRecurringValue: FLOAT NULLABLE - 
  - ticketID: STRING NULLABLE - 
  - isNotifed: BOOLEAN NULLABLE - 

## pco-qa.test_layer.test_pricepush

- num_rows: 0

- schema:
  - clientID: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - subscriptionID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - individualAccountID: INTEGER NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - plannedIncrease: FLOAT NULLABLE - 
  - oldServiceCharge: STRING NULLABLE - 
  - oldProductionValue: STRING NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newServiceCharge: STRING NULLABLE - 
  - newProductionValue: STRING NULLABLE - 
  - newAnnualRecurringValue: INTEGER NULLABLE - 
  - recordCreatedAt: STRING NULLABLE - 
  - dataSource_date: DATE NULLABLE - %E4Y-%m-%d
  - isNotified: BOOLEAN NULLABLE - 

## pco-qa.test_layer.trad_appointment

- num_rows: 62190

- schema:
  - OfficeName: STRING NULLABLE - 
  - TechID: INTEGER NULLABLE - 
  - TechName: STRING NULLABLE - 
  - CustomerID: INTEGER NULLABLE - 
  - Customer: STRING NULLABLE - 
  - AppointmentDate: DATE NULLABLE - 
  - TimeIn: STRING NULLABLE - 
  - TimeOut: STRING NULLABLE - 
  - ServiceType: STRING NULLABLE - 
  - AppointmentDuration: INTEGER NULLABLE - 
  - ReportedTimeOnSite: FLOAT NULLABLE - 
  - ReportedDifference: FLOAT NULLABLE - 
  - ActualTimeOnSite: FLOAT NULLABLE - 
  - ActualDifference: FLOAT NULLABLE - 
  - ServiceNotes: STRING NULLABLE - 
  - string_field_15: STRING NULLABLE - 
  - string_field_16: STRING NULLABLE - 
  - string_field_17: STRING NULLABLE - 
  - string_field_18: STRING NULLABLE - 
  - string_field_19: STRING NULLABLE - 
  - string_field_20: STRING NULLABLE - 
  - string_field_21: STRING NULLABLE - 
  - string_field_22: STRING NULLABLE - 
  - string_field_23: STRING NULLABLE - 
  - string_field_24: STRING NULLABLE - 
  - string_field_25: STRING NULLABLE - 

## pco-qa.test_layer.trad_customer

- num_rows: 41292

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 

## pco-qa.test_layer.trad_subscription

- num_rows: 78789

- schema:
  - CustomerID: INTEGER NULLABLE - 
  - SubscriptionID: INTEGER NULLABLE - 
  - Subscription: STRING NULLABLE - 
  - LastName: STRING NULLABLE - 
  - FirstName: STRING NULLABLE - 

## pco-qa.test_layer.try_pisgah_a1

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - ARR: INTEGER NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.try_pisgah_a3

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - totalForecastNTMValue: FLOAT NULLABLE - 
  - forecastHoursInclNonSiteAndReservice: STRING NULLABLE - 
  - forecastDollarProductionPerHour: STRING NULLABLE - 
  - customerLevelCategorizationInputs: STRING NULLABLE - 
  - priceIncrease: STRING NULLABLE - 
  - percentageGapToTarget: STRING NULLABLE - 
  - discussedWithTech: STRING NULLABLE - 
  - recommendDefer: STRING NULLABLE - 
  - recommendContractReview: STRING NULLABLE - 

## pco-qa.test_layer.try_quell_a1

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.try_quell_a3

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - skip3: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - totalForecastNTMValue: FLOAT NULLABLE - 
  - forecastHoursInclNonSiteAndReservice: FLOAT NULLABLE - 
  - forecastDollarProductionPerHour: FLOAT NULLABLE - 
  - customerLevelCategorizationInputs: STRING NULLABLE - 
  - priceIncrease: FLOAT NULLABLE - 
  - percentageGapToTarget: FLOAT NULLABLE - 
  - discussedWithTech: STRING NULLABLE - 
  - recommendDefer: STRING NULLABLE - 
  - recommendContractReview: BOOLEAN NULLABLE - 

## pco-qa.test_layer.try_quell_a4

- num_rows: 0

- schema:
  - subsciptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - Planned_Increase: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: FLOAT NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: INTEGER NULLABLE - 
  - NewCharge: FLOAT NULLABLE - 

## pco-qa.test_layer.try_stb_a1

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - fullName: STRING NULLABLE - 
  - ARR: FLOAT NULLABLE - 
  - tenureSinceStart: FLOAT NULLABLE - 
  - tenureSinceProductChange: FLOAT NULLABLE - 

## pco-qa.test_layer.try_stb_a3

- num_rows: 0

- schema:
  - masterAccountID: INTEGER NULLABLE - 
  - skip1: STRING NULLABLE - 
  - skip2: STRING NULLABLE - 
  - clientId: STRING NULLABLE - 
  - name: STRING NULLABLE - 
  - totalForecastNTMValue: FLOAT NULLABLE - 
  - forecastHoursInclNonSiteAndReservice: FLOAT NULLABLE - 
  - forecastDollarProductionPerHour: FLOAT NULLABLE - 
  - customerLevelCategorizationInputs: STRING NULLABLE - 
  - priceIncrease: FLOAT NULLABLE - 
  - percentageGapToTarget: FLOAT NULLABLE - 
  - discussedWithTech: STRING NULLABLE - 
  - recommendDefer: STRING NULLABLE - 
  - recommendContractReview: BOOLEAN NULLABLE - 

## pco-qa.test_layer.try_stb_a4

- num_rows: 0

- schema:
  - clientID: STRING NULLABLE - 
  - subsciptionID: INTEGER NULLABLE - 
  - customerID: INTEGER NULLABLE - 
  - serviceID: INTEGER NULLABLE - 
  - serviceType: STRING NULLABLE - 
  - ticketID: INTEGER NULLABLE - 
  - Planned_Increase: FLOAT NULLABLE - 
  - oldAnnualRecurringValue: INTEGER NULLABLE - 
  - newAnnualRecurringValue: FLOAT NULLABLE - 
  - oldCharge: FLOAT NULLABLE - 
  - NewCharge: FLOAT NULLABLE - 

## pco-qa.transformation_layer.merged_appointment

- num_rows: 3067681
- description: Description for Appointment transformation table

- schema:
  - appointmentID: INTEGER REQUIRED - Unique Identifier
  - individualAccountID: INTEGER REQUIRED - Unique Identifier - customerID
  - subscriptionID: INTEGER REQUIRED - The ID of the subscription if this appointment belongs to a subscription. Standalone services or reservices will be -1.
  - appointmentDate: DATE NULLABLE - The date this appointment is scheduled for.
  - duration: INTEGER NULLABLE - The number of minutes this appointment is scheduled for.
  - type: INTEGER NULLABLE - The service type ID of this appointment.
  - status: INTEGER NULLABLE - The status code of this appointment.
  - isInitial: INTEGER NULLABLE - Whether or not this is the initial appointment for the associated subscription. 1: yes, 0: no.
  - servicedBy: INTEGER NULLABLE - The employee ID of the technician who serviced this appointment.
  - timeIn: TIMESTAMP NULLABLE - The time the technician checked into the appointment.
  - timeOut: TIMESTAMP NULLABLE - The time the technician checked out of the appointment.
  - ticketID: INTEGER NULLABLE - The ticket/invoice ID associated with the appointment.
  - productionValue: INTEGER NULLABLE - Production value of the appointment.
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated

## pco-qa.transformation_layer.merged_customer

- num_rows: 246405
- description: Description for Ticket transformation table

- schema:
  - masterAccountID: INTEGER REQUIRED - Master Account ID
  - individualAccountID: INTEGER REQUIRED - Unique Identifier - customerID
  - fname: STRING NULLABLE - The first name of the customer
  - lname: STRING NULLABLE - The last name of the customer.
  - status: INTEGER NULLABLE - 0: Inactive Customer, 1: Active Customer
  - billToAccount: STRING NULLABLE - Billing to Account
  - isCommercial: BOOLEAN NULLABLE - False: Not Commercial, True: Commercial
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - companyName: STRING NULLABLE - The company name of the customer
  - masterNumber: STRING NULLABLE - CustomerID for the master account in the connected property
  - email: STRING NULLABLE - Email Address of customer
  - phone1: STRING NULLABLE - The primary phone number of the customer
  - phone2: STRING NULLABLE - The secondary phone number of the customer
  - customerNumber: STRING NULLABLE - Customer Number that comes from the integration

## pco-qa.transformation_layer.merged_service_type

- num_rows: 1751

- schema:
  - typeId: INTEGER NULLABLE - 
  - officeId: INTEGER NULLABLE - 
  - description: STRING NULLABLE - 
  - frequency: INTEGER NULLABLE - 
  - defaultCharge: FLOAT NULLABLE - 
  - category: STRING NULLABLE - 
  - reservice: INTEGER NULLABLE - 
  - defaultLength: INTEGER NULLABLE - 
  - defaultInitialCharge: FLOAT NULLABLE - 
  - initialId: INTEGER NULLABLE - 
  - minimumRecurringCharge: FLOAT NULLABLE - 
  - minimumInitialCharge: FLOAT NULLABLE - 
  - regularService: INTEGER NULLABLE - 
  - initial: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.transformation_layer.merged_subscription

- num_rows: 310445
- description: Description for Ticket transformation table

- schema:
  - subscriptionID: INTEGER REQUIRED - Unique ID
  - individualAccountID: INTEGER REQUIRED - Column mapped with Billing Account which this subscription belongs to
  - dateAdded: TIMESTAMP NULLABLE - Date this subscription was added / created. Sold date.
  - active: INTEGER NULLABLE - 0: frozen, 1: active and being placed in the job pool.
  - recurringCharge: FLOAT NULLABLE - A shortcut for the subtotal of the recurring ticket
  - annualRecurringValue: FLOAT NULLABLE - Calculated as the recurringCharge * the number of services/year.
  - serviceID: INTEGER NULLABLE - The service type ID for the regular recurring services
  - serviceType: STRING NULLABLE - Friendly description of the serviceID
  - dateCancelled: TIMESTAMP NULLABLE - The date the subscription was cancelled if any.
  - preferredDays: INTEGER NULLABLE - Preferred days for appointments
  - preferredStart: TIME NULLABLE - Preferred appointment start time bound
  - preferredEnd: TIME NULLABLE - Preferred appointment end time bound
  - recurringTicket: STRING NULLABLE - The recurring ticket template associated with this subscription
  - annualRecurringServices: INTEGER NULLABLE - Services per year without counting initial
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - contractAdded: TIMESTAMP NULLABLE - Date the last contract was added / created
  - duration: INTEGER NULLABLE - Default duration of services in minutes
  - agreementLength: INTEGER NULLABLE - Agreement Length
  - seasonalStart: TIMESTAMP NULLABLE - seasonal start
  - seasonalEnd: TIMESTAMP NULLABLE - seasonal end
  - yifDiscount: FLOAT NULLABLE - seasonal start

## pco-qa.transformation_layer.merged_ticket

- num_rows: 4474500
- description: Description for Ticket transformation table

- schema:
  - ticketID: INTEGER REQUIRED - Unique ID
  - customerID: INTEGER NULLABLE - Unique Identifier
  - prodFromTicket: FLOAT NULLABLE - Value for reporting / commission purposes.
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - ticketProductionValue: FLOAT NULLABLE - Production value from ticket
  - active: INTEGER NULLABLE - 0: pending, 1: active and counts towards customer's outstanding balance, -1: a ticket template.
  - taxAmount: FLOAT NULLABLE - Total calculated tax
  - total: FLOAT NULLABLE - Subtotal + total
  - serviceCharge: FLOAT NULLABLE - Base charge for this ticket
  - serviceTaxable: INTEGER NULLABLE - Whether or not the base service charge for this ticket is taxable (1) or not (0)
  - balance: FLOAT NULLABLE - Any unpaid balance left on this ticket

## pco-qa.transformation_layer.rdate_merged_ticket

- num_rows: 1303031

- schema:
  - ticketID: INTEGER REQUIRED - Unique ID
  - customerID: INTEGER NULLABLE - Unique Identifier
  - prodFromTicket: FLOAT NULLABLE - Value for reporting / commission purposes.
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the ticket was updated
  - ticketProductionValue: FLOAT NULLABLE - Production value from ticket
  - active: INTEGER NULLABLE - 0: pending, 1: active and counts towards customer's outstanding balance, -1: a ticket template.
  - taxAmount: FLOAT NULLABLE - Total calculated tax
  - total: FLOAT NULLABLE - Subtotal + total
  - serviceCharge: FLOAT NULLABLE - Base charge for this ticket
  - serviceTaxable: INTEGER NULLABLE - Whether or not the base service charge for this ticket is taxable (1) or not (0)
  - balance: FLOAT NULLABLE - Any unpaid balance left on this ticket
  - refDate: TIMESTAMP NULLABLE - The date at which the record get loaded or updated

## pco-qa.transformation_layer.t_appointment

- num_rows: 3634253
- description: Description for Appointment transformation table

- schema:
  - appointmentID: INTEGER REQUIRED - Unique Identifier
  - individualAccountID: INTEGER REQUIRED - Unique Identifier - customerID
  - subscriptionID: INTEGER REQUIRED - The ID of the subscription if this appointment belongs to a subscription. Standalone services or reservices will be -1.
  - appointmentDate: DATE NULLABLE - The date this appointment is scheduled for.
  - duration: INTEGER NULLABLE - The number of minutes this appointment is scheduled for.
  - type: INTEGER NULLABLE - The service type ID of this appointment.
  - status: INTEGER NULLABLE - The status code of this appointment.
  - isInitial: INTEGER NULLABLE - Whether or not this is the initial appointment for the associated subscription. 1: yes, 0: no.
  - servicedBy: INTEGER NULLABLE - The employee ID of the technician who serviced this appointment.
  - timeIn: TIMESTAMP NULLABLE - The time the technician checked into the appointment.
  - timeOut: TIMESTAMP NULLABLE - The time the technician checked out of the appointment.
  - ticketID: INTEGER NULLABLE - The ticket/invoice ID associated with the appointment.
  - productionValue: INTEGER NULLABLE - Production value of the appointment.
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - isOverride: BOOLEAN NULLABLE - DQS - Override

## pco-qa.transformation_layer.t_appointment_helper

- num_rows: 375796

- schema:
  - appointmentID: INTEGER NULLABLE - 
  - masterAccountID: INTEGER NULLABLE - 
  - prodFromTicket: FLOAT NULLABLE - 
  - isRecurring: BOOLEAN NULLABLE - 
  - allocateReservices: BOOLEAN NULLABLE - 
  - isRervice: BOOLEAN NULLABLE - 
  - zeroVisitTime: BOOLEAN NULLABLE - 
  - computedAppointmentDate: DATE NULLABLE - 
  - twoYearsBefore: DATE NULLABLE - 
  - reference_date: DATE NULLABLE - 
  - inRefPeriod: BOOLEAN NULLABLE - 
  - crmMinutes: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - durationRatio: FLOAT NULLABLE - 
  - isErr: BOOLEAN NULLABLE - 
  - outlierFloor: BOOLEAN NULLABLE - 
  - outlierCeilShort: BOOLEAN NULLABLE - 
  - outlierCeilLong: BOOLEAN NULLABLE - 
  - inTwoYearLookBack: BOOLEAN NULLABLE - 
  - includeInAverage: BOOLEAN NULLABLE - 
  - durationRatioOverall: FLOAT NULLABLE - 
  - durationRatioInitials: FLOAT NULLABLE - 
  - durationRatioAvg: FLOAT NULLABLE - 
  - errorsOut: FLOAT NULLABLE - 
  - outliersOut: FLOAT NULLABLE - 
  - isMultiVisit: BOOLEAN NULLABLE - 
  - multivisitStartDate: TIMESTAMP NULLABLE - 
  - multivisitEndDate: TIMESTAMP NULLABLE - 
  - multi_visit_key: STRING NULLABLE - 
  - multivisitCount: INTEGER NULLABLE - 
  - onsiteMinutes: STRING NULLABLE - 

## pco-qa.transformation_layer.t_customer

- num_rows: 432295
- description: Description for Ticket transformation table

- schema:
  - masterAccountID: INTEGER REQUIRED - Master Account ID
  - individualAccountID: INTEGER REQUIRED - Unique Identifier - customerID
  - fname: STRING NULLABLE - The first name of the customer
  - lname: STRING NULLABLE - The last name of the customer.
  - status: INTEGER NULLABLE - 0: Inactive Customer, 1: Active Customer
  - billToAccount: STRING NULLABLE - Billing to Account
  - isCommercial: BOOLEAN NULLABLE - False: Not Commercial, True: Commercial
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - companyName: STRING NULLABLE - The company name of the customer
  - masterNumber: STRING NULLABLE - CustomerID for the master account in the connected property
  - email: STRING NULLABLE - Email Address of customer
  - phone1: STRING NULLABLE - The primary phone number of the customer
  - phone2: STRING NULLABLE - The secondary phone number of the customer
  - customerNumber: STRING NULLABLE - Customer Number that comes from the integration
  - isOverride: BOOLEAN NULLABLE - DQS - Override

## pco-qa.transformation_layer.t_service_type

- num_rows: 2009

- schema:
  - typeId: INTEGER NULLABLE - 
  - officeId: INTEGER NULLABLE - 
  - description: STRING NULLABLE - 
  - frequency: INTEGER NULLABLE - 
  - defaultCharge: FLOAT NULLABLE - 
  - category: STRING NULLABLE - 
  - reservice: INTEGER NULLABLE - 
  - defaultLength: INTEGER NULLABLE - 
  - defaultInitialCharge: FLOAT NULLABLE - 
  - initialId: INTEGER NULLABLE - 
  - minimumRecurringCharge: FLOAT NULLABLE - 
  - minimumInitialCharge: FLOAT NULLABLE - 
  - regularService: INTEGER NULLABLE - 
  - initial: INTEGER NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 

## pco-qa.transformation_layer.t_subscription

- num_rows: 579911
- description: Description for Ticket transformation table

- schema:
  - subscriptionID: INTEGER REQUIRED - Unique ID
  - individualAccountID: INTEGER REQUIRED - Column mapped with Billing Account which this subscription belongs to
  - dateAdded: TIMESTAMP NULLABLE - Date this subscription was added / created. Sold date.
  - active: INTEGER NULLABLE - 0: frozen, 1: active and being placed in the job pool.
  - recurringCharge: FLOAT NULLABLE - A shortcut for the subtotal of the recurring ticket
  - annualRecurringValue: FLOAT NULLABLE - Calculated as the recurringCharge * the number of services/year.
  - serviceID: INTEGER NULLABLE - The service type ID for the regular recurring services
  - serviceType: STRING NULLABLE - Friendly description of the serviceID
  - dateCancelled: TIMESTAMP NULLABLE - The date the subscription was cancelled if any.
  - preferredDays: INTEGER NULLABLE - Preferred days for appointments
  - preferredStart: TIME NULLABLE - Preferred appointment start time bound
  - preferredEnd: TIME NULLABLE - Preferred appointment end time bound
  - recurringTicket: STRING NULLABLE - The recurring ticket template associated with this subscription
  - annualRecurringServices: INTEGER NULLABLE - Services per year without counting initial
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - contractAdded: TIMESTAMP NULLABLE - Date the last contract was added / created
  - duration: INTEGER NULLABLE - Default duration of services in minutes
  - agreementLength: INTEGER NULLABLE - Agreement Length
  - seasonalStart: TIMESTAMP NULLABLE - seasonal start
  - seasonalEnd: TIMESTAMP NULLABLE - seasonal end
  - yifDiscount: FLOAT NULLABLE - seasonal start
  - isOverride: BOOLEAN NULLABLE - DQS - Override

## pco-qa.transformation_layer.t_subscription_helper

- num_rows: 309313

- schema:
  - subscriptionID: INTEGER NULLABLE - 
  - masterAccountID: FLOAT NULLABLE - 
  - recordCreatedAt: TIMESTAMP NULLABLE - 
  - clientId: STRING NULLABLE - 
  - crmSource: STRING NULLABLE - 
  - includedContract: BOOLEAN NULLABLE - 
  - adjEndDate: TIMESTAMP NULLABLE - 
  - endAfterRefDate: BOOLEAN NULLABLE - 
  - startOnOrBeforeRefDate: BOOLEAN NULLABLE - 
  - allocateReservice: BOOLEAN NULLABLE - 
  - isActive: BOOLEAN NULLABLE - 
  - constrainedTime: INTEGER NULLABLE - 
  - ticketID: STRING NULLABLE - 

## pco-qa.transformation_layer.t_ticket

- num_rows: 4542920
- description: Description for Ticket transformation table

- schema:
  - ticketID: INTEGER REQUIRED - Unique ID
  - customerID: INTEGER NULLABLE - Unique Identifier
  - prodFromTicket: FLOAT NULLABLE - Value for reporting / commission purposes.
  - recordCreatedAt: TIMESTAMP NULLABLE - A created timestamp logs when a record is added.
  - clientId: STRING NULLABLE - Client ID
  - crmSource: STRING NULLABLE - CRM SOURCE
  - dateUpdated: TIMESTAMP NULLABLE - The date the appointment was updated
  - ticketProductionValue: FLOAT NULLABLE - Production value from ticket
  - active: INTEGER NULLABLE - 0: pending, 1: active and counts towards customer's outstanding balance, -1: a ticket template.
  - taxAmount: FLOAT NULLABLE - Total calculated tax
  - total: FLOAT NULLABLE - Subtotal + total
  - serviceCharge: FLOAT NULLABLE - Base charge for this ticket
  - serviceTaxable: INTEGER NULLABLE - Whether or not the base service charge for this ticket is taxable (1) or not (0)
  - balance: FLOAT NULLABLE - Any unpaid balance left on this ticket
  - isOverride: BOOLEAN NULLABLE - DQS - Override

## pco-qa.visualization_layer.backlog_of_ttm

- num_rows: 3

- schema:
  - Metric: STRING NULLABLE - 
  - Value: FLOAT NULLABLE - 

## pco-qa.visualization_layer.cohort_stats

- num_rows: 4

- schema:
  - reportDate: TIMESTAMP NULLABLE - 
  - requireExtraTimeAttention: STRING NULLABLE - 
  - freqReservice: STRING NULLABLE - 
  - minutesBetween30And45: STRING NULLABLE - 
  - minVisitsGreaterThan45: STRING NULLABLE - 
  - freqReschedule: STRING NULLABLE - 
  - constrainedSchedule: STRING NULLABLE - 
  - categories: STRING NULLABLE - 
  - countCategories: INTEGER NULLABLE - 

## pco-qa.visualization_layer.full_base_stats

- num_rows: 7

- schema:
  - reportDate: TIMESTAMP NULLABLE - 
  - eligibilityCriteria: STRING NULLABLE - 
  - countEligibility: INTEGER NULLABLE - 
  - categoryName: STRING NULLABLE - 
  - countCategoryName: STRING NULLABLE - 
  - eligibleCustomerValueRisk: STRING NULLABLE - 
  - amountDollars: STRING NULLABLE - 
  - requireExtraTimeAttention: STRING NULLABLE - 
  - freqReservice: STRING NULLABLE - 
  - minutesBetween30And45: STRING NULLABLE - 
  - includedVisitsGreaterThan45: STRING NULLABLE - 
  - freqReschedule: STRING NULLABLE - 
  - constrainedSchedule: STRING NULLABLE - 

## pco-qa.visualization_layer.price_plan_summary

- num_rows: 9

- schema:
  - reportDate: TIMESTAMP NULLABLE - 
  - performerProduct: STRING NULLABLE - 
  - priceIncrease: FLOAT NULLABLE - 
  - customerCategory: STRING NULLABLE - 
  - priceIncreaseCategory: STRING NULLABLE - 
  - valueCategory: STRING NULLABLE - 
  - yearOneValue: STRING NULLABLE - 
  - yearTwoValue: STRING NULLABLE - 
  - yearThreeValue: STRING NULLABLE - 
  - valueEstimate: STRING NULLABLE - 
  - oddsOfBreakeven: STRING NULLABLE - 

## pco-qa.visualization_layer.ttm_status

- num_rows: 4

- schema:
  - Metric: STRING NULLABLE - 
  - Value: FLOAT NULLABLE - 

