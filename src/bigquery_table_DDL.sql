-- burea_agg_ext.sql
-- This table aggregates bureau data for each SK_ID_CURR
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.bureau_agg_ext (
  SK_ID_CURR INT64,
  total_credit FLOAT64,
  total_debt FLOAT64,
  bureau_debt_ratio FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/bureau_agg/*.parquet']
);


-- inst_late_avg_ext.sql
-- This table contains the average number of days late for each SK_ID_CURR in the installment
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.inst_late_avg (
  SK_ID_CURR INT64,
  avg_days_late FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/inst_late_avg/*.parquet']
);


-- prev_app_stats_ext.sql
-- This table contains the average difference between requested and approved amounts for previous applications
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.bureau_agg_ext (
  SK_ID_CURR INT64,
  total_credit FLOAT64,
  total_debt FLOAT64,
  bureau_debt_ratio FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/bureau_agg/*.parquet']
);


-- prev_stats_ext.sql
-- This table contains the average difference between requested and approved amounts for previous applications
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.prev_stats_ext
(
  SK_ID_CURR INT64,
  avg_diff_requested_approved FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/prev_app_stats/*.parquet']
);


-- severe_bureau_counts_ext.sql
-- This table contains the count of severe overdue statuses (5) for each SK_ID_CURR
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.severe_bureau_counts_ext
(
  SK_ID_CURR INT64,
  count INT64  -- rename during transformation if desired as severe_dpd_count
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/severe_bureau_counts/*.parquet']
);

-- pos_health_ext.sql
-- This table contains the future installment health for POS loans
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.pos_health_ext
(
  SK_ID_CURR INT64,
  future_pos_installments FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/pos_health/*.parquet']
);


-- cc_util_avg_ext.sql
-- This table contains the average credit card utilization for each SK_ID_CURR
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.cc_util_avg_ext
(
  SK_ID_CURR INT64,
  avg_cc_utilization FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/cc_util_avg/*.parquet']
);


-- purpose_risk_ext.sql
-- This table contains the average number of days late for each loan purpose
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.purpose_risk_ext
(
  NAME_CASH_LOAN_PURPOSE STRING,
  avg_avg_days_late FLOAT64  -- BigQuery aggregates are auto-prefixed with 'avg_'
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/purpose_risk/*.parquet']
);


-- temporal_pattern_ext.sql
-- This table contains the average number of days late for each weekday when the application process starts
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.temporal_pattern_ext
(
  WEEKDAY_APPR_PROCESS_START STRING,
  avg_avg_days_late FLOAT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/temporal_pattern/*.parquet']
);


-- contract_mix_ext.sql
-- This table contains the count of previous contracts and the diversity of contract types for each SK_ID
CREATE OR REPLACE EXTERNAL TABLE glass-chimera-465105-t5.cred_dataset.contract_mix_ext
(
  SK_ID_CURR INT64,
  prev_contract_count INT64,
  contract_type_diversity INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://cred_gold/contract_mix/*.parquet']
);