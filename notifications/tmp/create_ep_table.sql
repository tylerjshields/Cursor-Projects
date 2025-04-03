-- Create or replace the table to hold EP consumer data
CREATE OR REPLACE TABLE proddb.public.nvg_channels_ep_daily (
    ds DATE DEFAULT CURRENT_DATE(),
    ep_name VARCHAR,
    consumer_id VARCHAR,
    insertion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
