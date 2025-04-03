-- Create a table to store combined results from all engagement programs
-- This creates an empty table with the right structure that can be populated
-- through a separate process

-- Create the table
CREATE OR REPLACE TABLE proddb.public.nvg_channels_ep_daily (
    ds DATE,
    ep_name VARCHAR,
    consumer_id VARCHAR,
    total_consumers_in_program NUMBER,
    metadata VARCHAR,  -- Optional field for additional data
    
    -- Track when this record was added
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments to explain the table
COMMENT ON TABLE proddb.public.nvg_channels_ep_daily IS 
'Stores combined consumer IDs from all engagement programs that appear in the notification index. 
This table should be populated daily with the consumers eligible for each EP.';

-- Add column comments
COMMENT ON COLUMN proddb.public.nvg_channels_ep_daily.ds IS 'Date of the program run';
COMMENT ON COLUMN proddb.public.nvg_channels_ep_daily.ep_name IS 'Name of the engagement program';
COMMENT ON COLUMN proddb.public.nvg_channels_ep_daily.consumer_id IS 'Consumer ID targeted by the program';
COMMENT ON COLUMN proddb.public.nvg_channels_ep_daily.total_consumers_in_program IS 'Total number of consumers in this program for this date';
COMMENT ON COLUMN proddb.public.nvg_channels_ep_daily.metadata IS 'Optional field for additional data from the program query';
COMMENT ON COLUMN proddb.public.nvg_channels_ep_daily.created_at IS 'Timestamp when this record was created'; 