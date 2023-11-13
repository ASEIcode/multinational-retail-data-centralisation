SELECT * FROM dim_date_times

-- recast dtypes
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(12),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;