use ragarwal;

DROP TABLE IF EXISTS clickstream;
CREATE EXTERNAL TABLE IF NOT EXISTS clickstream(
    user_id int,
    session_id int,
    event_type STRING,
    event_page STRING,
    timestamp int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION 'clickstream.csv'
TBLPROPERTIES ("skip.header.line.count" = "1");


-- SELECT * FROM clickstream LIMIT 10;
WITH error_data AS (
    SELECT user_id, session_id, MIN(timestamp) AS error_time
    FROM clickstream
    WHERE event_type LIKE '%error%'
    GROUP BY user_id, session_id
),
joined AS (
    SELECT clickstream.*, COALESCE(error_data.error_time, 99999999999999999) AS error_time
    FROM clickstream
    LEFT JOIN error_data
        ON clickstream.user_id = error_data.user_id
        AND clickstream.session_id = error_data.session_id
),
valid_data AS (
    SELECT * FROM joined
    WHERE timestamp < error_time and event_type == 'page'
    ORDER BY timestamp, event_page
),
temporary_table1 AS (
SELECT user_id, session_id, event_page, timestamp,
    DENSE_RANK() OVER (PARTITION BY user_id, session_id ORDER BY timestamp, event_page) AS key
    FROM valid_data
),
temporary_table2 AS (
SELECT user_id, session_id, key, timestamp, MIN(event_page) AS event_page FROM temporary_table1 
GROUP BY user_id, session_id, key, timestamp),

routes_as_arrays AS (
    SELECT user_id, session_id, collect_list(event_page) AS route_arr
    FROM temporary_table2
    GROUP BY user_id, session_id
),
routes_as_strings AS (
    SELECT concat_ws("-", route_arr) AS route_str
    FROM routes_as_arrays
)
SELECT route_str, COUNT(1) AS counter
FROM routes_as_strings
GROUP BY route_str
ORDER BY counter DESC
LIMIT 30;
