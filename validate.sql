-- 1. Total number of rows
SELECT COUNT(*) AS total_rows
FROM sellers;

-- 2. Preview the first 10 rows
SELECT *
FROM sellers
LIMIT 10;

-- 3. Check for NULLs in each column
SELECT
  'seller_id' AS column_name, COUNT(*) AS null_count
FROM sellers WHERE seller_id IS NULL
UNION ALL
SELECT
  'seller_zip_code_prefix', COUNT(*)
FROM sellers WHERE seller_zip_code_prefix IS NULL
UNION ALL
SELECT
  'seller_city', COUNT(*)
FROM sellers WHERE seller_city IS NULL
UNION ALL
SELECT
  'seller_state', COUNT(*)
FROM sellers WHERE seller_state IS NULL;

-- 4. Check for duplicates by seller_id (should be unique)
SELECT
  seller_id,
  COUNT(*) AS occurrences
FROM sellers
GROUP BY seller_id
HAVING COUNT(*) > 1;

-- 5. Validate zip code range (optional sanity check)
SELECT
  MIN(seller_zip_code_prefix) AS min_zip,
  MAX(seller_zip_code_prefix) AS max_zip
FROM sellers;

-- 6. Count unique cities and states
SELECT COUNT(DISTINCT seller_city) AS unique_cities,
       COUNT(DISTINCT seller_state) AS unique_states
FROM sellers;