INSERT INTO sd_sales_dtls (
  trx_dt,
  total_trx,
  total_unique_cust,
  total_prd_sold,
  total_unique_prd,
  total_gmv
)
(
  SELECT
    trx_dt,
    COUNT(DISTINCT trx_id) AS total_trx,
    COUNT(DISTINCT cust_id) AS total_unique_cust,
    SUM(trx_qty) AS total_prd_sold,
    COUNT(DISTINCT prd_id) AS total_unique_prd,
    SUM(trx_total_price) AS total_gmv
  FROM ff_sales
  GROUP BY trx_dt
)
ON CONFLICT (trx_dt) DO UPDATE
SET total_trx = excluded.total_trx,
    total_unique_cust = excluded.total_unique_cust,
    total_prd_sold = excluded.total_prd_sold,
    total_unique_prd = excluded.total_unique_prd,
    total_gmv = excluded.total_gmv;