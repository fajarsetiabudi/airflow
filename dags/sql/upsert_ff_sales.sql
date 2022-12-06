INSERT INTO ff_sales (
  trx_id,
  trx_dt,
  cust_id,
  cust_name,
  cust_gender,
  prd_id,
  prd_name,
  prd_category,
  prd_base_price,
  trx_qty,
  trx_total_price
)
(
  SELECT
    trx.id AS trx_id,
    trx.trx_dt,
    cust.id AS cust_id,
    cust.name AS cust_name,
    cust.gender AS cust_gender,
    prd.id AS prd_id,
    prd.name AS prd_name,
    prd.category AS prd_category,
    prd.price AS prd_base_price,
    trx.qty AS trx_qty,
    trx.total_price AS trx_total_price
  FROM trx_transactions AS trx
  LEFT JOIN ref_customers AS cust
    ON trx.cust_id = cust.id
  LEFT JOIN ref_products AS prd
    ON trx.prd_id = prd.id
)
ON CONFLICT (trx_id) DO UPDATE
SET trx_dt = excluded.trx_dt,
    cust_name = excluded.cust_name,
    cust_gender = excluded.cust_gender,
    prd_name = excluded.prd_name,
    prd_category = excluded.prd_category,
    prd_base_price = excluded.prd_base_price,
    trx_qty = excluded.trx_qty,
    trx_total_price = excluded.trx_total_price;