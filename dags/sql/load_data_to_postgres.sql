COPY {{params.tablename}}
FROM '/shared/data-csv/{{params.filename}}'
DELIMITER ','
CSV HEADER;