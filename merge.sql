CREATE TABLE IF NOT EXISTS $table_id(
    host_name STRING,
    data STRING,
    user STRING,
    tempo_logado STRING
);


MERGE `$table_id` AS t
USING `$stage_table_id` AS s
ON t.host_name = s.host_name AND t.data = s.data AND t.user = s.user
WHEN MATCHED THEN
UPDATE SET t.tempo_logado = s.tempo_logado
WHEN NOT MATCHED THEN
INSERT (host_name, data, user, tempo_logado)
VALUES (s.host_name, s.data, s.user, s.tempo_logado)