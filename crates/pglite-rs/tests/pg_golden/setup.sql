CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT, n INT, price NUMERIC(10,2), meta JSONB, tags TEXT[], created TIMESTAMP DEFAULT now(), flag BOOLEAN)
CREATE TABLE u (id SERIAL PRIMARY KEY, t_id INT REFERENCES t(id), v TEXT)
INSERT INTO t (name, n, price, meta, tags, flag) VALUES ('alice', 5, 1.50, '{"a":1}', '{x,y}', true), ('bob', 3, 2.00, '{"a":2}', '{y}', false), ('carol', NULL, NULL, NULL, NULL, NULL)
INSERT INTO u (t_id, v) VALUES (1, 'v1'), (1, 'v2'), (2, 'v3')
