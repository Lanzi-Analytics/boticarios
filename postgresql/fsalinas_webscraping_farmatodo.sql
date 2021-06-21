CREATE TABLE IF NOT EXISTS web_scraping.drog_farmatodo(
	id						SERIAL,
	url_producto			TEXT,
	fecha_scraping			TEXT,
	hora_scraping			TEXT,
	titulo					TEXT,
	presentacion			TEXT,
	precio_por				TEXT,
	calificacion			SMALLINT,
	numero_calificaciones	INTEGER,
	tiempo_entrega			TEXT,
	precio_tachado			NUMERIC,
	precio_final			NUMERIC,
	PRIMARY KEY (url_producto, fecha_scraping)
);