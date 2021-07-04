CREATE TABLE IF NOT EXISTS web_scraping.drog_cafam(
	id						SERIAL,
	url_producto			TEXT,
	fecha_scraping			TEXT,
	hora_scraping			TEXT,
	titulo					TEXT,
	marca_producto			TEXT,
	precio_tachado			NUMERIC,
	precio_original			NUMERIC,
	PRIMARY KEY (url_producto, fecha_scraping)
);