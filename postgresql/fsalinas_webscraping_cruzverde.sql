CREATE TABLE IF NOT EXISTS web_scraping.drog_cruzverde(
	id						SERIAL,
	url_producto			TEXT,
	fecha_scraping			TEXT,
	hora_scraping			TEXT,
	titulo					TEXT,
	marca_producto			TEXT,
	nota_envio				TEXT,
	precio_oferta			NUMERIC,
	precio_original			NUMERIC,
	PRIMARY KEY (url_producto, fecha_scraping)
);