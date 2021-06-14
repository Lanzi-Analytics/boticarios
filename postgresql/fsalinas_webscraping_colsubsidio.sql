CREATE TABLE IF NOT EXISTS web_scraping.drog_colsubsidio(
	id						SERIAL,
	url_producto			TEXT PRIMARY KEY,
	fecha_hora_scraping		TIMESTAMP WITHOUT TIME ZONE,
	breadcumb				TEXT,
	titulo					TEXT,
	nombre_imagen			TEXT,
	presentacion			TEXT,
	precio					TEXT,
	descripcion				TEXT,
	atributos				TEXT
);

CREATE TABLE IF NOT EXISTS web_scraping.drog_colsubsidio2(
	id						SERIAL,
	url_producto			TEXT,
	fecha_scraping			TEXT,
	hora_scraping			TEXT,
	titulo					TEXT,
	presentacion			TEXT,
	precio_tachado			NUMERIC,
	precio_final			NUMERIC,
	PRIMARY KEY (url_producto, fecha_scraping)
);