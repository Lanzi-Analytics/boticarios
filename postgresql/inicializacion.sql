 /*
 * Proyecto: BOTICARIOS.COM
 *
 * Host: localhost
 * Port: 5432
 * Usuarios: (postgres, aadd4455), (fsalinas, fsalinas1547), (cgomez, cgomez4526)
 * Base de datos: boticarios
 * Esquemas: web_scraping
 *
 */
 
CREATE ROLE fsalinas WITH SUPERUSER CREATEDB CREATEROLE LOGIN ENCRYPTED PASSWORD 'fsalinas1547';
CREATE ROLE cgomez WITH SUPERUSER CREATEDB CREATEROLE LOGIN ENCRYPTED PASSWORD 'cgomez4526';
CREATE DATABASE boticarios;
CREATE SCHEMA IF NOT EXISTS web_scraping;
CREATE SCHEMA IF NOT EXISTS datawarehouse;