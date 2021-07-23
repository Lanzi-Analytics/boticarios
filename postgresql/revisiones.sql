select *
from web_scraping.drog_colsubsidio2
where lower(titulo) like '%omepra%'

select *
from web_scraping.drog_colsubsidio2
where lower(titulo) like '%anot%'

select *
from web_scraping.drog_colsubsidio2
where url_producto in ('https://www.drogueriascolsubsidio.com/esomeprazol-40-mg-tableta-con-cubierta-enterica-procaps-7703153022075/p')

select fecha_scraping, count(*) as n
from web_scraping.drog_colsubsidio2
group by 1
order by 1


select *
from web_scraping.drog_colsubsidio2

select *
from web_scraping.drog_farmatodo
where lower(titulo) like '%panotil%'

select *
from web_scraping.drog_cruzverde

select fecha_scraping, count(*) as n
from web_scraping.drog_farmatodo
group by 1
order by 1

select *
from web_scraping.drog_cruzverde
where lower(titulo) like '%panotil%'

select fecha_scraping, count(*) as n
from web_scraping.drog_cruzverde
group by 1
order by 1





select fecha_scraping, count(*) as n
from web_scraping.drog_colsubsidio2
group by 1
order by 1

select fecha_scraping, count(*) as n
from web_scraping.drog_cruzverde
group by 1
order by 1

select fecha_scraping, count(*) as n
from web_scraping.drog_farmatodo
group by 1
order by 1

select fecha_scraping, count(*) as n
from web_scraping.drog_cafam
group by 1
order by 1

-- Revisión día
select * from (
	select 'colsubsidio' as drogueria, (select max(fecha_scraping) as f from web_scraping.drog_colsubsidio2) as fecha, count(*) as n
	from web_scraping.drog_colsubsidio2
	where fecha_scraping = (select max(fecha_scraping) as f from web_scraping.drog_colsubsidio2)
	
	union
	
	select 'farmatodo' as drogueria, (select max(fecha_scraping) as f from web_scraping.drog_farmatodo) as fecha, count(*) as n
	from web_scraping.drog_farmatodo
	where fecha_scraping = (select max(fecha_scraping) as f from web_scraping.drog_farmatodo)
	
	union
	
	select 'cruzverde' as drogueria, (select max(fecha_scraping) as f from web_scraping.drog_cruzverde) as fecha, count(*) as n
	from web_scraping.drog_cruzverde
	where fecha_scraping = (select max(fecha_scraping) as f from web_scraping.drog_cruzverde)
	
	union
	
	select 'cafam' as drogueria, (select max(fecha_scraping) as f from web_scraping.drog_cruzverde) as fecha, count(*) as n
	from web_scraping.drog_cafam
	where fecha_scraping = (select max(fecha_scraping) as f from web_scraping.drog_cruzverde)
) as t order by 1


-- Revisión día
select * from (
	select 'colsubsidio' as drogueria, fecha_scraping, count(*) as n
	from web_scraping.drog_colsubsidio2
	group by 2
	
	union
	
	select 'farmatodo' as drogueria, fecha_scraping, count(*) as n
	from web_scraping.drog_farmatodo
	group by 2
	
	union
	
	select 'cruzverde' as drogueria, fecha_scraping, count(*) as n
	from web_scraping.drog_cruzverde
	group by 2
	
	union
	
	select 'cafam' as drogueria, fecha_scraping, count(*) as n
	from web_scraping.drog_cafam
	group by 2
) as t order by 1, 2



select 'cruzverde' as drogueria, titulo, fecha_scraping, precio_oferta, precio_original
from web_scraping.drog_cruzverde
where lower(titulo) like '%omepra%'

union

select 'colsubsidio' as drogueria, titulo, fecha_scraping, precio_final, precio_tachado
from web_scraping.drog_colsubsidio2
where lower(titulo) like '%omepra%'

union

select 'farmatodo' as drogueria, titulo, fecha_scraping, precio_final, precio_tachado
from web_scraping.drog_farmatodo
where lower(titulo) like '%omepra%'



select *
from web_scraping.drog_cruzverde
where lower(titulo) like '%genoprazol%'










select *
from web_scraping.drog_colsubsidio2


select *
from web_scraping.drog_cruzverde


select fecha_scraping, count(*) as n
from web_scraping.drog_farmatodo
group by 1
order by 1

















