with product as (SELECT
MARA.MATNR material_id,
MAKT.MAKTX AS material_name,
MARA.MEINS base_unit_of_measure,
MARA.MTART material_type,
T134T.MTBEZ AS product_category,
MARA.MATKL material_group,
T023T.WGBEZ AS product_sub_category,
MARA.BISMT odoo_product_id,
MARA.ZZBRAND_DESC brand_name
FROM SAPHANADB.MARA
INNER JOIN SAPHANADB.MAKT ON MARA.MATNR=MAKT.MATNR AND MAKT.SPRAS='E'
LEFT JOIN SAPHANADB.T134T ON MARA.MTART=T134T.MTART AND T134T.SPRAS='E' AND T134T.MANDT=100
LEFT JOIN SAPHANADB.T023T ON MARA.MATKL=T023T.MATKL AND T023T.SPRAS='E' AND T023T.MANDT=100
LEFT JOIN SAPHANADB.TSPAT ON MARA.SPART=TSPAT.SPART AND TSPAT.SPRAS='E' AND TSPAT.MANDT=100
LEFT JOIN SAPHANADB.MARM on MARM.MEINH in ('KG','L') and MARA.MATNR = MARM.MATNR
WHERE MARA.MTART LIKE 'Z%' and MARA.MANDT=100
),
location as (SELECT
	state_data.state_id AS state_id,
	state_data.state_name AS state_name,
	T001W.WERKS AS node_id,
	T001W.NAME1 AS node_name
	
FROM SAPHANADB.T001W
LEFT JOIN
(SELECT
	T005S.BLAND AS state_id,
	T005U.BEZEI AS state_name
FROM SAPHANADB.T005S
LEFT JOIN SAPHANADB.T005U ON T005U.LAND1=T005S.LAND1 AND T005U.BLAND=T005S.BLAND AND T005U.MANDT=T005S.MANDT
WHERE T005S.MANDT=100 AND T005S.LAND1='IN' AND T005U.SPRAS = 'E' AND LENGTH(LTRIM(T005U.BLAND,' +-.0123456789'))=0
) AS state_data ON T001W.REGIO=state_data.state_id
WHERE T001W.MANDT=100)


select
location.state_id,
location.state_name,
location.node_id,
location.node_name,
product.material_id,
product.material_name,
case when MARC.PSTAT is not null then 'Yes' end extended_at_node,
case when MARC.LVORM='X' then 'Yes' end blocked_at_node,
product.base_unit_of_measure,
product.material_type,
product.product_category,
product.material_group,
product.product_sub_category,
product.odoo_product_id,
product.brand_name,
a901.kschl as scheme_code,
t685t.vtext as scheme_type,
to_date(A901.DATAB) valid_from_date,
to_date(A901.DATBI) valid_to_date,
A901.KNUMH condition_Record_number,
konp.kopos as condition_item,
 KONP.KBETR price_per_unit,
konp.konwa price_type,
KONP.KPEIN pricing_unit,
KONP.KMEIN unit_of_measure,
case when current_date >= to_date(A901.DATAB) and current_date <= to_date(A901.DATBI) 
then 1 else 0 end as is_active
from SAPHANADB.A901
inner join product on product.material_id = A901.MATNR
left join SAPHANADB.KONP on KONP.KNUMH = A901.KNUMH
left join location on location.node_id = A901.VKBUR
left join saphanadb.t685t on t685t.kschl = a901.kschl and t685t.kappl = a901.kappl
and t685t.spras = 'E' and t685t.mandt = 100
left join SAPHANADB.MARC on MARC.MATNR = product.material_id and MARC.WERKS = location.node_id
where A901.VKORG ='GAPL'
and A901.KSCHL = 'ZPR0'
and a901.spart = 10
and current_date >= to_date(A901.DATAB) and current_date <= to_date(A901.DATBI)
