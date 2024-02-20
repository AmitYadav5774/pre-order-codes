with product as (SELECT
MARA.MATNR material_id,
to_date(MARA.ERSDA) material_created_date,
MARA.ERNAM  created_by,
MAKT.MAKTX AS material_name,
MARA.MEINS base_unit_of_measure,
ifnull(MARM.UMREN/MARM.UMREZ,1) base_weight_vol,
ifnull(MARM.MEINH,MARA.MEINS) base_unit_to_convert,
MARA.WRKST base_material_qty,
MARA.MTART material_type,
T134T.MTBEZ AS product_category,
MARA.MATKL material_group,
T023T.WGBEZ AS product_sub_category,
MARA.BISMT odoo_product_id,

MARA.TRAGR transportation_group,
MARA.SPART domain_id,
TSPAT.VTEXT AS product_domain,

MARA.XCHPF batch_management,
MARA.EXTWG external_material_group,
MARA.MTPOS_MARA general_item_cat_group,
MARA.ZZBRAND brand_id,
MARA.ZZBRAND_DESC brand_name,
MARC.STEUC HSN,
MARA.ZZMAT_SUBGRP_ID subgroup_id,
MARA.ZZMAT_SUBGRP_NAME subgroup_name,
A4AR.KNUMH Condition_record,
KONP.KBETR*2/1000 tax_rate,

MARA.ZZMAT_BUSEG_NAME business_segment,
MARA.ZZMAT_BISFIN_NAME business_category



FROM SAPHANADB.MARA
INNER JOIN SAPHANADB.MAKT ON MARA.MATNR=MAKT.MATNR AND MAKT.SPRAS='E'
LEFT JOIN SAPHANADB.T134T ON MARA.MTART=T134T.MTART AND T134T.SPRAS='E' AND T134T.MANDT=100
LEFT JOIN SAPHANADB.T023T ON MARA.MATKL=T023T.MATKL AND T023T.SPRAS='E' AND T023T.MANDT=100
LEFT JOIN SAPHANADB.TSPAT ON MARA.SPART=TSPAT.SPART AND TSPAT.SPRAS='E' AND TSPAT.MANDT=100
LEFT JOIN SAPHANADB.MARM on MARM.MEINH in ('KG','L') and MARA.MATNR = MARM.MATNR
left join (select distinct MATNR,STEUC from SAPHANADB.MARC)MARC on MARC.MATNR = MARA.MATNR
left join
(select A4AR.* from
(select A4AR.*,row_number() over (partition by STEUC order by TAXM1 desc) ranking
from SAPHANADB.A4AR where A4AR.KSCHL = 'JOCG' and A4AR.KAPPL='V' and to_date(A4AR.DATBI)>current_date)A4AR where ranking=1)A4AR
    on A4AR.STEUC = MARC.STEUC  and A4AR.ranking=1
left join SAPHANADB.KONP on KONP.KNUMH = A4AR.KNUMH and KONP.KAPPL='V' and A4AR.KSCHL = 'JOCG'

WHERE MARA.MTART LIKE 'Z%' and MARA.MANDT=100 and MARA.SPART='10'
),
------------------------------------ sale rate ----------------------------------------------------------------------------------
sale_rate as 
(Select
right(MATDOC.MATBF,8) "material_id",
MATDOC.WERKS "plant_id",

sum(case when days_between(to_date(MATDOC.BUDAT),add_days(current_date,-1))<=7 and MATDOC.BWART in ('601','602','653','654') then MATDOC.STOCK_QTY*-1 end) "days7_nmove_qty",
sum(case when days_between(to_date(MATDOC.BUDAT),add_days(current_date,-1))<=15 and MATDOC.BWART in ('601','602','653','654') then MATDOC.STOCK_QTY*-1 end) "days15_nmove_qty",
sum(case when days_between(to_date(MATDOC.BUDAT),add_days(current_date,-1))<=30 and MATDOC.BWART in ('601','602','653','654') then MATDOC.STOCK_QTY*-1 end) "days30_nmove_qty",
sum(case when days_between(to_date(MATDOC.BUDAT),add_days(current_date,-1))<=60 and MATDOC.BWART in ('601','602','653','654') then MATDOC.STOCK_QTY*-1 end) "days60_nmove_qty",

max(case when MATDOC.BWART in ('601','602') then to_date(MATDOC.BUDAT) end) last_invoice_date,
max(case when MATDOC.BWART in ('101','102') and MATDOC.KZZUG!='X' then to_date(MATDOC.BUDAT) end) last_grn_date


from SAPHANADB.MATDOC as MATDOC
inner join product on product.material_id = MATDOC.MATBF
where MATDOC.BWART in ('601','602','653','654','101','102') and MATDOC.LBBSA_SID='01'
and to_date(MATDOC.BUDAT)<current_date
group by MATDOC.MATBF , MATDOC.WERKS
)

select 
*
from sale_rate
where "days7_nmove_qty">0