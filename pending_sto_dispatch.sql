with product as (SELECT
MARA.MATNR material_id,
to_date(MARA.ERSDA) material_created_date,
MARA.ERNAM  created_by,
replace(MAKT.MAKTX,',','') AS material_name,
MARA.MEINS base_unit_of_measure,
ifnull(MARM.UMREN/MARM.UMREZ,1) base_weight_vol,
ifnull(MARM.MEINH,MARA.MEINS) base_unit_to_convert,
MARA.WRKST base_material_qty,
MARA.MTART material_type,
T134T.MTBEZ AS product_category,
MARA.MATKL material_group,
T023T.WGBEZ AS product_sub_category,
MARA.BISMT odoo_id,

MARA.TRAGR transportation_group,
MARA.SPART domain_id,
TSPAT.VTEXT AS product_domain,

MARA.XCHPF batch_management,
MARA.EXTWG external_material_group,
MARA.MTPOS_MARA general_item_cat_group,
MARA.ZZBRAND brand_id,
replace(MARA.ZZBRAND_DESC,',','') AS brand_name,
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

WHERE MARA.MTART LIKE 'Z%' and MARA.MANDT=100 and TSPAT.VTEXT='Input'
),


location as (SELECT
	state_data.state_id AS state_id,
	state_data.state_name AS state_name,
	T001W.WERKS AS node_id,
	T001W.NAME1 AS node_name,
	T001L.LGORT AS location_id,
	T001L.LGOBE AS location_name,
	T001L.SPART AS business_domain
	
FROM SAPHANADB.T001W
LEFT JOIN SAPHANADB.T001L ON T001W.WERKS=T001L.WERKS AND T001W.MANDT=T001L.MANDT
LEFT JOIN
(SELECT
	T005S.BLAND AS state_id,
	T005U.BEZEI AS state_name
FROM SAPHANADB.T005S
LEFT JOIN SAPHANADB.T005U ON T005U.LAND1=T005S.LAND1 AND T005U.BLAND=T005S.BLAND AND T005U.MANDT=T005S.MANDT
WHERE T005S.MANDT=100 AND T005S.LAND1='IN' AND T005U.SPRAS = 'E' AND LENGTH(LTRIM(T005U.BLAND,' +-.0123456789'))=0
) AS state_data ON T001W.REGIO=state_data.state_id
WHERE T001W.MANDT=100),

sto_data as 
(select
to_timestamp(concat(EKBE.CPUDT,EKBE.CPUTM)) last_updated_at,
EKKO.EBELN po_number,
EKKO.BUKRS company_code,
to_date(EKKO.AEDAT) po_creation_date,
EKKO.ERNAM po_created_by,
to_timestamp(EKKO.LASTCHANGEDATETIME) po_last_changed_on,
EKPO.AFNAM Requisitoner,

EKPO.EBELP po_item_id,
EKPO.UNIQUEID po_document_item,
to_date(EKPO.AEDAT) last_change_po_item,
EKPO.TXZ01 material_short_name,
EKPO.MATNR material_id,
right(EKPO.MATNR,8) material_id_gui, ----- define material details
product.odoo_id,
product.material_name,
product.brand_name,
product.product_category,
product.product_sub_category,
product.base_weight_vol uom,
product.base_unit_to_convert base_unit,
product.product_domain,

EKPO.MENGE order_quantity,
EKPO.MEINS order_unit,
EKPO.BPRME order_price_unit,
EKPO.LMEIN base_unit_of_measure,
EKPO.NETPR net_order_price,
EKPO.PEINH price_unit,
EKPO.NETWR net_order_value,
EKPO.LOEKZ sto_deletion_indicator,

EKKO.RESWK supplying_plant,
location2.node_name supplying_node,
location2.state_name supplying_state,


EKPO.WERKS receiving_plant,
location.node_name receiving_node,
location.state_name receiving_state,
EKKO.EKORG purchase_orgnization,
EKKO.EKGRP purchasing_group,
to_date(EKKO.BEDAT) po_document_date,
EKKO.PROCSTAT po_document_proc_state,

EKBE.VGABE transaction_event_type,
DD07V.DDTEXT event_type_name,
EKBE.BELNR material_document,
EKBE.BUZEI material_Document_item,
EKBE.BEWTP po_history_Category,
po_his.BEWTL po_history_name,
EKBE.BWART movement_type,
mov_type.btext move_type_name,
to_date(EKBE.BUDAT) move_posting_date,


---------- highlight case if MEINS!=BPRME-------------

EKBE.SHKZG debit_Credit_indicator,
case when EKBE.BWART in ('642','102') then -1*EKBE.MENGE else EKBE.MENGE end movement_qty,
case when EKBE.BWART in ('642','102') then -1*EKBE.BPMNG else EKBE.BPMNG end movement_qty_opun,
----- BAMNG is po order quantity
case when EKBE.VGABE='1' then EKBE.MENGE*EKBE2.DMBTR/EKBE2.MENGE 
when EKBE.BWART in ('642','102') then -1*EKBE.DMBTR else EKBE.DMBTR end material_doc_amount,

---- DMBTR is zero for goods receipt in STO so need to pick same as goods issues

to_date(EKBE.CPUDT) move_entry_date,
EKBE.CHARG move_batch,
to_date(EKBE.BLDAT) move_document_date,
EKBE.ERNAM move_created_by,
EKBE.VBELN_ST stock_picking_document,
EKBE.VBELP_ST stock_picking_document_item



from SAPHANADB.EKKO as EKKO
inner join SAPHANADB.EKPO as EKPO 
	on EKKO.EBELN = EKPO.EBELN and EKPO.MANDT = EKKO.MANDT
left join SAPHANADB.EKBE as EKBE 
	on EKBE.EBELN = EKPO.EBELN and EKBE.EBELP = EKPO.EBELP and EKBE.MANDT = EKPO.MANDT 
	and EKBE.VGABE in ('1','6')
left join product as product 
	on product.material_id = EKPO.MATNR
left join (Select distinct location.node_id,node_name, state_name from location)location 
	on location.node_id = EKPO.WERKS
left join (Select location.node_id,node_name, state_name from location group by  node_id,node_name,state_name)location2 
	on location2.node_id = EKKO.RESWK
left join SAPHANADB.T163C as po_his 
	on po_his.mandt = ekbe.mandt and po_his.bewtp = ekbe.bewtp and po_his.spras='E'
left join SAPHANADB.T156HT as mov_type 
	on mov_type.SPRAS='E' and mov_type.BWART=ekbe.BWART and mov_type.MANDT='100'
left join SAPHANADB.DD07V 
	on DD07V.DOMNAME='VGABE' and DD07V.DDLANGUAGE='E' and DD07V.DOMVALUE_L = EKBE.VGABE
left join (Select EKBE2.VBELN_ST,EKBE2.VBELP_ST,sum(EKBE2.DMBTR)as DMBTR, sum(EKBE2.MENGE) as MENGE from SAPHANADB.EKBE 
	as EKBE2 where EKBE2.VGABE='6' group by EKBE2.VBELN_ST,EKBE2.VBELP_ST)	as EKBE2 
	on EKBE2.VBELN_ST = EKBE.VBELN_ST and EKBE2.VBELP_ST = EKBE.VBELP_ST and EKBE.VGABE='1' 

where 
EKKO.MANDT='100'
and EKKO.BSTYP='F'
and EKKO.BSART='UB'
--and EKKO.EBELN='2100000002'
--and EKPO.MATNR='000000000012000451'
),


----------------------------- data from z reschedule table ----------------------------------------------------
canc_reason_data as (
select
zr.VBELN sales_del_doc,
zr.POSNR sales_del_doc_item,
coalesce(LIPS.VGBEL,zr.VBELN_SO) po_id,
coalesce(LIPS.VGPOS,zr.POSNR_SO) po_item_id,
zr.CHARG del_doc_batch,
zre.zreason_type,
zre.ZREASON_DESC,
zr.RESCHEDULE_QTY qty

from SAPHANADB.ZTSD_RESCHEDULE as zr
left join SAPHANADB.ZTSD_REASON as zre on zre.ZREASON_ID = zr.RESCHEDULE_REASON
left join SAPHANADB.LIPS on LIPS.VBELN = zr.VBELN and LIPS.POSNR = zr.POSNR
left join SAPHANADB.VBAK on VBAK.VBELN = zr.VBELN
where zr.CANC_RESHCD_IND in ('C') 
and zre.zreason_type in ('07','09','11')
and to_date(zr.ERDAT)>='2022-09-01'
and zr.RESCHEDULE_QTY>0
order by zr.VBELN_SO

)

select
s.po_number,s.po_item_id,s.receiving_plant,s.receiving_node,s.supplying_plant,s.supplying_node,
s.po_creation_date,s.material_id,s.material_name,s.uom,s.base_unit,
s.order_quantity,s.issued_qty,s.cancelled_qty,
s.order_quantity-s.issued_qty-s.cancelled_qty pending_dispatch

from (
select
s.po_number,s.po_item_id,s.receiving_plant,s.receiving_node,s.supplying_plant,s.supplying_node,
s.po_creation_date,
right(s.material_id,8) material_id,s.material_name,s.uom,s.base_unit,
avg(s.order_quantity) order_quantity,
coalesce(sum(case when s.transaction_event_type='6' then movement_qty end),0) issued_qty,
coalesce(canc.cancelled,0) cancelled_qty
from sto_data as s
left join (select po_id,po_item_id,sum(qty) cancelled from canc_reason_data group by po_id,po_item_id) canc 
	on canc.po_id = s.po_number and to_integer(canc.po_item_id) = to_integer(s.po_item_id)
where (sto_deletion_indicator!='L' or sto_deletion_indicator is null)
--where s.po_number='2100010361'
group by 
s.po_number,s.po_item_id,s.receiving_plant,s.receiving_node,s.supplying_plant,s.supplying_node,
s.po_creation_date,right(s.material_id,8),s.material_name,s.uom,s.base_unit,coalesce(canc.cancelled,0)
) s
where (s.order_quantity-s.issued_qty-s.cancelled_qty)>0