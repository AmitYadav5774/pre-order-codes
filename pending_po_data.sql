WITH product AS (
SELECT
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
replace(MARA.ZZBRAND_DESC,',','') brand_name,
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

WHERE MARA.MTART LIKE 'Z%' and MARA.MANDT=100	),


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

po_grn_invoice_data as (

select
to_timestamp(concat(EKBE.CPUDT,EKBE.CPUTM)) last_updated_at,
EKKO.EBELN po_number,
EKKO.BUKRS company_code,
EKKO.STATU status_of_purchasing_doc,
to_date(EKKO.AEDAT) po_creation_date,
EKKO.ERNAM po_created_by,
name.FULLNAME po_created_by_name,
to_timestamp(EKKO.LASTCHANGEDATETIME) last_changed_on,
EKKO.LPONR last_item_number,
EREV.RSCOD po_reason_code,
T16CT.RSTXT po_creation_reason,


EKKO.LIFNR supplier,
LFA1.NAME1 supplier_name,
LFA1.ORT01 supplier_city,
LFA1.PSTLZ supplier_pincode,
LFA1.REGIO supplier_region,
LFA1.TELF1 supplier_phone,
LFA1.KTOKK supplier_group,
BUT000.BPKIND supplier_bpkind,
TB004T.TEXT40 supplier_type,
	
EKKO.ZTERM payment_terms,
EKKO.EKORG purchase_orgnization,
EKKO.EKGRP purchasing_group,
to_date(EKKO.BEDAT) po_document_date,
EKKO.PROCSTAT po_document_proc_state,

case when EKPO.LOEKZ='L' then 'Deleted' when EKPO.LOEKZ='S' then 'Blocked' 
else 'Active' end po_line_status,
EKPO.RETPO return_po_indicator,
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
product.base_weight_vol,
product.base_unit_to_convert,
product.product_domain,
product.business_segment,
product.business_category,


EKPO.BERID mrp_area,
EKPO.WERKS plant_id,
location.node_name,
location.state_name,
EKPO.LGORT storage_location,
EKPO.INFNR purchasing_info_record,
case when EKPO.RETPO='X' then -1*EKPO.MENGE else EKPO.MENGE end order_quantity,
EKPO.MEINS order_unit,
EKPO.BPRME order_price_unit,
--EKPO.LMEIN base_unit_of_measure,
 
---------- highlight case if MEINS!=BPRME-------------

EKPO.NETPR net_order_price,
EKPO.PEINH price_unit,
to_date(EKPO.PRDAT) price_date,
case when EKPO.RETPO='X' then -1*EKPO.NETWR else EKPO.NETWR end net_order_value,
case when EKPO.RETPO='X' then -1*EKPO.BRTWR else EKPO.BRTWR end gross_order_value,
case when EKPO.RETPO='X' then -1*EKPO.EFFWR else EKPO.EFFWR end effective_order_value,

EKPO.PRSDR print_price_indicator,
EKPO.WEPOS goods_receipt_indicator,
EKPO.REPOS invoice_receipt_indicator,
EKPO.WEBRE gr_based_invoice_verif_indicator,
EKPO.KO_PRCTR profit_center,
EKPO.ELIKZ delivery_completed_indicator,
EKPO.EREKZ final_invoice_indicator,
to_date(EKET.EINDT) expected_delivery_date,

case
when EKPO.ELIKZ='X' and EKPO.EREKZ='X' then 'GRN-Invoice Complete'
when EKPO.ELIKZ='X' then 'GRN Complete'
when EKPO.ELIKZ!='X' or EKPO.ELIKZ is null then 'GRN not complete'
when EKPO.EREKZ is null or EKPO.EREKZ!='X' then 'Invoice Pending'
end grn_closure_status,

EKBE.VGABE transaction_event_type,
DD07V.DDTEXT event_type_name,
EKBE.BELNR material_document,
EKBE.BUZEI material_Document_item,
EKBE.BEWTP po_history_Category,
po_his.BEWTL po_history_name,
EKBE.BWART movement_type,
mov_type.btext move_type_name,
to_date(EKBE.BUDAT) posting_date,
EKBE.SHKZG debit_Credit_indicator,
case when EKBE.SHKZG='H' then -1*EKBE.MENGE else EKBE.MENGE end movement_qty,
case when EKBE.SHKZG='H' then -1*EKBE.BPMNG else EKBE.BPMNG end movement_qty_opun,
case when EKBE.SHKZG='H' then -1*EKBE.DMBTR else EKBE.DMBTR end material_doc_amount


from SAPHANADB.EKKO as EKKO
inner join SAPHANADB.EKPO as EKPO 
	on EKKO.EBELN = EKPO.EBELN and EKPO.MANDT = EKKO.MANDT
left join SAPHANADB.EREV on EREV.EDOKN = EKKO.EBELN and EREV.BSTYP='F' and EREV.REVNO='00000000'
left join SAPHANADB.T16CT on T16CT.RSCOD = EREV.RSCOD

left join SAPHANADB.EKBE as EKBE 
	on EKBE.EBELN = EKPO.EBELN and EKBE.EBELP = EKPO.EBELP and EKBE.MANDT = EKPO.MANDT and EKBE.VGABE in ('1','2')
left join SAPHANADB.LFA1 	on LFA1.LIFNR = EKKO.LIFNR and LFA1.MANDT = EKKO.MANDT 
left join SAPHANADB.BUT000 	on LFA1.LIFNR = BUT000.PARTNER
left join SAPHANADB.TB004T on TB004T.BPKIND = BUT000.BPKIND and TB004T.SPRAS='E'
left join product as product 
	on product.material_id = EKPO.MATNR
left join (Select distinct location.node_id,node_name, state_name from location)location 
	on location.node_id = EKPO.WERKS
left join SAPHANADB.T163C as po_his 
	on po_his.mandt = ekbe.mandt and po_his.bewtp = ekbe.bewtp and po_his.spras='E'
left join SAPHANADB.T156HT as mov_type 
	on mov_type.SPRAS='E' and mov_type.BWART=ekbe.BWART and mov_type.MANDT='100'
left join SAPHANADB.DD07V 
	on DD07V.DOMNAME='VGABE' and DD07V.DDLANGUAGE='E' and DD07V.DOMVALUE_L = EKBE.VGABE
left join SAPHANADB.MCH1 
	on MCH1.MANDT='100' and MCH1.MATNR = EKBE.MATNR  and MCH1.CHARG = EKBE.CHARG and MCH1.LVORM=''
left join
(select USR21.BNAME,ADRP.NAME_TEXT FULLNAME from SAPHANADB.USR21
left join SAPHANADB.ADRP on USR21.PERSNUMBER = ADRP.PERSNUMBER
where USR21.MANDT='100' and ADRP.CLIENT='100' and DATE_TO='99991231')name on name.BNAME = EKKO.ERNAM
left join (select EBELN,EBELP,min(EINDT) EINDT from SAPHANADB.EKET group by EBELN,EBELP) EKET 
    on EKET.EBELN = EKKO.EBELN  and EKET.EBELP = EKPO.EBELP



where 
EKKO.MANDT='100'
and EKKO.BSTYP='F'
and EKKO.BSART='ZINP'

--and EKKO.EBELN ='1100000499'
)

select * from
(select 
state_name,plant_id,node_name,po_creation_date,po_number,
right(material_id,8) material_id,
expected_delivery_date,
material_name,business_category,business_segment,
supplier,base_weight_vol uom, base_unit_to_convert base_unit,
coalesce(round(sum(order_quantity)/count(po_number),0),0) ordered_qty,
coalesce(sum(case when EVENT_TYPE_NAME ='Goods Receipt' then movement_qty end),0) grn_qty,
coalesce(sum(material_doc_amount),0) grn_amount

from po_grn_invoice_data
where 
po_line_status='Active'
and grn_closure_status = 'GRN not complete'
and (EVENT_TYPE_NAME ='Goods Receipt' or EVENT_TYPE_NAME is null)
group by
state_name,plant_id,node_name,po_creation_date,po_number,
right(material_id,8),
expected_delivery_date,
material_name,business_category,business_segment,
supplier,base_weight_vol, base_unit_to_convert
) s where ordered_qty>grn_qty 
