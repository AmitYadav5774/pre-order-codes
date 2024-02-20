select
right(material_id,8) material_id,
plant_id,
sum(stock_quantity) total_sto_intransit_qty,
sum(case when sto_reason='106' then stock_quantity end) pre_order_sto_intransit


from
(Select
MATDOC.VBELN_IM delivery1,
MATDOC.VBELP_IM delivery_item,
LIPS.VGBEL sto_id,
EREV.RSCOD sto_reason,
case when length(MATDOC.CHARG_SID)<1 then MATDOC.CHARG_CID else MATDOC.CHARG_SID end batch_id,
MATDOC.MATBF material_id,
MATDOC.WERKS plant_id,

case when MATDOC.LBBSA_SID='06' then 'Transit' else MATDOC.LGORT_SID end storage_location,
case 
when MATDOC.DMBTR_STOCK!=0 then MATDOC.DMBTR_STOCK
when STO_IT.Transit_value>0 then ((STO_IT.Transit_value)/coalesce(STO_IT.transit_qty,1))*(MATDOC.STOCK_QTY)
when MATDOC.BWART in ('161','162') then MATDOC.DMBTR_STOCK
when MATDOC.SALK3>0 then (MATDOC.SALK3/MATDOC.LBKUM)*MATDOC.STOCK_QTY
end amount_lc_sign,

MATDOC.MENGE quantity,
MATDOC.STOCK_QTY stock_quantity,

case when STO_IT2.transit_qty2>0 then 'Transit STO' end Transit_tag


from SAPHANADB.MATDOC as MATDOC
inner join (select MATNR from SAPHANADB.MARA WHERE MARA.MTART LIKE 'Z%' and MARA.MANDT=100 and MARA.SPART='10') MARA
		on MARA.MATNR = MATDOC.MATNR		
------ joining storage location field LGORT
left join SAPHANADB.T001L on T001L.WERKS = MATDOC.WERKS and T001L.LGORT = MATDOC.LGORT_SID
----- tagging intransit STOs
left join 
(select VBELN_IM,VBELP_IM,sum(case when LBBSA_SID='06' then STOCK_QTY end) transit_qty,
sum(case when LBBSA_SID='06' then DMBTR_STOCK end) transit_value
from SAPHANADB.MATDOC where MATDOC.KZZUG='X' and MATDOC.BWART in ('641','642') group by VBELN_IM,VBELP_IM ) STO_IT 
	on STO_IT.VBELN_IM = MATDOC.VBELN_IM and STO_IT.VBELP_IM = MATDOC.VBELP_IM 

left join SAPHANADB.LIPS on LIPS.VBELN = MATDOC.VBELN_IM and LIPS.POSNR = MATDOC.VBELP_IM
left join SAPHANADB.EREV on EREV.EDOKN = LIPS.VGBEL and EREV.BSTYP='F' and EREV.REVNO='00000000'

left join 
(select VBELN_IM,VBELP_IM,sum(STOCK_QTY) transit_qty2
from SAPHANADB.MATDOC where LBBSA_SID='06' group by VBELN_IM,VBELP_IM ) STO_IT2
	on STO_IT2.VBELN_IM = MATDOC.VBELN_IM and STO_IT2.VBELP_IM = MATDOC.VBELP_IM 
	
where MATDOC.LBBSA_SID='06'
and MATDOC.KZZUG='X' and MATDOC.BWART in ('641','101','102','642')


) s
where Transit_tag='Transit STO'
group by 
right(material_id,8),plant_id