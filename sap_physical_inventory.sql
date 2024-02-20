select
right(MCHB.MATNR,8) "material_id",
MCHB.WERKS "plant_id",
sum(case when MCHB.LGORT='SL01' then MCHB.CLABS end) "sellable_stock"
from SAPHANADB.NSDM_V_MCHB as MCHB
where MCHB.CLABS>0 
and MCHB.LGORT in ('SL01','CL01')
group by 
right(MCHB.MATNR,8),MCHB.WERKS