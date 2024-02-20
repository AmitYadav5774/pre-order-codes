select 
zi.werks "plant_id",
right(zi.matnr,8) material_id,
sum(clabs) "net_inventory"
from SAPHANADB.ZMMT_SCH_INV as zi
where zi.lgort='SL01'
and zi.clabs>0
group by zi.werks,right(zi.matnr,8) 