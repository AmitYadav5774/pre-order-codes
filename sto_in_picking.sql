select 
VBBE.WERKS plant_id,
right(VBBE.MATNR,8) material_id, sum(VBBE.OMENG) picking_start_sto_qty
from SAPHANADB.VBBE
inner join (select VBELN,VGBEL from SAPHANADB.LIPS group by VBELN,VGBEL)li
	on li.VBELN = vbbe.VBELN
inner join (Select EBELN from SAPHANADB.EKKO group by EBELN) po 
	on po.EBELN = li.VGBEL
group by VBBE.WERKS,right(VBBE.MATNR,8)

