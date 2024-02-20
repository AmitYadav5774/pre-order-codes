select
right(c3.MATNR,8) material_id,
c3.WERKS plant_id,
SUM(VMENG) reserved_qty
from SAPHANADB.VBBE as c3
where c3.LGORT='SL01'
group by right(c3.MATNR,8),c3.WERKS