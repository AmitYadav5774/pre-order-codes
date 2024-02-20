select 
pcc.*, css.sales_office_id sap_node_id,
ip.supply_plant_name node_name, ip.supply_plant_cluster node_cluster, ip.supply_plant_state node_state

from dev.s3_tables.preorder_config_changes pcc
left join input_backend_db.public.common_salesoffice as css on css.id = pcc.sales_office
left join (select supply_plant,supply_plant_name,supply_plant_cluster,supply_plant_state 
            from dev.s3_tables.input_plant_wh_info as ip
            group by supply_plant,supply_plant_name,supply_plant_cluster,supply_plant_state) ip 
                    on cast(ip.supply_plant as integer)= cast(css.sales_office_id as integer) 
                    
where css.state_id not in (20)


