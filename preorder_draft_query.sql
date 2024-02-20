select 
sale_saleorder.id as order_id,
sale_saleorder.sap_order_id,
sp.config_id,cnfg.type_id config_type,
sale_saleorder.order_date at time zone 'Asia/Kolkata' as order_date,
cancelled_at at time zone 'Asia/Kolkata' as cancelled_at,
confirmed_at at time zone 'Asia/Kolkata' as confirmed_at,
sale_saleorder.sales_channel,
case when common_state.name = 'Andaman and Nicobar' then 'Andaman and Nico.Is.'
when common_state.name = 'Chattisgarh' then 'Chhattisgarh' 
when common_state.name = 'Dadra and Nagar Haveli' then 'Dadra and Nagar Hav.' 
else common_state.name end as state,
common_salesoffice.name as node,
common_salesoffice.sales_office_id as node_id,
CASE
when common_salesoffice.sales_office_id::integer in (1011,1035,1001,1009,1015,1004) then 'Bihar Cluster A'
when common_salesoffice.sales_office_id::integer in (1008,1026,1010,1019) then 'Bihar Cluster B'
when common_salesoffice.sales_office_id::integer in (1005,1032,1040,1014) then 'Bihar Cluster C'
when common_salesoffice.sales_office_id::integer in (1031,1002,1017) then 'Bihar Cluster D'
when common_salesoffice.sales_office_id::integer in (1018,1037,1012,1003) then 'Bihar Cluster E'
when common_salesoffice.sales_office_id::integer in (1027,1021,1022,1036) then 'Bihar Cluster F'
when common_salesoffice.sales_office_id::integer in (922,905,924) then 'UP Cluster A'
when common_salesoffice.sales_office_id::integer in (904,909,910,915) then 'UP Cluster B'
when common_salesoffice.sales_office_id::integer in (907,908,911,930) then 'UP Cluster C'
when common_salesoffice.sales_office_id::integer in (901,902,923,914,925) then 'UP Cluster D'
when common_salesoffice.sales_office_id::integer in (928,919,906,918,912) then 'UP Cluster E'
when common_salesoffice.sales_office_id::integer in (931,913,920,921) then 'UP Cluster F'
end node_cluster_ib,
sale_saleorderline.product_id,sale_saleorder.status,
case when cashback_unit_price is null then price_unit else cashback_unit_price end  as price_unit,
price_unit - cashback_unit_price as cashback,
quantity,
concat(eta_start_date,' - ',eta_end_date) as estimated_delivery_range,
case when eta_days_start is null then eta_start_date - 
date(sale_saleorder.order_date at time zone 'Asia/Kolkata') else eta_days_start end eta_days_start,

case when eta_days_start is null then eta_end_date - 
date(sale_saleorder.order_date at time zone 'Asia/Kolkata') else eta_days_end end eta_days_end,
end_date as pre_order_end_date
--smr.reason_description as cancellation_reason,
--smrt.short_description as cancellation_type
from sale_saleorder
join sale_saleorderline on sale_saleorder.id = sale_saleorderline.sale_order_id
left join sale_preorderconfigsaleordermap sp on sp.sale_order_id = sale_saleorder.id
left join common_salesoffice on common_salesoffice.id = sale_saleorder.sales_office_id
left join common_state on common_state.id = common_salesoffice.state_id
left join (select id,
min( case when status = 'cancelled' then history_date end) as cancelled_at,
min( case when status = 'confirmed' then history_date end) as confirmed_at
from sale_historicalsaleorder
group by id) s_date on s_date.id = sale_saleorder.id
left join sale_preorderconfigsaleordermap spmp on spmp.sale_order_id = sale_saleorder.id
left join sale_preorderconfig cnfg on cnfg.id = spmp.config_id
--left join stock_management_reasons smr on smr.id = sale_saleorder.cancellation_reason_id
--left join stock_management_reasontype smrt on smrt.reason_type = smr.reason_type

where sale_saleorder.sap_order_id is null
and sale_saleorder.status='draft'
and sale_saleorder.type='preorder'
and cnfg.type_id in ('CF1','CF2','ACH','MNL')