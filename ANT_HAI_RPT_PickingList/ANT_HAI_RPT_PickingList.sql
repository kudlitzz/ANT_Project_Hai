ALTER function [dbo].[ANT_F_HAI_RPT_PickingList] (@tkl_ID int)
returns table
as
return
(
select *
	, sum(tskCuQuantity) over (partition by tkl_ID order by tkl_ID) as TotalTskCuSum
	, sum(Qty) over (partition by tkl_ID order by tkl_ID) as TotalTskSum
from(
	select tkl_ID
			, tkl_Code
			, ord_ID
			, ord_Code
			, ord_Memo
			, ord_CustomerOrderCode
			, shp_ID
			, shp_Code
			, ost_Code
			--, iif(toLoc.loc_Code = 'A045', toLoc.loc_Code, oav17.oav_Value) as locNaznachenia
			, oav16.oav_Value as locNaznachenia
			, prd_PrimaryCode
			, prd_SecondaryCode
			, prdl_ShortDescription
			, sum(isnull(Alt.stk_CUQuantity, oa.tpt_Quantity)) as Qty
			, Untl.untl_ShortDescription as Unit
			, sum(isnull(Alt.altQty, oa.tpt_Quantity)) as tskCuQuantity
			, isnull(Alt.altDesc, cuUntl.untl_ShortDescription) as cuUnit
			, all4.all_Value as Status
			, fromLoc.loc_Code as fromLocationCode
			, tsk_SSCC
			, cmpDep.cmp_ShortName depName
			, cmpCus.orc_FullName cusName
			, cmpRec.orr_FullName recName
			, convert(varchar(20), ord_ExpShipDate, 104) + ' ' + convert(varchar(5), ord_ExpShipDate, 108) as PrintDate
			, ort_Code + ' - ' + ort_Description as Type
			, oav6.oav_Value as Otpr
			, oav7.oav_Value as Poluch
	from LV_TaskList(nolock)
			left join LV_Task(nolocK) on tsk_TaskListID = tkl_ID
			left join LV_OrderShipItemStock(nolock) on tsk_ID = oss_TaskID

			left join (select  stk_ID
								, stk_CUQuantity
								, cuUntl.untl_Description
								, isnull(stk_LengthQty, stk_WeightQty) as altQty
								, altUntl.untl_Description as altDesc
						from LV_Stock(nolock) 
							left join LV_ItemUnit(nolock) as cuItu on isnull(stk_LengthUnitID, stk_WeightUnitID) = itu_ID
							left join LV_Unit cuUnt with(nolock) on cuUnt.unt_ID = cuItu.itu_UnitID
							left join LV_Unit altUnt with(nolock) on altUnt.unt_ID = isnull(stk_LengthUnitID, stk_WeightUnitID)
							left join LV_UnitLang(nolock) as cuUntl on cuUnt.unt_ID = cuUntl.untl_UnitID and cuUntl.untl_LanguageID = 4
							left join LV_UnitLang(nolock) as altUntl on altUnt.unt_ID = altUntl.untl_UnitID and altUntl.untl_LanguageID = 4
						where itu_AlternateStockUnitLED = 1) as Alt on oss_StockID = Alt.stk_ID


			left join LV_OrderShipItem(nolock) on oss_OrderShipItemID = osi_ID 
			left join LV_OrderItem(nolock) on osi_OrderItemID = ori_ID
			left join LV_OrderShipment(nolocK) on osi_OrderShipmentID = ost_ID
			left join LV_Order(nolock) on ord_ID = ost_OrderID
			left join LV_OrderType(nolock) on ort_ID = ord_TypeID 

			left join (select ROW_NUMBER() over (partition by tpt_TaskID order by tpt_ID,tpt_taskid) as cnt
					, tpt_quantity
					, tpt_taskId
					, tpt_ItemUnitID 
					, tpt_ID
				from lv_taskpacktype(nolock) 
				where tpt_ParentID is null) oa on oa.tpt_TaskID = tsk_ID and oa.cnt = 1

			left join LV_Location toLoc(nolock) on toLoc.loc_ID = tsk_ToLocationID
			left join LV_Location fromLoc(nolock) on fromLoc.loc_ID = tsk_FromLocationID

			left join LV_Shipment(nolock) on shp_ID = ost_ShipmentID

			left join LV_Depositor(nolock) on dep_ID = ord_DepositorID
			left join LV_Company cmpDep(nolock) on cmpDep.cmp_ID = dep_CompanyID

			left join LV_OrderCustomer cmpCus(nolock) on orc_OrderID = ord_ID
			left join LV_OrderReceiver cmpRec(nolock) on ord_ID = orr_OrderID

			left join LV_OrderAttributesValues oav16(nolock) on oav16.oav_OrderID = ord_ID and oav16.oav_AttributeID = 16 --vorota
			
			left join LV_Product(nolock) on prd_ID = tsk_ProductID
			left join LV_ProductLang(nolocK) on prdl_LanguageID = 4 and prdl_ProductID = prd_ID

			left join LV_ItemUnit itu(nolock) on ori_ItemUnitID = itu.itu_ID
			left join LV_UnitLang Untl(nolock) on Untl.untl_UnitID = itu.itu_UnitID and Untl.untl_LanguageID = 4

			left join LV_ItemUnit cuItu(nolocK) on cuItu.itu_ID = oa.tpt_ItemUnitID
			left join LV_UnitLang cuUntl(nolock) on cuUntl.untl_UnitID = cuItu.itu_UnitID and cuUntl.untl_LanguageID = 4

			left join LV_TaskAttributesValues tav4(nolock) on tav4.tav_AttributeID = 4 and tav4.tav_TaskID = tsk_ID --status	
			left join LV_StockAttributesList sal4(nolocK) on sal4.sal_AttributeID = tav4.tav_AttributeID
																	and sal4.sal_Code = tav4.tav_Value
			left join LV_AttributeListValue all4(nolock) on all4.all_StkAttrListID = sal4.sal_ID and all4.all_LanguageID = 4

			left join (select ROW_NUMBER() over (partition by oav_OrderID order by oav_ID) as rowNum
					, oav_AttributeID
					, oav_OrderID
					, oav_Value 
					, oav_ID
				from LV_OrderAttributesValues(nolock) 
				where oav_AttributeID = 6) oav6 on oav6.oav_OrderID = ord_ID and oav6.rowNum = 1 -- склад отправитель

			left join (select ROW_NUMBER() over (partition by oav_OrderID order by oav_ID) as rowNum
					, oav_AttributeID
					, oav_OrderID
					, oav_Value 
					, oav_ID
				from LV_OrderAttributesValues(nolock) 
				where oav_AttributeID = 7) oav7 on oav7.oav_OrderID = ord_ID and oav7.rowNum = 1 -- склад получатель

	where tkl_ID = @tkl_ID
	group by tkl_ID
			, tkl_Code
			, ord_ID
			, ord_Code
			, ord_Memo
			, ord_CustomerOrderCode
			, shp_ID
			, shp_Code
			, ost_Code
			--, iif(toLoc.loc_Code = 'A045', toLoc.loc_Code, oav17.oav_Value)
			, oav16.oav_Value
			, prd_PrimaryCode
			, prd_SecondaryCode
			, prdl_ShortDescription
			, Untl.untl_ShortDescription
			, isnull(Alt.altDesc, cuUntl.untl_ShortDescription)
			, all4.all_Value 
			, fromLoc.loc_Code
			, tsk_SSCC
			, cmpDep.cmp_ShortName
			, cmpCus.orc_FullName
			, cmpRec.orr_FullName
			, ort_Code + ' - ' + ort_Description
			, oav6.oav_Value
			, oav7.oav_Value
			, convert(varchar(20), ord_ExpShipDate, 104) + ' ' + convert(varchar(5), ord_ExpShipDate, 108)
) main
)