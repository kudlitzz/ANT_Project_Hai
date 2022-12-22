
alter function [dbo].[ANT_F_HAI_RPT_ShipmentList] (@ord_ID int)
returns table
as
return
(
select *
	, sum(factQTY) over (partition by ord_ID) as TotalFactSum
from (
	select ord_ID
			, ord_Code
			, ort_Code + ' - ' + ort_Description as Type
			, odv_Address
			, oav6.oav_Value as Otpr
			, ord_Memo
			, convert(varchar(255), ord_ExpShipDate, 104) + ' ' + convert(varchar(5), ord_ExpShipDate, 108) as PribTsDateTime
			, isnull(oav11.oav_Value, '')  as TsInfo
			, isnull(oav12.oav_Value, '') as FioDriver
			, convert(varchar(255), getdate(), 104) + ' ' + convert(varchar(255), getdate(), 108) as PrintDate
			, (select stuff((select distinct ', ' + per_LastName
							from LV_Order(nolock)
									left join LV_OrderShipment(nolock) on ord_ID = ost_OrderID
									left join LV_OrderShipItem(nolock) on ost_ID = osi_OrderShipmentID
									left join LV_OrderShipItemStock(nolock) on osi_ID = oss_OrderShipItemID
									left join LV_TaskList(nolock) on  oss_TaskListID = tkl_ID
									left join LV_Users(nolock) on usr_ID = tkl_CreateUserID
									left join COM_Person(nolock) on per_ID = usr_PersonID
									where tkl_TransactionTypeID = 3
							for xml path('')),1,1,'')) as Users
			, oav16.oav_Value as VorotaOrd
			, cmpDep.cmp_ShortName as DepName
			, cmpCus.orc_Code + ' - ' + cmpCus.orc_FullName as Customer
			, cmpRec.orr_Code + ' - ' + cmpRec.orr_FullName as GruzopolName
			, prd_PrimaryCode
			, prd_SecondaryCode
			, prdl_ShortDescription
			, convert(numeric(10,4), sum(w.factQTY)) as factQTY
			, w.untl_ShortDescription as cuUnit
			, w.SSCC
			, ost_Code
	from LV_Order(nolocK)
			left join LV_OrderShipment(nolock) on ost_OrderID = ord_ID
			left join LV_OrderType(nolock) on ort_ID = ord_TypeID
			left join LV_OrderDelivery(nolock) on ord_ID = odv_OrderID
			left join LV_OrderShipItem(nolock) on osi_OrderShipmentID = ost_ID

			left join (
							select osi_ID as 'OrderShipItem'
								, unt_ID
								, isnull(Alt.altDesc, untl_ShortDescription) as untl_ShortDescription
								, ori_ProductID as oriProdID
								, sum(isnull(oss_Quantity, 0) * isnull(iuc_Conversion,1)) as 'factQTY'
								, all4.all_Value as Status
								, cu.itu_ID as cuItuID
								, stc_SSCC as SSCC
							from LV_OrderShipItem(nolock)
										inner join LV_OrderItem(nolock) on osi_OrderItemID = ori_ID
										inner join LV_OrderShipItemStock(nolock) on oss_OrderShipItemID = osi_ID
										inner join LV_Stock(nolock) on stk_Id = oss_StockID
										left join LV_StockContainer(nolock) on stc_ID = stk_ContainerID

										left join LV_StockAttributesValues sav4(nolock) on sav4.sav_AttributeID = 4 and sav4.sav_StockID = stk_ID
										left join LV_StockAttributesList sal4(nolocK) on sal4.sal_AttributeID = sav4.sav_AttributeID and sal4.sal_Code = sav4.sav_Value
										left join LV_AttributeListValue all4(nolock) on all4.all_StkAttrListID = sal4.sal_ID and all4.all_LanguageID = 4

										left join (select  stk_ID
															, stk_CUQuantity
															, cuUntl.untl_Description
															, isnull(stk_LengthQty, stk_WeightQty) as altQty
															, altUntl.untl_Description as altDesc
													from LV_Stock(nolock) 
														left join LV_ItemUnit(nolock) as cuItu on isnull(stk_LengthUnitID, stk_WeightUnitID) = itu_ID
														left join LV_Unit cuUnt(nolock) on cuUnt.unt_ID = cuItu.itu_UnitID
														left join LV_Unit altUnt(nolock) on altUnt.unt_ID = isnull(stk_LengthUnitID, stk_WeightUnitID)
														left join LV_UnitLang(nolock) as cuUntl on cuUnt.unt_ID = cuUntl.untl_UnitID and cuUntl.untl_LanguageID = 4
														left join LV_UnitLang(nolock) as altUntl on altUnt.unt_ID = altUntl.untl_UnitID and altUntl.untl_LanguageID = 4
													where itu_AlternateStockUnitLED = 1) as Alt on oss_StockID = Alt.stk_ID
								
										left join LV_ItemUnit(nolock) on itu_ID = oss_SUItemUnitID
										left join LV_ItemUnitConversion(nolock) on iuc_ConvertedUnitID = itu_UnitID and iuc_ProductID = ori_ProductID

										left join LV_ItemUnit cu(nolock) on cu.itu_ID = stk_CUItemUnitID
										left join LV_Unit(nolock) on unt_ID = cu.itu_UnitID
										left join LV_UnitLang(nolock) on untl_UnitID = unt_ID and untl_LanguageID = 4
							group by osi_ID
									, unt_ID
									, untl_ShortDescription
									, ori_ProductID
									, all4.all_Value 
									, cu.itu_ID
									, stc_SSCC 
									, Alt.altDesc

							union 
				
							select osi_ID
								, unt_ID
								, untl_ShortDescription
								, ori_ProductID
								, sum(isnull(osk_Quantity,0) * isnull(iuc_Conversion,1)) as 'Quantity'
								, all4.all_Value as Status
								, cu.itu_ID as cuItuID
								, shc_SSCC
							from LV_OrderShipItem(nolock)
									inner join LV_OrderItem(nolock) on osi_OrderItemID = ori_ID
									inner join LV_OrderShipItemShipStock(nolock) on osk_OrderShipItemID = osi_ID
									inner join LV_ShipStock(nolock) on shs_Id = osk_ShipStockID
									left join LV_ShipContainer(nolock) on shc_ID = shs_ContainerID
						
									left join LV_ShipStockAttrValues ssv4(nolock) on ssv4.ssv_AttributeID = 4 and ssv4.ssv_ShipStockID = shs_ID
									left join LV_StockAttributesList sal4(nolocK) on sal4.sal_AttributeID = ssv4.ssv_AttributeID and sal4.sal_Code = ssv4.ssv_Value
									left join LV_AttributeListValue all4(nolock) on all4.all_StkAttrListID = sal4.sal_ID and all4.all_LanguageID = 4

									left join LV_ItemUnit(nolock) on itu_ID = osk_SUItemUnitID
									left join LV_ItemUnitConversion(nolock) on iuc_ConvertedUnitID = itu_UnitID and iuc_ProductID = ori_ProductID
							
									left join LV_ItemUnit cu(nolock) on cu.itu_ID = shs_CUItemUnitID
									left join LV_Unit(nolock) on unt_ID = cu.itu_UnitID
									left join LV_UnitLang(nolock) on untl_UnitID = unt_ID and untl_LanguageID = 4
							group by osi_ID
								, unt_ID
								, untl_ShortDescription
								, ori_ProductID
								, all4.all_Value
								, cu.itu_ID
								, shc_SSCC
							) as w on w.OrderShipItem = osi_ID
		
			left join LV_Product (nolock) on prd_ID = oriProdID
			left join LV_ProductLang (nolock) on prdl_ProductID = prd_ID and prdl_LanguageID = 4

			left join LV_Depositor(nolock) on dep_ID = ord_DepositorID
			left join LV_Company cmpDep(nolock) on cmpDep.cmp_ID = dep_CompanyID

			left join LV_OrderCustomer cmpCus(nolock) on orc_OrderID = ord_ID
			left join LV_OrderReceiver cmpRec(nolock) on ord_ID = orr_OrderID

			left join LV_Company cmpGrp(nolock) on cmpGrp.cmp_ShortName = orr_ShortName

			
			left join (select ROW_NUMBER() over (partition by oav_OrderID order by oav_ID) as rowNum
					, oav_AttributeID
					, oav_OrderID
					, oav_Value 
					, oav_ID
				from LV_OrderAttributesValues(nolock) 
				where oav_AttributeID = 11) oav11 on oav11.oav_OrderID = ord_ID and oav11.rowNum = 1 --№ авто

			left join (select ROW_NUMBER() over (partition by oav_OrderID order by oav_ID) as rowNum
					, oav_AttributeID
					, oav_OrderID
					, oav_Value 
					, oav_ID
				from LV_OrderAttributesValues(nolock) 
				where oav_AttributeID = 12) oav12 on oav12.oav_OrderID = ord_ID and oav12.rowNum = 1 --ФИО водителя/Экспедитора

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
				where oav_AttributeID = 16) oav16 on oav16.oav_OrderID = ord_ID and oav16.rowNum = 1 -- ячейка отгрузки

			
	
	where ord_ID = @ord_ID
			and (ost_StatusID = 4 or ost_StatusID = 8)

	group by ort_Code + ' - ' + ort_Description
			, odv_Address
			, oav6.oav_Value
			, oav16.oav_Value
			, ord_Memo
			, convert(varchar(255), ord_ExpShipDate, 104) + ' ' + convert(varchar(5), ord_ExpShipDate, 108) 
			, isnull(oav11.oav_Value, '') 
			, isnull(oav12.oav_Value, ''), ord_ID
			, ord_Code
			, cmpdep.cmp_Code + ' - ' + cmpDep.cmp_FullName
			, cmpgrp.cmp_Code + ' - ' + cmpGrp.cmp_FullName
			, cmpCus.orc_Code + ' - ' + cmpCus.orc_FullName
			, cmpRec.orr_Code + ' - ' + cmpRec.orr_FullName
			, prd_PrimaryCode
			, prd_SecondaryCode
			, prdl_ShortDescription
			, w.untl_ShortDescription
			, w.SSCC
			, ost_Code
			, cmpDep.cmp_ShortName
			, cmpRec.orr_FullName
) main
		
)
