ALTER function [dbo].[ANT_F_HAI_RPT_ReceiptList] (@rct_ID int)
returns table
as
return
(

select 
	rct_ID
	, rct_Code
	, rct_Memo
	, convert(varchar(255), getdate(), 104) as PrintDate
	, depCmp.cmp_ShortName as DepName
	, supCmp.cmp_ShortName as SuplName
	, rtt_Description
	, convert(varchar(255), rct_ExpectedDate, 104) as ExpDate
	--, rav8.rav_Value as MarkaTs
	--, rav9.rav_Value as GosNumTs
	, rav11.rav_Value as MarkaGosNumTs
	, rav12.rav_Value as Driver
	, prd_PrimaryCode 
	, prd_SecondaryCode
	, prdl_ShortDescription
	, sum(rci_ExpQuantity) * isnull(iuc_Conversion, 1) as ExpQuantity
	, loc_Code
	, pav1.pav_Value as PrdNEW
	, rav7.rav_Value as PlompNum
from LV_Receipt with(nolock)
		left join LV_Depositor with(nolock) on dep_ID = rct_DepositorID
		left join LV_Company depCmp with(nolock) on depCmp.cmp_ID = dep_CompanyID

		left join LV_Supplier with(nolock) on spl_ID = rct_SupplierID
		left join LV_Company supCmp with(nolock) on supCmp.cmp_ID = spl_CompanyID

		left join LV_ReceiptType with(nolock) on rtt_ID = rct_TypeID

		left join LV_ReceiptAttributesValues rav11 with(nolock) on rav11.rav_AttributeID = 11 and rav11.rav_ReceiptID = rct_ID --№ Авто 
		left join LV_ReceiptAttributesValues rav7 with(nolock) on rav7.rav_AttributeID = 7 and rav7.rav_ReceiptID = rct_ID --№ пломбы
		left join LV_ReceiptAttributesValues rav12 with(nolock) on rav12.rav_AttributeID = 12 and rav12.rav_ReceiptID = rct_ID --Водитель

		left join LV_ReceiptItem with(nolock) on rci_ReceiptID = rct_ID
		left join LV_Location with(nolock) on loc_ID = rci_LocationID
		left join LV_Product with(nolock) on prd_ID = rci_ProductID
		left join LV_ProductLang with(nolocK) on prdl_ProductID = prd_ID and prdl_LanguageID = 4

		left join LV_ProductAttributesValues pav1 with(nolock) on pav1.pav_ProductID = prd_ID and pav1.pav_AttributeID = 1 --Новинка

		left join LV_ItemUnit with(nolock) on itu_ID = rci_InputItemUnitID
		left join LV_ItemUnitConversion with(nolock) on iuc_ConvertedUnitID = itu_UnitID and iuc_ProductID = itu_ProductID
where rct_ID = @rct_ID
group by
	rct_ID
	, rct_Code
	, rct_Memo
	, depCmp.cmp_ShortName 
	, supCmp.cmp_ShortName 
	, rtt_Description
	, convert(varchar(255), rct_ExpectedDate, 104) 
	--, rav8.rav_Value 
	--, rav9.rav_Value 
	, rav11.rav_Value
	, rav12.rav_Value 
	, prd_PrimaryCode
	, prd_SecondaryCode
	, prdl_ShortDescription
	, loc_Code
	, pav1.pav_Value 
	, isnull(iuc_Conversion, 1)
	, rav7.rav_Value
)
