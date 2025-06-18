with cte1 as(
select  
distinct VisitFID
, LicenseKey
, Carrier
, Provider
, ReferringProvider
, Facility
, ChartNum
, PatientName
, DOB
, SubscriberID

from [All_Licensekey].[dbo].[CPT (All) Extract]
--where VisitFID = '19633992'
),
cte2 as(
select 
 VisitFID
  , Accession
 ,CONCAT('''',STRING_AGG(REPLACE(cast(ChargeDetail_UID as varchar), ' ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "ChargeDetail_UID"
 ,CONCAT('''',STRING_AGG(REPLACE(cast(FinancialClass as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "FinancialClass"
 ,max(BeginDOS) BeginDOS
 ,max(EndDOS) EndDOS
 ,max(DOE) DOE
 ,min(LastBillDate) LastBillDate
 ,CONCAT('''',STRING_AGG(REPLACE(cast(BillOccurance as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as BillOccurance
 ,CONCAT('''',STRING_AGG(REPLACE(cast(EntryUser as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as EntryUser
 ,CONCAT('''',STRING_AGG(REPLACE(cast([Procedure] as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "Procedure"
 ,CONCAT('''',STRING_AGG(REPLACE(cast([Modifiers] as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "Modifiers"
 ,CONCAT('''',STRING_AGG(REPLACE(cast([Primary Dx (ICD-10)] as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "Primary Dx (ICD-10)"
 ,CONCAT('''',STRING_AGG(REPLACE(cast(Code as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "Code"
 ,CONCAT('''',STRING_AGG(REPLACE(cast(Reason as varchar), '   ', ''), ',') WITHIN GROUP (ORDER BY [ChargeDetail_UID] ASC)) as "Reason"

,sum(TotalCharge) TotalCharge
,sum(AllowedAmount) AllowedAmount
,sum(CarrierPayment) CarrierPayment
,sum(CarrierWO) CarrierWO
,sum(PatientPayment) PatientPayment
,sum(PatientWO) PatientWO
,sum(CarrierBalance) CarrierBalance
,sum(PatientBalance) PatientBalance
,sum(TotalBalance) TotalBalance

from [All_Licensekey].[dbo].[CPT (All) Extract]
--where VisitFID = '19633992'
group by VisitFID, Accession
)

select  

cte1.VisitFID
,cte1.LicenseKey
,cte1.Carrier
,cte1.Provider
,cte1.ReferringProvider
,cte1.Facility
,cte1.ChartNum
,cte1.PatientName
,cte1.DOB
,cte1.SubscriberID

,cte2.Accession
,cte2.ChargeDetail_UID
,cte2.FinancialClass
,cte2.BeginDOS
,cte2.EndDOS
,cte2.DOE
,cte2.LastBillDate
,cte2.BillOccurance
,cte2.EntryUser
,cte2.[Procedure]
,cte2.Modifiers
,cte2.[Primary Dx (ICD-10)]
,cte2.Code
,cte2.Reason

,cte2.TotalCharge
,cte2.AllowedAmount
,cte2.CarrierPayment
,cte2.CarrierWO
,cte2.PatientPayment
,cte2.PatientWO
,cte2.CarrierBalance
,cte2.PatientBalance
,cte2.TotalBalance

into [All_Licensekey].[dbo].[Visit (All) Extract]
from cte1
left join cte2 on cte1.VisitFID = cte2.VisitFID
--left join [All_Licensekey].[dbo].[customtabdata] ctd on ctd.FIDValue = cte1.VisitFID
--where cte1.VisitFID = 19633992

