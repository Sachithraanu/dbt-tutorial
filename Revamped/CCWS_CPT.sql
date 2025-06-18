with cte1 as(
	select * from (
	select distinct(Appointment_UID), PatientFID, left(InsuranceBillingSequence,1 ) InsuranceBillingSequence,LicenseKey,ProfileFID
	from [AdvanceMD_New].[dbo].[vw_ODBC_appts_Appointments] where InsuranceBillingSequence like ('%,%')
	union
	select distinct(Appointment_UID), PatientFID, InsuranceBillingSequence,LicenseKey,ProfileFID
	from [AdvanceMD_New].[dbo].[vw_ODBC_appts_Appointments] where InsuranceBillingSequence not like ('%,%')
	) p where p.Appointment_UID <> 1
	)

select 

pgp.LicenseKey
,cd.ChargeDetail_UID
,CONCAT(ch.CarrierName,' - ',ch.CarrierCode) as 'Carrier'
,CONCAT(fc.FinancialClassCode,' - ',fc.Description) as "FinancialClass"
,pgpf.Description as "Provider"
,ap.FullName ReferringProvider
,ap.FacilityName Facility
,cte7.ChartNumber ChartNum
,cte7.LastName+','+cte7.FirstName as PatientName
--,CAST(cte7.DOB as date) DOB
,CONVERT(varchar,cte7.DOB,101) as DOB
,b.SubscriberIDNumber 'SubscriberID'
,cd.VisitFID
,ccfvv.TextValue Accession
--,CAST(cd.begindateofservice as date) BeginDOS
,CONVERT(varchar,cd.begindateofservice,101) as BeginDOS
,CONVERT(varchar,cd.EndDateOfService,101) as EndDOS
--,CAST(cd.PostingDate as date) DOE
,CONVERT(varchar,cd.PostingDate,101) as DOE
--,case when CAST(cbh.CarrierBilledDate as date) is null  then CAST( GETDATE() AS Date ) else CAST(cbh.CarrierBilledDate as date) end as 'LastBillDate'
,CONVERT(varchar,cbh.CarrierBilledDate,101) as LastBillDate
,cbh.ClaimID BillOccurance
,cd.CreatedBy EntryUser
,CONCAT(cte10.ChargeCode,' - ',cte10.InsuranceDescription) as "Procedure"
,cd.Modifiers
,CONCAT(ds.DiagnosisCode,' - ',ds.InsuranceDescription) 'Primary Dx (ICD-10)'
--,fscd.Amount
--,fscd.Medicare_fee
, cd.TotalPortion TotalCharge
, cd.AllowedAmount
,case
	when pt.CarrierPayment = 0 then 0
	when pt.CarrierPayment is null then 0
	else
	pt.CarrierPayment
end as CarrierPayment
,case when wod.WriteOffAmount is null then 0 else wod.WriteOffAmount end as 'CarrierWO'
,case
	when pp.PatientPayment is null then 0
	else pp.PatientPayment
end	as 'PatientPayment'
,case when woa.WriteOffAmount is null then 0 else woa.WriteOffAmount end as 'PatientWO'
,cd.InsuranceBalance as 'CarrierBalance'
,cd.PatientBalance as 'PatientBalance'
,cd.InsuranceBalance+cd.PatientBalance as TotalBalance
,pr.Code
,pr.Reason
,CONVERT(varchar,pdr.Denail_Posted_Date,101) as Denail_Posted_Date
,ctd.[Collection Status]
,ctd.[Method Of Inquiry]
,ctd.[Follow Up Type]
,ctd.[ICN Number]
,ctd.[Denial Reason]
,ctd.[Corrective Action]
,ctd.[Note]
,ctd.[Worked Date]
,ctd.[CreatedAt]
,ctd.[CreatedBy]
,ctd.[ChangedAt]
,ctd.[ChangedBy]

--,mf.Medicare_Reimbursement_Rate as 'Medicare Fee'
----,fscd.Amount
----,fscd.Medicare_fee  --Medicare Fee in future
--,mf.Medicare_Reimbursement_Rate * 3 as 'MCR*3'


into [All_Licensekey].[dbo].[CPT (All) Extract]
from [AdvanceMD_New].[dbo].[vw_ODBC_actv_ChargeDetail] cd

----left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_actv_PaymentDetails] where ChargeDetailFID <> 1) pd on pd.ChargeDetailFID = cd.ChargeDetail_UID and pd.LicenseKey = cd.LicenseKey

left join
	[AdvanceMD_New].[dbo].[vw_ODBC_mf_PGPractice] pgp on pgp.LicenseKey = cd.LicenseKey

left join
(select * from [AdvanceMD_New].[dbo].[vw_ODBC_actv_CustomClaimFieldValueVisit] 
	where 
	CONCAT(VisitFID,' - ',LicenseKey,' - ',TextValue) 
	not in 
	('19582689 - 156942 - 1154'
	,'19582692 - 156942 - 1263'
	,'19582693 - 156942 - 1268'
	,'19582694 - 156942 - 1155'
	,'19582695 - 156942 - 1261'
	,'19582696 - 156942 - 1212'
	,'19582697 - 156942 - 1209'
	,'19582698 - 156942 - 1213'
	,'19582699 - 156942 - 1214'
	,'19582729 - 156942 - 1165'
	,'19582730 - 156942 - 1171'
	,'19582731 - 156942 - 1210'
	,'19582732 - 156942 - 1150'
	,'19582733 - 156942 - 1269'
	,'19582734 - 156942 - 1156'
	,'19582735 - 156942 - 1149'
	,'19582743 - 156942 - 1262'
	,'19582744 - 156942 - 1266'
	,'19582746 - 156942 - 1264'
	,'19582765 - 156942 - 1166') 
	AND CONCAT(LicenseKey,' - ',ClaimFieldFID) 
	not in ('152046 - 38485')

	--ClaimFieldFID not in (38485,38580)
	)ccfvv
	on cd.VisitFID = ccfvv.VisitFID and ccfvv.LicenseKey = cd.LicenseKey

left join cte1 a on cd.VisitFID = a.Appointment_UID and cd.LicenseKey = a.LicenseKey

left join [AdvanceMD_New].[dbo].[vw_ODBC_pt_InsuranceCoverages] b  on concat(a.PatientFID,'_',a.InsuranceBillingSequence,'_',a.LicenseKey)=concat(b.PatientFID,'_',b.SequenceNumber,'_',b.LicenseKey)

left join (
	select distinct(Carrier_UID), CarrierCode, CarrierName,LicenseKey from [AdvanceMD_New].[dbo].[vw_ODBC_mf_Carriers]) ch
on b.CarrierFID = ch.Carrier_UID and b.LicenseKey = ch.LicenseKey

left join [AdvanceMD_New].[dbo].[vw_ODBC_mf_PGProfiles] pgpf on a.ProfileFID = pgpf.Profile_UID and a.LicenseKey = pgpf.LicenseKey
	

left join 
	(select 
		distinct(fc.FinancialClass_UID) as FinancialClass_UID,
		fc.LicenseKey,
		fc.FinancialClassCode,
		fc.Description
	from  [AdvanceMD_New].[dbo].[vw_ODBC_mf_FinancialClasses] fc  where FinancialClass_UID <> 1 
	--and LicenseKey in (155931, 151406, 155974, 151341)
	
	) fc
on CONCAT(cd.FinancialClassFID,' - ',cd.LicenseKey) = CONCAT(fc.FinancialClass_UID,' - ',fc.LicenseKey)

left join
	(select Appointment_UID, cte3.LicenseKey,cte5.NPINumber,
		cte5.FullName,
		cte6.Facilityname
		from [AdvanceMD_New].[dbo].[vw_ODBC_appts_Appointments] cte3
		left join [AdvanceMD_New].[dbo].[vw_ODBC_pt_ReferralPlans] cte4
		on CONCAT(cte3.ReferralPlanFID,' - ',cte3.LicenseKey) = CONCAT(cte4.ReferralPlan_UID,' - ',cte4.LicenseKey)
		left join [AdvanceMD_New].[dbo].[vw_ODBC_mf_ReferringProviders] cte5
		on CONCAT(cte4.ByReferringProviderFID,' - ',cte4.LicenseKey) = CONCAT(cte5.ReferringProvider_UID,' - ',cte5.LicenseKey)	
		left join [AdvanceMD_New].[dbo].[vw_ODBC_mf_Facilities] cte6
		on CONCAT(cte3.FacilityFID,' - ',cte3.LicenseKey) = CONCAT(cte6.Facility_UID,' - ',cte6.LicenseKey)) ap
		on CONCAT(cd.VisitFID,' - ',cd.LicenseKey) = CONCAT(ap.Appointment_UID,' - ',ap.LicenseKey)

left join 
	[AdvanceMD_New].[dbo].[vw_ODBC_pt_PatientInfo] cte7 
	on CONCAT(cte7.Patient_UID,' - ',cte7.LicenseKey) = CONCAT(cd.PatientFID,' - ',cd.LicenseKey)


left join
	[AdvanceMD_New].[dbo].[vw_ODBC_actv_CarrierBillingHistory] cbh
	on cd.CarrierBillingHistoryFID = cbh.CarrierBillingHistory_UID and cd.LicenseKey = cbh.LicenseKey

left join
	(	select  distinct(ChargeCode_UID), LicenseKey,ChargeCode ,InsuranceDescription, LEFT(ChargeCode, 5) Refine_ChargeCode
	from [AdvanceMD_New].[dbo].[vw_ODBC_mf_ProcChargeCodes] 
	--where LicenseKey = 159395
	 --and ChargeCode not like ('#%')
	 ) cte10 on CONCAT(cte10.ChargeCode_UID,' - ',cte10.LicenseKey) = CONCAT(cd.ChargeCodeFID,' - ',cd.LicenseKey)

--left join
--	(select Amount/3 as 'Medicare_fee', * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_FeeScheduleChargeCode] where ChargeCodeFID <> 1) fscd
--		on fscd.ChargeCodeFID = cd.ChargeCodeFID and fscd.LicenseKey = cd.LicenseKey   --Medicare Fee in future
left join
	(select cte8.ChargeDetailFID,
		--sum(cte8.PaymentAmount)  as CarrierPayment 
		case when sum(cte8.PaymentAmount) < 0 then 0 else sum(cte8.PaymentAmount) end  as CarrierPayment
		from [AdvanceMD_New].[dbo].[vw_ODBC_actv_PaymentDetails] cte8
	left join [AdvanceMD_New].[dbo].[vw_ODBC_actv_Payments] cte9 on cte8.PaymentFID = cte9.Payment_UID
		where cte9.PaymentSource in (2, 4 , 6) 
		--and cte8.PaymentAmount > 0 
		--and cte8.ChargeDetailFID in (539,540,541)
			group by cte8.ChargeDetailFID) pt
	on cd.ChargeDetail_UID = pt.ChargeDetailFID

left join
	(select pd3.ChargeDetailFID,
		case when sum(pd3.PaymentAmount) < 0 then 0 else sum(pd3.PaymentAmount) end  as PatientPayment
		from [AdvanceMD_New].[dbo].[vw_ODBC_actv_PaymentDetails] pd3
	left join [AdvanceMD_New].[dbo].[vw_ODBC_actv_Payments] p3 on pd3.PaymentFID = p3.Payment_UID
		where p3.PaymentSource = 1 and pd3.ChargeDetailFID != 1
		--and cte8.PaymentAmount > 0 
		--and cte8.ChargeDetailFID in (539,540,541)
			group by pd3.ChargeDetailFID) pp
	on cd.ChargeDetail_UID = pp.ChargeDetailFID

left join 
(select wd.ChargeDetailFID, SUM(wd.WriteOffAmount) as 'WriteOffAmount',wd.LicenseKey from [AdvanceMD_New].[dbo].[vw_ODBC_actv_WriteOffDetails] wd
			where wd.WriteOffDetail_UID <> 1 and wd.WriteOffFID in (select WriteOffs_UID from [AdvanceMD_New].[dbo].[vw_ODBC_actv_Writeoffs] where WriteoffSource = 2 and Void = 0)
			group by wd.ChargeDetailFID, wd.LicenseKey) wod
			on cd.LicenseKey = wod.LicenseKey and cd.ChargeDetail_UID = wod.ChargeDetailFID

--left join
--	(
--	select  ChargeDetailFID, SUM(WriteOffAmount) as 'WriteOffAmount',LicenseKey
--from [AdvanceMD_New].[dbo].[vw_ODBC_actv_WriteOffDetails] where Void = 0 
--group by ChargeDetailFID,LicenseKey
--	) wod on CONCAT(cd.LicenseKey,' - ',cd.ChargeDetail_UID) = CONCAT(wod.LicenseKey,' - ',wod.ChargeDetailFID)

left join (select ChargeDetailFID, 
max(PaymentDetail_UID) PaymentDetail_UID
,max(LicenseKey) LicenseKey
from [AdvanceMD_New].[dbo].[vw_ODBC_actv_PaymentDetails]
--where ChargeDetailFID =21432514
group by ChargeDetailFID) pdg on pdg.ChargeDetailFID = cd.ChargeDetail_UID and pdg.LicenseKey = cd.LicenseKey

left join (select 
PaymentDetailFID
,max(PaymentReasonFID) PaymentReasonFID
,max(LicenseKey) LicenseKey
,max(CreatedAt) Denail_Posted_Date
from [AdvanceMD_New].[dbo].[vw_ODBC_actv_PaymentDetailReason]
--where PaymentDetailFID =22110954
group by PaymentDetailFID)
pdr on pdr.PaymentDetailFID = pdg.PaymentDetail_UID and pdr.LicenseKey = pdg.LicenseKey

left join [AdvanceMD_New].[dbo].[vw_ODBC_mf_PaymentReasons] pr on pr.PaymentReason_UID = pdr.PaymentReasonFID and pr.LicenseKey = pdr.LicenseKey

left join 
(select wd.ChargeDetailFID, SUM(wd.WriteOffAmount) as 'WriteOffAmount',wd.LicenseKey from [AdvanceMD_New].[dbo].[vw_ODBC_actv_WriteOffDetails] wd
			where wd.WriteOffDetail_UID <> 1 and wd.WriteOffFID in (select WriteOffs_UID from [AdvanceMD_New].[dbo].[vw_ODBC_actv_Writeoffs] where WriteoffSource = 1 and Void = 0)
			group by wd.ChargeDetailFID, wd.LicenseKey) woa
			on cd.LicenseKey = woa.LicenseKey and cd.ChargeDetail_UID = woa.ChargeDetailFID

--left join
--	(select LicenseKey,ChargeDetailFID,MAX(WriteOffFID) as 'WriteOffFID' from [AdvanceMD_New].[dbo].[vw_ODBC_actv_WriteOffDetails] where Void = 0 and WriteOffFID <> 1
--	and ChargeDetailFID <> 1--patient WO
--	group by ChargeDetailFID, LicenseKey) wop
--	on CONCAT(cd.LicenseKey,' - ',cd.ChargeDetail_UID) = CONCAT(wop.LicenseKey,' - ',wop.ChargeDetailFID)

--left join
--	(select * from [AdvanceMD_New].[dbo].[vw_ODBC_actv_Writeoffs] where WriteoffSource = 1 and Void = 0) woa
--	on CONCAT(woa.LicenseKey,' - ',woa.WriteOffs_UID) = CONCAT(wop.LicenseKey,' - ',wop.WriteOffFID)

left join
	(select * from [AdvanceMD_New].[dbo].[vw_ODBC_actv_lnk_ChargeDetail_DiagnosisCodes] where CodeSet = 10 and CodeSequence = 1) cdds --CodeSequence = 1 is PrimaryDiagnosis
	on CONCAT(cdds.LicenseKey,' - ',cdds.ChargeDetailFID) = CONCAT(cd.LicenseKey,' - ',cd.ChargeDetail_UID)

left join
	(select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_DiagnosisCodes] where CodeSet = 10) ds
	on CONCAT(ds.LicenseKey,' - ',ds.DiagnosisCode_UID) = CONCAT(cdds.LicenseKey,' - ',cdds.DiagnosisCodeFID)

left join 
	[All_Licensekey].[dbo].[customtabdata] ctd on ctd.FIDValue = cd.VisitFID and ctd.LicenseKey = cd.LicenseKey

where 
--cd.LicenseKey = 154635
--and 
cd.Void = 0
and cd.ChargeDetail_UID <> 1
--AND cd.VisitFID = 19673996
--and cte10.ChargeCode = '88305'
--and cd.PostingDate > '2023-06-01'

--and cd.fee > 5
--and ccfvv.TextValue = 'DSCV00049207'
order by cd.ChargeDetail_UID

