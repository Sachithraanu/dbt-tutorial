--#There is no data in 151175 / 156221 / 157775 / 159394 / 159829 / 159832 / 159960 / 160064 / 160098 / 160651 / 160696 LicenseKeys


with cte as (
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 155931 and UserFileTemplateFieldFID = 7002 and UserFileValue_UID != 509309--rder by FIDValue
),
cte1 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6915) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7005 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6922) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7004) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6921) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7003) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7022) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6923) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7035) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6913) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7021) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7008) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7009) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 155931 and ufv.UserFileTemplateFieldFID = 7002 and ufv.seq = 1 --and ufv.FIDValue = 19610528 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte2 as(
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 151406 and UserFileTemplateFieldFID = 7012
),
cte3 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte2 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6929) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7015 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6926) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7014) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6925) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7013) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7028) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6927) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7036) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6928) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7027) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7018) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7019) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 151406 and ufv.UserFileTemplateFieldFID = 7012 and ufv.seq = 1 --and ufv.FIDValue = 19580641 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte4 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 155974 and UserFileTemplateFieldFID = 6994
),
cte5 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte4 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6919) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 6996 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6916) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 6995) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6920) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7001) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7025) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6917) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7038) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6931) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7024) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 6999) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7000) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 155974 and ufv.UserFileTemplateFieldFID = 6994 and ufv.seq = 1-- and ufv.FIDValue = 19579433 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte6 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 151341 and UserFileTemplateFieldFID = 249 and UserFileValue_UID not in (35706,287691)
),
cte7 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte6 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 39) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 6973 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 57) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 246) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6930) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7020) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7031) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 41) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7037) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6933) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7030) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 251) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 252) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 151341 and ufv.UserFileTemplateFieldFID = 249 and ufv.seq = 1 --and ufv.FIDValue = 19538602 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte8 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 156764 and UserFileTemplateFieldFID = 7062
),
cte9 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte8 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6948) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7065 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6952) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7064) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6951) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7063) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6950) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7067) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6953) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7068) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7069) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7070) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 156764 and ufv.UserFileTemplateFieldFID = 7062 and ufv.seq = 1 --and ufv.FIDValue = 19591102 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte10 as(
		select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
		where FIDValue is not null and LicenseKey = 127101 and UserFileTemplateFieldFID = 6817
),
cte11 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte10 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6885) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 6819 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6884) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 6818) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6899) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 6852) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6886) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 6820) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6887) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 6821) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 6822) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 6823) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 127101 and ufv.UserFileTemplateFieldFID = 6817 and ufv.seq = 1 --and ufv.FIDValue = 10621995 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte12 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 152856 and UserFileTemplateFieldFID = 274
),
cte13 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte12 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 39) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 281 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 77) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 275) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 75) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 276) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 41) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 278) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 74) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 279) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 283) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 280) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 152856 and ufv.UserFileTemplateFieldFID = 274 and ufv.seq = 1 --and ufv.FIDValue = 19536413 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte14 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 154533 and UserFileTemplateFieldFID = 6975
),
cte15 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte14 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6908) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 6977 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6907) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 6976) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 75) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 276) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6909) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 6978) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6910) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 6979) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 6980) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 6981) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 154533 and ufv.UserFileTemplateFieldFID = 6975 and ufv.seq = 1 --and ufv.FIDValue = 19621577 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte16 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 154635 and UserFileTemplateFieldFID = 249
),
cte17 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte16 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 39) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 247 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 57) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 246) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 75) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 276) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 59) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 248) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 60) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 250) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 251) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 252) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 154635 and ufv.UserFileTemplateFieldFID = 249 and ufv.seq = 1 --and ufv.FIDValue = 19621812 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte18 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 155414 and UserFileTemplateFieldFID = 249
),
cte19 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte18 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 58) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 6973 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 57) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 246) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 75) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 276) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 59) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 248) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 60) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 250) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 251) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 252) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 155414 and ufv.UserFileTemplateFieldFID = 249 and ufv.seq = 1 --and ufv.FIDValue = 19584985 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte20 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 156380 and UserFileTemplateFieldFID = 249
),
cte21 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte20 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 39) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 247 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 57) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 246) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 75) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 276) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 59) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 248) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 60) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 250) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 251) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 252) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 156380 and ufv.UserFileTemplateFieldFID = 249 and ufv.seq = 1 --and ufv.FIDValue = 19589642 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte22 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 156884 and UserFileTemplateFieldFID = 7043
),
cte23 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte22 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6939) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7045 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6944) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7044) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 75) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 276) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7066) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6941) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7046) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6940) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7047) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7048) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7049) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 156884 and ufv.UserFileTemplateFieldFID = 7043 and ufv.seq = 1 --and ufv.FIDValue = 19577600 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte24 as(
	select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
	where FIDValue is not null and LicenseKey = 156942 and UserFileTemplateFieldFID = 7072
),
cte25 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte24 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6954) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7075 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6959) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7074) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6958) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7073) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7076) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6957) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7077) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6956) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7078) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7079) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7080) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 156942 and ufv.UserFileTemplateFieldFID = 7072 and ufv.seq = 1 --and ufv.FIDValue = 19591560 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte27 as(
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 152046 and UserFileTemplateFieldFID = 7166 --and UserFileValue_UID != 509309--rder by FIDValue
),
cte28 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 
--, colst.TextValue as 'Collection Status'
, colst.Name as 'Collection Status'
--,moi.TextValue as 'Method Of Inquiry'
,moi.Name as 'Method Of Inquiry'
--,fut.TextValue as 'Follow Up Type'
,fut.Name as 'Follow Up Type'
,icn.TextValue as 'ICN Number'
--,dr.TextValue as 'Denial Reason'
,dr.Name as 'Denial Reason'
--,ca1.TextValue as 'Corrective Action' 
,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte27 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6896) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7170 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6895) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7168) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6977) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7167) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7169) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6897) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7171) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6898) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7172) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7173) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7174) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 152046 and ufv.UserFileTemplateFieldFID = 7166 and ufv.seq = 1 --and ufv.FIDValue = 19668901 
) ,

cte29 as (
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 159490 and UserFileTemplateFieldFID = 7148 
),

cte30 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 

, colst.Name as 'Collection Status'

,moi.Name as 'Method Of Inquiry'

--,fut.Name as 'Follow Up Type' --not in custom tab data
,'' as 'Follow Up Type'
--,icn.TextValue as 'ICN Number'  --not in custom tab data
,'' as 'ICN Number'
,dr.Name as 'Denial Reason' 

,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte29 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6972) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7150 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6976) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7149) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --follow up type
--(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
--left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6921) ufslo3
--on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
--where UserFileTemplateFieldFID = 7003) fut
--on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --icn number 
--(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7022) icn
--on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6974) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7151) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6973) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7152) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7153) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7154) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 159490 and ufv.UserFileTemplateFieldFID = 7148 and ufv.seq = 1 --and ufv.FIDValue = 19610528 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte31 as(
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 126722 and UserFileTemplateFieldFID = 5607
),

cte32 as (
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 

, colst.Name as 'Collection Status'

,moi.Name as 'Method Of Inquiry'

--,fut.Name as 'Follow Up Type' --not in custom tab data
,'' as 'Follow Up Type'
--,icn.TextValue as 'ICN Number'  --not in custom tab data
,'' as 'ICN Number'
,dr.Name as 'Denial Reason' 

,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte31 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 69) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 5609 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 72) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 5608) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --follow up type
--(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
--left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6921) ufslo3
--on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
--where UserFileTemplateFieldFID = 7003) fut
--on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --icn number 
--(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7022) icn
--on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 71) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 5610) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 70) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 5611) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 5612) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7193) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 126722 and ufv.UserFileTemplateFieldFID = 5607 and ufv.seq = 1 --and ufv.FIDValue = 19610528 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914

),

cte33 as(
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 159395 and UserFileTemplateFieldFID = 7129
),

cte34 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 

, colst.Name as 'Collection Status'

,moi.Name as 'Method Of Inquiry'

,fut.Name as 'Follow Up Type' --not in custom tab data
--,'' as 'Follow Up Type'
,icn.TextValue as 'ICN Number'  --not in custom tab data
--,'' as 'ICN Number'
,dr.Name as 'Denial Reason' 

,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'

from cte33 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6963) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7133 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6962) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7131) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --follow up type
(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6961) ufslo3
on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
where UserFileTemplateFieldFID = 7130) fut
on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --icn number 
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7132) icn
on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6964) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7134) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6965) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7135) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7136) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7137) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 159395 and ufv.UserFileTemplateFieldFID = 7129 and ufv.seq = 1 --and ufv.FIDValue = 19610528 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914

),

cte35 as (
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 159663 and UserFileTemplateFieldFID = 7180 
),

cte36 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 

, colst.Name as 'Collection Status'

,moi.Name as 'Method Of Inquiry'

--,fut.Name as 'Follow Up Type' --not in custom tab data
,'' as 'Follow Up Type'
--,icn.TextValue as 'ICN Number'  --not in custom tab data
,'' as 'ICN Number'
,dr.Name as 'Denial Reason' 

,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
--,wd.TextValue as 'Worked Date'
,'' as 'Worked Date'  --not in custom tab data

from cte35 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 7013) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7181 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 7016) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7182) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --follow up type
--(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
--left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6961) ufslo3
--on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
--where UserFileTemplateFieldFID = 7130) fut
--on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --icn number 
--(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7132) icn
--on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 7015) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7183) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 7014) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7184) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7185) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --worked date
--(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7137) wd
--on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 159663 and ufv.UserFileTemplateFieldFID = 7180 and ufv.seq = 1 --and ufv.FIDValue = 19610528 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914

),
cte37 as(
select *, RANK() over(partition by FIDValue order by CreatedAt desc) as 'seq' from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] 
where FIDValue is not null and LicenseKey = 152616 and UserFileTemplateFieldFID = 7140 
),

cte38 as(
select
ufv.UserFileValue_UID
,ufv.LicenseKey
,ufv.UserFileInstanceFID
,ufv.UserFileTemplateFieldFID
,ufv.FIDValue
,ufv.TextValue
,ufv.CreatedAt
,ufv.CreatedBy
,ufv.ChangedAt
,ufv.ChangedBy
, RANK() over(partition by ufv.FIDValue order by ufv.CreatedAt desc) as 'seq' 

, colst.Name as 'Collection Status'

,moi.Name as 'Method Of Inquiry'

--,fut.Name as 'Follow Up Type' --not in custom tab data
,'' as 'Follow Up Type'
--,icn.TextValue as 'ICN Number'  --not in custom tab data
,'' as 'ICN Number'
,dr.Name as 'Denial Reason' 

,ca1.Name as 'Corrective Action' 
,note.TextValue as 'Note'
,wd.TextValue as 'Worked Date'
--,'' as 'Worked Date'  --not in custom tab data

from cte37 ufv--[AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv

left join --collection status
 (select ufv1.TextValue, ufv1.LicenseKey, ufv1.UserFileInstanceFID, ufv1.CreatedAt, ufslo1.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv1
 left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6968) ufslo1
 on CONCAT(ufslo1.LicenseKey,' - ',ufslo1.Value) = CONCAT(ufv1.LicenseKey,' - ',ufv1.TextValue)
 where  ufv1.UserFileTemplateFieldFID = 7143 ) colst
on CONCAT(colst.LicenseKey,' - ',colst.UserFileInstanceFID,' - ',CONVERT(varchar,colst.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --method of inquriry
(select ufv2.TextValue, ufv2.LicenseKey, ufv2.UserFileInstanceFID, ufv2.CreatedAt, ufslo2.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv2
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6967) ufslo2
on CONCAT(ufslo2.LicenseKey,' - ',ufslo2.Value) = CONCAT(ufv2.LicenseKey,' - ',ufv2.TextValue)
where UserFileTemplateFieldFID = 7178) moi
on CONCAT(moi.LicenseKey,' - ',moi.UserFileInstanceFID,' - ',CONVERT(varchar,moi.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --follow up type
--(select ufv3.TextValue, ufv3.LicenseKey, ufv3.UserFileInstanceFID, ufv3.CreatedAt, ufslo3.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv3
--left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6961) ufslo3
--on CONCAT(ufslo3.LicenseKey,' - ',ufslo3.Value) = CONCAT(ufv3.LicenseKey,' - ',ufv3.TextValue)
--where UserFileTemplateFieldFID = 7130) fut
--on CONCAT(fut.LicenseKey,' - ',fut.UserFileInstanceFID,' - ',CONVERT(varchar,fut.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

--left join --icn number 
--(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7132) icn
--on CONCAT(icn.LicenseKey,' - ',icn.UserFileInstanceFID,' - ',CONVERT(varchar,icn.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --denial reason
(select ufv4.TextValue, ufv4.LicenseKey, ufv4.UserFileInstanceFID, ufv4.CreatedAt, ufslo4.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv4
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 6969) ufslo4
on CONCAT(ufslo4.LicenseKey,' - ',ufslo4.Value) = CONCAT(ufv4.LicenseKey,' - ',ufv4.TextValue)
where UserFileTemplateFieldFID = 7144) dr
on CONCAT(dr.LicenseKey,' - ',dr.UserFileInstanceFID,' - ',CONVERT(varchar,dr.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --corrective action / resolution 
(select ufv5.TextValue, ufv5.LicenseKey, ufv5.UserFileInstanceFID, ufv5.CreatedAt, ufslo5.Name from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] ufv5
left join (select * from [AdvanceMD_New].[dbo].[vw_ODBC_mf_UserFileSelectListOption] where UserFileSelectListFID = 7012) ufslo5
on CONCAT(ufslo5.LicenseKey,' - ',ufslo5.Value) = CONCAT(ufv5.LicenseKey,' - ',ufv5.TextValue)
where UserFileTemplateFieldFID = 7179) ca1
on CONCAT(ca1.LicenseKey,' - ',ca1.UserFileInstanceFID,' - ',CONVERT(varchar,ca1.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --note
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7145) note
on CONCAT(note.LicenseKey,' - ',note.UserFileInstanceFID,' - ',CONVERT(varchar,note.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

left join --worked date
(select *from [AdvanceMD_New].[dbo].[vw_ODBC_pt_UserFileValues] where UserFileTemplateFieldFID = 7147) wd
on CONCAT(wd.LicenseKey,' - ',wd.UserFileInstanceFID,' - ',CONVERT(varchar,wd.CreatedAt,21)) = CONCAT(ufv.LicenseKey,' - ',ufv.UserFileInstanceFID,' - ',CONVERT(varchar,ufv.CreatedAt,21))

where ufv.FIDValue is not null and ufv.LicenseKey = 152616 and ufv.UserFileTemplateFieldFID = 7140 and ufv.seq = 1 --and ufv.FIDValue = 19610528 
--order by ufv.CreatedAt--ufv.UserFileInstanceFID --19605914
),

cte26 as(
select * from cte1
UNION ALL
select * from cte3
UNION ALL
select * from cte5
UNION ALL
select * from cte7
UNION ALL
select * from cte9
UNION ALL
select * from cte11
UNION ALL
select * from cte13
UNION ALL
select * from cte15
UNION ALL
select * from cte17
UNION ALL
select * from cte19
UNION ALL
select * from cte21
UNION ALL
select * from cte23
UNION ALL
select * from cte25
UNION ALL
select * from cte28
UNION ALL
select * from cte30
UNION ALL
select * from cte32
UNION ALL
select * from cte34
UNION ALL
select * from cte36
UNION ALL
select * from cte38
)


select *
--NextGen_V2

into [All_Licensekey].[dbo].[customtabdata]
from cte26 --where cte26.LicenseKey in (152616)