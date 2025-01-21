delete from khac_Diachi where thang_kt = 202412
commit;
select* from css_hcm.db_Thuebao a
left join css_Hcm.db_adsl b on a.thuebao_id = b.thuebao_id
where khachhang_id = 9769564 and trangthaitb_id not in (7,8,9) and loaitb_id in  (11, 18, 58, 59, 61, 171, 210, 222, 224) -- where thang_kt = 202412
and b.chuquan_id= 145
insert into khac_Diachi
insert into abc 
select muc_2.thuebao_id, muc_2.khachhang_id, muc_2.thang_kt,count(distinct  a.ma_tt ) + 1 as sl
from muc_2
    left join DCHI_TOANBO_THUEBAO b on muc_2.thuebao_id = b.thuebao_id
    left join ds_thuebao_cung_kh a on  muc_2.khachhang_id = a.khachhang_id and muc_2.thang_kt = a.thang_kt and muc_2.ma_tt != a.ma_tt
    where nvl(a.quan_id,0) != nvl(b.quan_id,0) or nvl(a.tinh_id,0) != nvl(b.tinh_id,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id,0) 
and muc_2.thang_kt = 202412
group by muc_2.thuebao_id, muc_2.khachhang_id, muc_2.thang_kt
select* from abc where muc_2 = 8480455
select a.*, b.sl sl_matt,c.ma_kh
from muc_2 a
    left join css_hcm.db_khachhang c on a.khachhang_id = c.khachhang_id
    left join abc b on a.thuebao_id = b.thuebao_id
    where sl is null
    select* from muc_2 where thuebao_id= 8234607
select* from muc_2 where sl_Tb <> sl_thuebao_phu + 1
update muc_2 a
set sl_ma_tt = (select sl from abc where thuebao_id = a.thuebao_id)
where sl_ma_tt is null
commit;
insert into muc_2(thang_kt) values (-1)
rollback;
select* from abc where thuebao_id = 9238100
select* from muc_2 where thuebao_id = 9238100 -- sl_ma_tt is null
select a.thuebao_id, a.khachhang_id, a.thang_kt , a.sl_ma_tt, abc.sl from muc_2 a
join abc on a.thuebao_id = abc.thuebao_id and a.khachhang_id = abc.khachhang_id and a.thang_kt = abc.thang_kt
where a.sl_ma_tt != abc.sl
insert into motchamhai 
select* from (
with ds_chot as
(
    select* from ds_tb_chot where thang_kt = 202402
)
select 
a.thuebao_id,a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, p.ten_phuong||', ' ||q.ten_quan||', '||t.tentinh as dia_Chi,
 RTRIM(XMLAGG(XMLELEMENT(E,b.ma_tb,',').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') AS ds_matb_phu  
 ,count (b.thuebao_id) as sl_thuebao_phu, count(b.ma_tt) sl_ma_tt 
-- , count (distinct b.ma_tt ) sl_matt
 from ds_Chot a
    left join ds_Thuebao_cung_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id and a.thang_kt = b.thang_kt 
    left join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id 
    left join DCHI_TOANBO_THUEBAO d on b.thuebao_id = d.thuebao_id 
    left join css_Hcm.tinh t on c.tinh_id = t.tinh_id
    left join css_hcm.quan q on c.quan_id = q.quan_id
    left join css_hcm.phuong p on c.phuong_id = p.phuong_id
where nvl(c.quan_id,0) != nvl(b.quan_id,0) or nvl(c.tinh_id,0) != nvl(b.tinh_id,0) or nvl(c.phuong_id,0) != nvl(b.phuong_id,0) 
group by a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, p.ten_phuong,q.ten_quan,t.tentinh
)
----
with ma_tt as (
    select a.khachhang_id, count(distinct b.ma_tt) as sl_ma
    from css_hcm.db_thuebao a
            left join css_hcm.db_Thanhtoan b on a.thanhtoan_id = b.thanhtoan_id
            left join css_hcm.db_adsl c on a.thuebao_id = c.thuebao_id 
    where c.chuquan_id = 145 and a.trangthaitb_id not in (7,8,9) and a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) and a.khachhang_id =  10123129
    group by a.khachhang_id
)
select* from muc_2 where thang_kt = 202412
select a.*, b.sl sl_ma_tt, 
from muc_2 a
    left join ma_Tt b on a.khachhang_id = b.khachhang_id 
update muc_2 set sl_tb = sl_thuebao_phu + 1 where sl_Tb is null
commit;
select* from khac_diachi where thuebao_id = 11822516
select khachhang_id from khac_diachi group by khachhang_id, thang_kt having count(khachhang_id) > 1
select a.khachhang_id,a.thang_kt, a.thuebao_id, a.loaitb_id, a.ma_Tb, a.dia_chi,
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt, a.sl_thuebao_phu, a.ds_matb_phu, lkh.khdn
from khac_diachi a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv
    where ten_nv = 'Kh??ng Th? Hoài Thu'
drop table diachi
commit;

create table diachi as select* from (
with a as (
select * from khac_diachi where khachhang_id not in (
select khachhang_id
 from khac_diachi
 group by khachhang_id, ma_tt, dia_chi
 having count (distinct thuebao_id) =1)
 )
, bang as (
 select a.* 
 from khac_diachi b 
 join a on b.thang_kt = a.thang_kt and b.khachhang_id = a.khachhang_id and b.thuebao_id = a.thuebao_id
-- where a.loaitb_id = 58
 ) 
 
 create table diachi as 
 Select thuebao_id,thang_kt,khachhang_id, loaitb_id, ma_tb, ma_tt,dia_chi,ds_matb_phu,sl_Thuebao_phu from(
Select a.*, row_number() over(partition by a.ma_tt, a.dia_chi, a.khachhang_id order by a.loaitb_id) ra
From khac_diachi a 
where loaitb_id >18 )
Where ra = 1

union all 

select * from khac_diachi where khachhang_id  in (
select khachhang_id
 from khac_diachi
 group by khachhang_id, ma_tt, dia_chi
 having count (distinct thuebao_id) =1)
)
--- 
select* from diachi
select a.thuebao_id,a.thang_kt,a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt,a.dia_chi,a.ds_matb_phu,a.sl_Thuebao_phu,
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, lkh.khdn
from diachi a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id 
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv

 create table backup_diachi as select* from khac_diachi
 drop table diachi