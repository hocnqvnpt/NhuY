create table chot as select* from (
with
ds_tb as (
    select* from ds_Tb_chot where thang_kt = 202408 and loaitb_id in (58,59)
)
, ds_cung_diachi_matt as (
    select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id, b.thuebao_id tb_phu, b.ma_tb matb_phu, a.thang_kt
    , b.loaitb_id loaitb
from ds_tb a
    left join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id 
    left join ds_Thuebao_cung_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id AND A.THANG_KT = B.THANG_KT
where  a.thang_kt = 202408 and c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt = b.ma_tt
    and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0 --and a.thang_kt != d.thang_kt
),
goi_dadv as (
select nhomtb_id, a.thuebao_id from css_hcm.bd_goi_dadv b
    join ds_Thuebao_cung_kh a on a.thuebao_id = b.thuebao_id
    where trangthai = 1 and dichvuvt_id = 4 and a.thang_kt = 202408
    and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
             --   and nhomtb_id not in (2691065)
) ,
chot as ( 
    select a.thuebao_id, a.khachhang_id,a.loaitb_id,a.ma_Tb,a.ma_tt,a.tinh_id,a.quan_id,a.phuong_id,a.tb_phu,
    a.matb_phu,a.loaitb,b.nhomtb_id,c.nhomtb_id nhomphu, a.thang_kt
    from ds_cung_diachi_matt a
        left join goi_dadv b on a.thuebao_id = b.thuebao_id 
        left join goi_dadv c on a.tb_phu = c.thuebao_id 
    where (nvl(c.nhomtb_id ,0) <> nvl(b.nhomtb_id,0)) or (c.nhomtb_id is null or b.nhomtb_id is null)
),
ds_lechky as (
    select a.khachhang_id ,a.thuebao_id, LISTAGG(b.ma_tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_lechky, a.thang_kt, count (b.thuebao_id) as sl_lechky
    from chot a
        left join  ds_tb_chot b on a.khachhang_id = b.khachhang_id and a.thang_kt != b.thang_kt
    where a.thang_kt = 202408
    group by a.khachhang_id ,a.thuebao_id, a.thang_kt
),
t1 as
(
 select a.thuebao_id, a.khachhang_id,a.loaitb_id,a.ma_Tb,a.ma_tt,
   a.nhomtb_id, a.thang_kt,
    listagg(case when a.ma_Tb != a.matb_phu then a.matb_phu end,'; ') within group (order by a.tb_phu) ds_chua_ghep_goi,
    count (a.tb_phu) as sl_chua_ghep_goi,
    listagg(nhomphu,'; ') within group (order by a.tb_phu) ds_nhomtb_phu

 from chot  A
    group by   a.thuebao_id, a.khachhang_id,a.loaitb_id,a.ma_Tb,a.ma_tt,
   a.nhomtb_id, a.thang_kt
) select* from t1);
create table chunggoi as select* from (
with
ds_tb as (
    select* from ds_Tb_chot where thang_kt = 202408 and loaitb_id in (58,59)
)
, ds_cung_diachi_matt as (
    select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id, b.thuebao_id tb_phu, b.ma_tb matb_phu, a.thang_kt
    , b.loaitb_id loaitb
from ds_tb a
    left join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id 
    left join ds_Thuebao_cung_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id AND A.THANG_KT = B.THANG_KT
where  a.thang_kt = 202408 and c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt = b.ma_tt
    and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0 --and a.thang_kt != d.thang_kt
),
goi_dadv as (
select nhomtb_id, a.thuebao_id from css_hcm.bd_goi_dadv b
    join ds_Thuebao_cung_kh a on a.thuebao_id = b.thuebao_id
    where trangthai = 1 and dichvuvt_id = 4 and a.thang_kt = 202408
    and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
             --   and nhomtb_id not in (2691065)
) ,
chot as ( 
    select a.thuebao_id, a.khachhang_id,a.loaitb_id,a.ma_Tb,a.ma_tt,a.tinh_id,a.quan_id,a.phuong_id,a.tb_phu,
    a.matb_phu,a.loaitb,b.nhomtb_id,c.nhomtb_id nhomphu, a.thang_kt
    from ds_cung_diachi_matt a
        left join goi_dadv b on a.thuebao_id = b.thuebao_id 
        left join goi_dadv c on a.tb_phu = c.thuebao_id 
    where c.nhomtb_id = b.nhomtb_id
)

select khachhang_id, 
    listagg(case when a.ma_Tb != a.matb_phu then a.matb_phu end,'; ') within group (order by a.tb_phu) ds_da_ghep_goi,
    count (a.tb_phu) as sl_chua_ghep_goi
    from chot  A
    group by   a.khachhang_id
)
;
select* from chot
create table final as select* from (
with ds_lechky as (
    select a.khachhang_id ,a.thuebao_id, LISTAGG(b.ma_tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_lechky, a.thang_kt, count (b.thuebao_id) as sl_lechky
    from chot a
        left join  ds_tb_chot b on a.khachhang_id = b.khachhang_id and a.thang_kt != b.thang_kt
    where a.thang_kt = 202408
    group by a.khachhang_id ,a.thuebao_id, a.thang_kt
) 
select chot.thuebao_id, chot.khachhang_id, chot.loaitb_id, chot.ma_tb, chot.ma_tt, chot.thang_kt, chot.nhomtb_id, chot.ds_Chua_ghep_goi
,chot.sl_chua_ghep_goi,chot.ds_Nhomtb_phu, a.ds_da_ghep_goi, a.sl_chua_ghep_goi sl_da_ghep_goi, b.ds_ma_tb_lechky, b.sl_lechky
from 
    chot
        left join chunggoi a on chot.khachhang_id = a.khachhang_id
        left join ds_lechky b on chot.khachhang_id = b.khachhang_id )
        ;
select a.khachhang_id, a.thang_kt, a.thuebao_id, a.loaitb_id, a.ma_Tb, 
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt,a.sl_chua_ghep_goi, a.ds_Chua_ghep_goi,a.ds_da_ghep_goi, a.sl_da_ghep_goi,
    NVL(sl_chua_ghep_goi,0)+nvl(sl_da_ghep_goi,0) tong ,
    a.nhomtb_id,a.ds_Nhomtb_phu, lkh.khdn,ds_ma_tb_lechky,
    sl_lechky

from final a
left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id 
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv
