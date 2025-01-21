select* from ttkdhcm_ktnv.ds_chungtu_tratruoc where phieutt_id = 7835854

insert into
select* from t2 --ds_chua_ghep_ma_tt --(khachhang_id, thuebao_id
select * from v_thongtinkm_all
select distinct thang_kt from t1
drop table t1
--select* from ds_chua_ghep_ma_tt
select thang_kt from t2
create table t1 as
--
select* from css_hcm.phieutt_hd
select* from cung_matt
create table cung_matt as
select t1.khachhang_id,t1.thang_kt, LISTAGG(a.ma_tb , '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ds_tb_da_ghep_ma_tt, count(a.thuebao_id) as sl
from t1 
    left join ds_Thuebao_cung_kh a on t1.khachhang_id = a.khachhang_id and t1.thang_kt = a.thang_kt
    where nvl(ma_tt_chinh,'') = a.ma_tt
    group by t1.khachhang_id,t1.thang_kt
   select ma_tt from css_hcm.db_Thanhtoan where thanhtoan_id in ( select thanhtoan_id from css_hcm.db_Thuebao where khachhang_id = 9701763 and loaitb_id in 
    (11, 18, 58, 59, 61, 171, 210, 222, 224) and trangthaitb_id not in (7,8,9) )--2824363
alter table t1 drop column  cung_matt 
alter table t1 add sl_cung_matt number
update t1 set sl_cung_matt = (select a.sl from cung_matt a where a.khachhang_id = t1.khachhang_id and a.thang_kt = t1.thang_kt)
commit;
select * from ds_Thuebao_cung_kh
;insert into t2
flashback table nhuy.ds_tb to before drop;
with
ds_tb as (
    select* from ds_Tb_chot where thang_kt = 202408 and loaitb_id in (58,59)
)

, ds_cung_diachi as (
    select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id, b.thuebao_id tb_phu, b.ma_tb matb_phu, a.thang_kt,b.ma_Tt ma_tt_phu
    , b.loaitb_id loaitb
from ds_tb a
    left join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id 
    left join ds_Thuebao_cung_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id AND A.THANG_KT = B.THANG_KT
where  a.thang_kt = 202408 and c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt != b.ma_tt
    and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0 --and a.thang_kt != d.thang_kt
)  ,
ds_lechky as (
    select a.khachhang_id ,a.thuebao_id, LISTAGG(b.ma_tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_lechky, a.thang_kt, count (b.thuebao_id) as sl_lechky
    from ds_tb_chot a
        left join  ds_tb_chot b on a.khachhang_id = b.khachhang_id and a.thang_kt != b.thang_kt
    where a.thang_kt = 202408
    group by a.khachhang_id ,a.thuebao_id, a.thang_kt
)
select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, a.tinh_id,a.quan_id,a.phuong_id,a.thang_kt,-- a.ma_tt_phu,
    LISTAGG(a.matb_phu , '; ') WITHIN GROUP (ORDER BY a.tb_phu) ds_ma_tb_phu,
--        RTRIM(XMLAGG(XMLELEMENT(E,a.matb_phu,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATB,
    count(tb_phu) as sl_tb_phu,
    ------------------
    ------------------
--        RTRIM(XMLAGG(XMLELEMENT(E,a.ma_tt_phu,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATT,

    count(distinct a.ma_tt_phu) as sl_ma_Tt,
    case when count(distinct a.ma_tt_phu)  <  count(tb_phu) then stats_mode(a.ma_tt_phu) else null end ma_tt_chinh,
    case when b.ds_ma_tb_lechky is null then 0 else 1 end lech_ky, b.ds_ma_tb_lechky, b.sl_lechky
from ds_cung_diachi a
    left join ds_lechky b on a.khachhang_id = b.khachhang_id and a.thuebao_id = b.thuebao_id 
group by a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, a.tinh_id,a.quan_id,a.phuong_id, a.thang_kt,b.ds_ma_tb_lechky, b.sl_lechky
;
rollback;
select* from ds_Thuebao_cung_kh --ds_Tb_Chot where khachhang_id  in (9662923);

with ds
as (
    select t1.khachhang_id,t1.thuebao_id,  t1.thang_kt ,
    count(a.ma_tt ) as sl --, , 
--    a.ma_tb, a.thang_kt, a.ma_tt, t1.ma_tt--
    ,LISTAGG(a.ma_tb , '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ds_ma_tb
    from t1 left join ds_thuebao_cung_kh a on t1.khachhang_id = a.khachhang_id and t1.thang_kt = a.thang_kt
    where t1.ma_tt = a.ma_tt and t1.thuebao_id != a.thuebao_id 
    group by t1.thang_kt,t1.thuebao_id,  t1.khachhang_id
)
select a.khachhang_id, a.thang_kt, a.thuebao_id, a.loaitb_id, a.ma_Tb, 
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt,ds.sl sl_Thuebao_cung_matt_voi_thuebao_goc, ds.ds_ma_tb as DS_TBAO_CUNG_MATT_TBAO_GOC, a.sl_tb_phu SL_TB_Chua_Ghep_Ma, a.ds_ma_tb_phu DS_TB_Chua_Ghep_Ma,a.ds_ma_tt_phu ds_ma_tt_phu, A.sl_ma_tt ,a.ma_tt_chinh,
    a.ds_ma_tb_lechky, sl_lechky , lkh.khdn, a.cung_matt tb_cung_ma_Tt_chinh, a.sl_cung_matt
from t1 a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id 
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv
    left join ds on a.khachhang_id = ds.khachhang_id and a.thang_kt = ds.thang_kt and a.thuebao_id = ds.thuebao_id
    
    