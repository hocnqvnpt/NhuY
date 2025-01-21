-- THANH LY
create table tmp_thanhly as select* from (
with cccl as (
    select a.thuebao_id, d.ma_kv, e.ma_nv,e.ten_nv tennv_ttvt, c.ten_dv ten_tovt, c.ten_dvql ten_ttvt
    from css_hcm.dbtb_kv a
        left join css_hcm.khuvuc d on a.khuvuc_id = d.khuvuc_id 
        left join css_hcm.khuvuc_nv b on a.khuvuc_id = b.khuvuc_id 
        left join admin_hcm.nhanvien e on b.nhanvien_id = e.nhanvien_id
        left join admin_hcm.donvi c on e.donvi_id = c.donvi_id
    where b.loaikv_id = 4 and b.nhiemvu = 1 and b.loainv_id = 51 and C.donvi_cha_id IN  (283453,283452,283454,283467,283455,283468,283466,283469,283451)
)
select tb.thuebao_id,tb.thanhtoan_id,tb.khachhang_id,tb.ma_tb,tb.ngay_sd,tb.ngay_td,tb.ngay_cat, cccl.ten_tovt, cccl.ten_ttvt,cccl.ma_kv,
    tt.so_dt sdt_tt, kh.so_dt sdt_kh, tt.mst mst_tt, kh.mst mst_kh--, count(distinct tb.thuebao_id ) sl
from css_hcm.db_Thuebao tb
    join cccl on tb.thuebao_id = cccl.thuebao_id
    left join css_hcm.db_khachhang kh on tb.khachhang_id = kh.khachhang_id
    left join css_hcm.db_thanhtoan tt on tb.thanhtoan_id = tt.thanhtoan_id
where  tb.trangthaitb_id in (7,9) and to_char(tb.ngay_cat,'yyyy') in ('2021','2022','2023') and tb.loaitb_id in (58,59)
)
;
-- FIBER DON LE
create table tmp_donle as select* from (
with cccl as (
    select a.thuebao_id, d.ma_kv, e.ma_nv,e.ten_nv tennv_ttvt, c.ten_dv ten_tovt, c.ten_dvql ten_ttvt
    from css_hcm.dbtb_kv a
        left join css_hcm.khuvuc d on a.khuvuc_id = d.khuvuc_id 
        left join css_hcm.khuvuc_nv b on a.khuvuc_id = b.khuvuc_id 
        left join admin_hcm.nhanvien e on b.nhanvien_id = e.nhanvien_id
        left join admin_hcm.donvi c on e.donvi_id = c.donvi_id
    where b.loaikv_id = 4 and b.nhiemvu = 1 and b.loainv_id = 51 and C.donvi_cha_id IN (283453,283452,283454,283467,283455,283468,283466,283469,283451)
)
select tb.thuebao_id,tb.thanhtoan_id,tb.khachhang_id,tb.ma_tb,tb.ngay_sd,tb.ngay_td, cccl.ten_tovt, cccl.ten_ttvt,cccl.ma_kv,
    tt.so_dt sdt_tt, kh.so_dt sdt_kh, tt.mst mst_tt, kh.mst mst_kh--,  count(distinct tb.thuebao_id ) sl_donle,
from css_hcm.db_Thuebao tb
    join cccl on tb.thuebao_id = cccl.thuebao_id
    left join css_hcm.db_khachhang kh on tb.khachhang_id = kh.khachhang_id
    left join css_hcm.db_thanhtoan tt on tb.thanhtoan_id = tt.thanhtoan_id
where  tb.trangthaitb_id not in (7,8,9) and tb.loaitb_id in (58,59)  and not exists 
(select 1 from css_hcm.db_thuebao where loaitb_id not in (58,59) and dichvuvt_id in (4, 12, 1, 11) 
and trangthaitb_id not in (7,8,9) and khachhang_id = tb.khachhang_id)
)
;
group by cccl.ten_pb;
select* from css_hcm.db_Thuebao where trangthaitb_id in (7,8,9) and ngay_cat is not null order by ngay_cat desc
select * from css_hcm.db_Thuebao where thuebao_id in (9215492,9358947)
select* from css_hcm.kieu_ld where kieuld_id = 11
select chuquan_id from css_Hcm.db_adsl where  thuebao_id in (9215492,9358947) --khachhang_id in (9857300,9609046)-- and trangthaitb_id not in (7,8,9)
select * from hocnq_ttkd.tmp_donle-- where dichvuvt_id in (4, 12, 1, 11) 
select * from css_hcm.dbtb_kv where thuebao_id in  (9215492,9358947)
select* from  css_hcm.db_khachhang
select distinct tap, ghichu from hocnq_ttkd.dulieu_vnp_hcm 
select * from css_hcm.db_Thanhtoan;
-- FIBER_NCCK
create table tmp_ncck as select * from-- where GOI_NCCK = 1 and thuebao_id not in
(
with cccl as (
    select a.thuebao_id, d.ma_kv, e.ma_nv,e.ten_nv tennv_ttvt, c.ten_dv ten_tovt, c.ten_dvql ten_ttvt
    from css_hcm.dbtb_kv a
        left join css_hcm.khuvuc d on a.khuvuc_id = d.khuvuc_id 
        left join css_hcm.khuvuc_nv b on a.khuvuc_id = b.khuvuc_id 
        left join admin_hcm.nhanvien e on b.nhanvien_id = e.nhanvien_id
        left join admin_hcm.donvi c on e.donvi_id = c.donvi_id
    where b.loaikv_id = 4 and b.nhiemvu = 1 and b.loainv_id = 51 and C.donvi_cha_id IN (283453,283452,283454,283467,283455,283468,283466,283469,283451)
)
select tb.thuebao_id,tb.thanhtoan_id,tb.khachhang_id,tb.ma_tb,tb.ngay_sd,tb.ngay_td, cccl.ten_tovt, cccl.ten_ttvt,cccl.ma_kv,
    tt.so_dt sdt_tt, kh.so_dt sdt_kh, tt.mst mst_tt, kh.mst mst_kh
from css_hcm.db_Thuebao tb
    join cccl on tb.thuebao_id = cccl.thuebao_id
    left join css_hcm.db_khachhang kh on tb.khachhang_id = kh.khachhang_id
    left join css_hcm.db_thanhtoan tt on tb.thanhtoan_id = tt.thanhtoan_id
    join hocnq_ttkd.dulieu_vnp_hcm c on nvl(tt.so_dt, kh.so_dt)  = c.ma_tb
    where  c.tap = 7 and  tb.trangthaitb_id not in (7,8,9) and tb.loaitb_id in (58,59)
)
;
SELECT* FROM  HOCNQ_TTKD.tmp_ncck
select ten_ttvt,to_char(ngay_cat,'yyyy') nam, count(thuebao_id) sl_thanhly
from tmp_thanhly
group by ten_ttvt,to_char(ngay_cat,'yyyy');
select ten_ttvt, count(thuebao_id) sl_donle
from tmp_donle
group by ten_ttvt;
select ten_ttvt, count(distinct thuebao_id) sl_ncck
from tmp_ncck
group by ten_ttvt; 
select ten_ttvt, sum(goi_Ncck) sl_ncck
select* from tmp_ncck where thuebao_id not in (select thuebao_id from hocnq_ttkd.tmp_ncck where  goi_ncck = 1 ) and ten_ttvt in ('TTVT C? Chi','TTVT Ch? L?n')  
group by ten_ttvt;--where sdt_kh in (select ma_tb,tap from hocnq_ttkd.dulieu_vnp_hcm where ma_tb= 0908130135)