-- tra sau
-- them cot phieu_id

drop table ct_trsau;
create table ct_trsau as 
-- lay danh sach chung tu id600, bo hinh thuc UNC/UNT theo file
select a.ma_nh nganhang,to_char(a.ngay_ct,'dd/mm/yyyy') ngay_nh,a.ma_ct ma_chungtu,a.tien_ct tien_chungtu,c.hinhthuc hinhthuc_tt,to_char(b.ngay_tt,'dd.mm/yyyy') ngay_tt
                ,b.ma_tt,b.ma_tb
                ,(select loaihinh_tb from css_hcm.loaihinh_tb n,css_hcm.db_thuebao m where n.loaitb_id=m.loaitb_id and m.ma_tb=b.ma_tb and rownum=1) loaihinh_tb
                ,b.ghichu
                ,(select ma_nv from admin_hcm.nhanvien where nhanvien_id= b.nhanvien_id and rownum=1) nguoi_cnt
--                ,(select ten_nd from admin_hcm.nguoidung where ma_nd= b.nguoi_cn and rownum=1) nhanvien_gach
                ,(select ma_pb from ttkdhcm_ktnv.nhanvien where nhanvien_id= (select nhanvien_id from admin_hcm.nguoidung where ma_nd=b.nguoi_cn)and rownum=1) tendv_gach
                ,b.tragoc,b.trathue,b.tongtra, c.httt_id
                from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a, ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_ts b, qltn_hcm.hinhthuc_tt c
                where a.chungtu_id = b.chungtu_id
                            and b.httt_id = c.httt_id
                            and to_number(to_char(b.ngay_tt, 'yyyymm')) = 202406 and tongtra> 0 and  c.httt_id not in (170, 171)
                            
  -- doan lay httt_id = 192 KBNN
;
select* from qltn_hcm.bangphieutra where ma_Tt = 'HCM010066987' and ky_cuoc = 20240501;
union 
-- lay danh sach chung tu id44, bo hinh thuc UNC/UNT theo file
select a.nganhang,to_char(a.ngay_nganhang,'dd/mm/yyyy') ngay_nh,a.ma_capnhat ma_chungtu,a.so_tien_ghico tien_chungtu,c.hinhthuc hinhthuc_tt,to_char(b.ngay_tt,'dd.mm/yyyy') ngay_cn
,b.ma_tt,b.ma_tb
,(select loaihinh_tb from css_hcm.loaihinh_tb n,css_hcm.db_thuebao m where n.loaitb_id=m.loaitb_id and m.ma_tb=b.ma_tb and rownum=1) loaihinh_tb
,b.ghichu
,(select ma_nv from admin_hcm.nhanvien where nhanvien_id= b.nhanvien_id and rownum=1) nguoi_cnt
--,(select ten_nd from admin_hcm.nguoidung where ma_nd= b.nguoi_cn and rownum=1) nhanvien_gach
,(select ma_pb from ttkdhcm_ktnv.nhanvien where nhanvien_id= (select nhanvien_id from admin_hcm.nguoidung where ma_nd=b.nguoi_cn)and rownum=1) tendv_gach
,b.tragoc,b.trathue,b.tongtra,c.httt_id
from ttkdhcm_ktnv.ds_chungtu_tratruoc a, ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_ts b, qltn_hcm.hinhthuc_tt c
where a.so_chungtu = b.chungtu
            and b.httt_id = c.httt_id
            and to_char(a.ngay_nganhang,'yyyymmdd') = to_char(b.ngaynganhang,'yyyymmdd')
            and to_number(to_char(b.ngay_tt, 'yyyymm')) = 202406 and b.tragoc > 0 and  c.httt_id not in (170, 171)
order by nganhang;
select* from ct_trsau;
-- tra truoc
-- them cot phieutt_id
create table ct_trtruoc as

select distinct a.so_ct ma_ct,a.tien_ct
,a.khoanmuctt_id,a.ma_gd
,(select ten_dvvt from css_hcm.dichvu_vt where dichvuvt_id=a.dichvuvt_id) dichvu_vt
,a.ma_tt,a.ma_tb,a.ten_kieuld,a.loaihinh_tb,a.ten_kh,to_char(a.ngay_yc,'dd/mm/yyyy') ngay_yc,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt,a.tien_hopdong,a.vat_hopdong,(a.tien_hopdong + a.vat_hopdong) tongtien
,a.manv_xly,a.tendv_xly,a.manv_tuvan,a.manv_tt,a.tendv_tt,a.phongban_tt,a.manv_hd,a.tendv_hd,a.phongban_hd,a.ten_ht_tra,a.kenhthu,a.ten_nganhang,to_char(a.ngay_nh,'dd/mm/yyyy') ngay_nh
,a.ghichu
from ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_tt a
where to_number(to_char(a.ngay_tt,'yyyymm')) = 202406
--and a.so_ct is not null
order by ma_ct;

select* from admin_hcm.nhanvien where ma_nv ='dl_mailinh';
----Xuat file tra truoc
---loai nvl(a.bosung, 999) = 1 --> còn l?i lay h?t
create table loai_cttt as 
select a.chungtu_id,a.ma ma_gd
,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct,a.tongtra tien_tt
,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) nhanvien_tt
,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
    
    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                  where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
                    and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                                    where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)))) nhanvien_cn
,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a --4991
where  (EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = a.chungtu_id and hoantat=1)
    or EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where chungtu_id = a.chungtu_id and hoantat=1))
    and a.tratruoc = 1
and to_number(to_char(a.ngay_tt,'yyyymm')) = 202406
and nvl(a.bosung, 999) <> 1
and exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma)
;
select* from ct_trsau where nguoi_cnt like '%mailinh%';
delete from ct_trtruoc a where exists (Select 1 from loai_cttt where a.ma_gd = ma_Gd)
;

commit;
----Xuat file tra sau
TS: loai upload theo file
select * from qltn_hcm.bangphieutra partition for (20240501) ---thang n-1
where  httt_id in (170, 171);
with ct as (
    select NGANHANG,  MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB,null ma_Gd, LOAIHINH_TB, GHICHU, nguoi_cnt ma_nv,TENDV_GACH, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, 0 Tra_Truoc
    from ct_trsau
    union all 
    -- chi tien cho nhan vien TT
    select ten_nganhang,  ma_Ct, tien_ct, ten_ht_Tra, NGAY_TT, MA_TT,null, MA_gd, LOAIHINH_TB, GHICHU,manv_tt, TENDV_tt,a.tien_hopdong,a.vat_hopdong, TONGTien, null,1
    from ct_trtruoc a
    where exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma_gd) and manv_tt =nvl(manv_Hd,manv_tt) and manv_tt is not null
),
chot as (select ma_nv, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd, COUNT(DISTINCT CASE WHEN tra_truoc = 0 THEN ct.ma_tb ELSE NULL END) AS sl_matb
        from ct
        group by ma_nv, ma_chungtu
) 
, final as (
    select a.*, case when ma_nv like 'dl_mailinh%' then 2
                    when sl_magd > 0 and sl_matb > 0 then 1.5
                    when sl_matb >= 5 or sl_magd > 1 then 1.5
                    when sl_matb < 5 or sl_magd = 1 then 1
                    else null end heso, 4000 dongia
    from chot a
)
select case when MA_NV = 'dl_mailinh' then 'VNP016966' else ma_nv end ma_nv, MA_CHUNGTU, SL_MAGD, SL_MATB, HESO, DONGIA, heso*dongia tien_dongia from final a;
select* from ttkd_Bsc.nhanvien where ma_Nv = 'VNP016966';
select* from ct;
create table ds_Chungtu_dongia as
with ct as (
    select NGANHANG,  MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB,null ma_Gd, LOAIHINH_TB, GHICHU, nguoi_cnt ma_nv,TENDV_GACH, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, 0 Tra_Truoc
    from ct_trsau
--    where  MA_CHUNGTU = 'VCB8695_20240409'
    union all 
    -- chi tien cho nhan vien TT
    select ten_nganhang,  ma_Ct, tien_ct, ten_ht_Tra, NGAY_TT, MA_TT,null, MA_gd, LOAIHINH_TB, GHICHU,manv_tt, TENDV_tt,a.tien_hopdong,a.vat_hopdong, TONGTien, null,1
    from ct_trtruoc a
    where exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma_gd) and manv_tt =nvl(manv_Hd,manv_tt) and manv_tt is not null 
)
select distinct ct.ma_chungtu, chungtu_id
from ct left join  ttkdhcm_ktnv.ds_chungtu_nganhang_oneb  b on ct.ma_chungtu = b.ma_Ct;
select* from ct_dongia_chungtu;
select distinct ma_pb, ten_pb from ttkd_bsc.nhanvien_202406;
INsert into ttkd_Bsc.tl_giahan_tratruoc (thang, ma_kpi, loai_tinh, ma_Nv, tien,ma_to,ma_pb);
with ct as (
    select NGANHANG,  MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB,null ma_Gd, LOAIHINH_TB, GHICHU, nguoi_cnt ma_nv,TENDV_GACH, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, 0 Tra_Truoc
    from ct_trsau
    union all 
    -- chi tien cho nhan vien TT
    select ten_nganhang,  ma_Ct, tien_ct, ten_ht_Tra, NGAY_TT, MA_TT,null, MA_gd, LOAIHINH_TB, GHICHU,manv_tt, TENDV_tt,a.tien_hopdong,a.vat_hopdong, TONGTien, null S,1
    from ct_trtruoc a
    where   exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma_gd) 
     manv_tt =nvl(manv_Hd,manv_tt) and manv_tt is not null
),
;
select * from css.v_ghtt_chungtu@dataguard where ma_gd = 'HCM-LD/01654389';
chot as (select ma_nv, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd, COUNT(DISTINCT CASE WHEN tra_truoc = 0 THEN ct.ma_tb ELSE NULL END) AS sl_matb
        from ct
        group by ma_nv, ma_chungtu
) 
, final as (
    select MA_CHUNGTU, SL_MAGD, SL_MATB,  case when a.MA_NV like 'dl_mailinh%' then 'VNP016966' else a.ma_nv end ma_nv
    , case when ma_nv like 'dl_mailinh%' then 2
                    when sl_magd > 0 and sl_matb > 0 then 1.5
                    when sl_matb >= 5 or sl_magd > 1 then 1.5
                    when sl_matb < 5 or sl_magd = 1 then 1
                    else null end heso, 4000 dongia
    from chot a
) --select* from final;
select 202406 thang , 'DONGIA' ma_Kpi, 'DONGIA_CHUNG_TU' LOAI_TINH,
MA_CHUNGTU, SL_MAGD, SL_MATB,  HESO, DONGIA, a.ma_nv, b.ma_to, b.ma_pb, b.ten_to, b.ten_pb, b.ten_vtcv
--, case when a.MA_NV = 'dl_mailinh' then 'VNP016966' else a.ma_nv end ma_nv
, heso*dongia tien_dongia 
--    case when a.MA_NV = 'dl_mailinh' then 'Nhân Viên Thu C??c Qua Ngân Hàng Và Thanh Toán Tr?c Tuy?n' else ten_Vtcv end ten_Vtcv, 
--    case when a.MA_NV = 'dl_mailinh' then 'VNP0700940' else ma_to end ma_to, 
--    case when a.MA_NV = 'dl_mailinh' then 'VNP0700900' else ma_pb end ma_pb 
from final a
    left join ttkd_Bsc.nhanvien_202406 b on a.ma_nv = b.ma_nv
where b.ma_pb != 'VNP0700600';
group by a.ma_nv, ma_pb, ma_to, b.ten_to, b.ten_pb, b.ten_vtcv;
select* from ttkd_Bsc.tl_giahan_tratruoc where ma_nv in ('CTV041776','VNP016871');
commit;
select A.MA_nV, A.TIEN, B.TIEN, A.*, B.* from ttkd_Bsc.tl_giahan_tratruoc a
join ttkd_Bsc.tl_giahan_tratruoc b on a.ma_nv = b.ma_Nv and a.thang = b.thang
where a.loai_tinh = 'DONGIA_CHUNGTU' AND A.TIEN != B.TIEN
;
select ma_Nv from ttkd_bsc.tl_Giahan_Tratruoc where thang = 202406 and loai_tinh = 'DONGIA_CHUNG_TU' group by ma_Nv having count(ma_Nv )> 1;
update ttkd_bsc.tl_Giahan_Tratruoc set  tien = 7250000 where  loai_tinh = 'DONGIA_CHUNG_TU' and thang = 202406 and ma_Nv = 'VNP016966';
s from  ttkd_bsc.tl_Giahan_Tratruoc where thang = 202406 and ma_Nv = 'VNP016966';
rollback;
select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202406 and ma_nv = '';
commit;
select ten_pb, sum(luong_dongia_Ghtt) from ttkd_Bsc.bangluong_dongia_202406 group by ten_pb;
select* from ttkd_Bsc.bangluong_dongia_202406  where ten_pb = 'Trung Tâm Vi?n Thông Ch? L?n';
select* from ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt ;
select* from css_hcm.phieutt_hd where ma_gd = 'HCM-LD/01654389';
select* from css_hcm.ct_phieutt where phieutt_id = 8390381;
select* from css_hcm.db_Thuebao where  ma_Tb ='vina12872';
;
select* from ct_Dongia_chungtu;
		select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_024' and thang_kt is null;
SELECT DISTINCT MA_TO, TEN_VTCV, TEN_TO, TEN_PB FROM TTKD_BSC.NHANVIEN_202406 WHERE MA_TO IN (SELECT distinct ma_vtcv FROM TTKD_BSC.NHANVIEN_202406 WHERE TEN_vTCV = 'Tr??ng Line');
DISTINCT MA_TO, TEN_VTCV, TEN_TO
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_tt where ma_gd = 'HCM-LD/01654389';
select* from ttkd_bsc.nhanvien_202406 where ten_nv like '%V??ng';
select distinct ma_nv, ma_to, ma_pb from ttkd_bsc.blkpi_dm_to_pgd where thang = 202406 and  ma_kpi = 'HCM_TB_GIAHA_024'
