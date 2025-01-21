--- bo sung don gia bhol;
-- thang 12
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt set tien_khop = 1, ;
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt set tien_khop = 1, ma_Chungtu = 'PQT'
where thang = 202412 and tien_khop = 0 and ma_gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_Hd_1);
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt set tien_khop = 1, ma_Chungtu = 'PQT'
where thang = 202412 and tien_khop = 0 and ma_gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_Hd_1);
COMMIT;
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt x where x.thang = 202412 and x.ma_Chungtu = 'PQT'; and upper(x.ma_gd) = 'C';
;
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt X
set nhanvien_xuathd = (select distinct ma_nv from css_hcm.phieutt_Hd a
                        left join admin_hcm.nhanvien b on a.thungan_tt_id = b.nhanvien_id
                        where nvl(a.thungan_Hd_id,thungan_tt_id) = thungan_tt_id and a.ma_Gd = x.ma_gd)
where x.thang = 202412 and x.ma_Chungtu = 'PQT' and upper(x.ma_gd) != 'C';
select* from admin_Hcm.nhanvien where nhanvien_id = 451341;
--- 
UPDATE ttkd_bsc.ct_dongia_tratruoc set nhanvien_xuathd = null
where thang = 202412 and ghichu = 'PQT' and nhanvien_xuathd in (select ma_nv from ttkd_Bsc.nhanvien where thang = 202412 and ma_pb = 'VNP0700900');
commit;
update ttkd_Bsc.ct_dongia_tratruoc x
set tien_khop = 1, tien_thuyetphuc = 6000, tien_xuathd = 4000, ghichu = 'PQT', 
nhanvien_xuathd =(select distinct nhanvien_xuathd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where x.ma_Tb= ma_Tb and x.ma_Nv = manv_Thuyetphuc and thang = 202412)
where thang = 202412 and LOAI_TINH = 'DONGIATRA_OB' AND (ma_Tb , ma_nv )
in (select ma_tb, manv_Thuyetphuc from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt WHERE THANG = 202412 AND ma_Chungtu = 'PQT');
select* from ttkd_Bsc.nhanvien where thang = 202412 and ma_nv in (
select nhanvien_xuathd from ttkd_bsc.ct_dongia_tratruoc where thang = 202412 and ghichu = 'PQT');

--- thang 11
--- bo sung don gia bhol;
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt set tien_khop = 1, ma_Chungtu = 'PQT'
where thang = 202411 and tien_khop = 0 and ma_gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_Hd_1);
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt set tien_khop = 1, ma_Chungtu = 'PQT'
where thang = 202411 and tien_khop = 0 and ma_gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_Hd_1) And ma_pb = 'VNP0703000';
COMMIT;

;
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt X
set nhanvien_xuathd = null where nhanvien_xuathd in (select ma_nv from ttkd_Bsc.nhanvien where thang = 202412 and ma_pb = 'VNP0700900')
and x.thang = 202411 and x.ma_Chungtu = 'PQT' ;
--- 
UPDATE ttkd_bsc.ct_dongia_tratruoc set nhanvien_xuathd = null
where thang = 202412 and ghichu = 'PQT' and nhanvien_xuathd in (select ma_nv from ttkd_Bsc.nhanvien where thang = 202412 and ma_pb = 'VNP0700900');
commit;

select* from ttkd_Bsc.nhanvien where thang = 202412 and ma_nv in (
select nhanvien_xuathd from ttkd_bsc.ct_dongia_tratruoc where thang = 202412 and ghichu = 'PQT');
insert into ttkd_Bsc.ct_dongia_Tratruoc 
select 202412 THANG, 'DONGIATRA_OB_BS' LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, 
HT_TRA_ONLINE, KHUVUC, DONGIA, DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU,1 TIEN_KHOP, 'PQT'GHICHU, TYLE_THANHCONG, HESO_CHUKY,
NHANVIEN_XUATHD,4000 TIEN_XUATHD,6000 TIEN_THUYETPHUC, IPCC
from  ttkd_Bsc.ct_dongia_tratruoc x
where thang = 202411 and LOAI_TINH = 'DONGIATRA_OB' AND (ma_Tb , ma_nv )
in (select ma_tb, manv_Thuyetphuc from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt WHERE THANG = 202411 AND ma_Chungtu = 'PQT');

update ttkd_Bsc.ct_dongia_tratruoc x
set
nhanvien_xuathd =(select distinct nhanvien_xuathd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where x.ma_Tb= ma_Tb and x.ma_Nv = manv_Thuyetphuc and thang = 202411)
where thang = 202412 and LOAI_TINH = 'DONGIATRA_OB_BS' ;

SELECT* FROM ttkd_Bsc.ct_dongia_tratruoc x where  thang = 202412 and LOAI_TINH = 'DONGIATRA_OB_BS' ;
UPDATE ttkd_bsc.ct_dongia_tratruoc set nhanvien_xuathd = null;
select* from ttkd_bsc.ct_dongia_tratruoc
where thang = 202411 and ghichu = 'PQT' and nhanvien_xuathd in (select ma_nv from ttkd_Bsc.nhanvien where thang = 202412 and ma_pb = 'VNP0700900');