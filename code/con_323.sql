select THANG,  NGAY_OB, THUEBAO_ID, MA_TB, MANV_OB, KIEULD_ID, RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, TIEN_HOPDONG,  TIEN_THANHTOAN, 
VAT_THANHTOAN,MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, 
 MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_NGANHANG, MA_CHUNGTU,  MA_TO, MA_PB,  KENHTHU,  TD_TH, TEN_HT_TRA, TEN_NGANHANG,
 case when  ma_gd in (
select ma_tt from qrcode where thang = 202405) then 'QR Code'
    when tien_khop = 4 then 'Thu t?i nhà'
    else 'Chuy?n kho?n' end HT_Tra,
NHANVIEN_XUATHD, NGAY_YC,'Các tr??ng h?p thanh toán chuy?n kho?n c?n l?y thông tin ti?n ch?ng t? t? phòng K? toán ?? xác nh?n GHTC'  ghichu
from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a 
--    join ttkD_bsc.nhanvien_202405 b--; on a.manv_thuyetphuc = b.ma_nv
where thang = 202405 and a.ma_pb = 'VNP0701100';
select * from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a 
--    join ttkD_bsc.nhanvien_202405 b--; on a.manv_thuyetphuc = b.ma_nv
where thang = 202405 and a.ma_pb = 'VNP0701100' and ma_gd in (
select ma_tt from qrcode where thang = 202405);
select* from dongia_bosung_T4_1 where thuebao_id in (select thuebao_id from dongia_bosung_T4) ;
select* from ttkd_Bsc.nhanvien_202405 where ten_Nv like '%H??ng';
select distinct loai_tinh from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202404 ;and ma_tb like 'ltkthoa%';

select chungtu_id,ma_gd,ma_ct,tien_tt,nhanvien_tt
,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=x.nhanvien_cn) nhanvien_cn
,ngay_tt,thuchien
from
(select a.chungtu_id,a.ma ma_gd
,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct,a.tongtra tien_tt
,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) nhanvien_tt
,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
    
    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                      where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
                        and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                                            where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)))) nhanvien_cn
,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd a --4991
where  (EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = a.chungtu_id and hoantat=1)
or EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where chungtu_id = a.chungtu_id and hoantat=1))
and a.tratruoc = 1) x