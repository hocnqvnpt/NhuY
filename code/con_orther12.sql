select THANG, OB_ID, NGAY_OB, THUEBAO_ID, MA_TB, LOAITB_ID, KHACHHANG_ID, THANHTOAN_ID, NHOMTB_ID, NGAY_BDDC_CU, NGAY_KTDC_CU, CUOC_DC_CU, SO_THANGDC_CU, CHITIETKM_ID_CU,
MA_ND_OB, NHANVIEN_ID_OB, MANV_OB, KIEULD_ID, RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, TIEN_HOPDONG, VAT_HOPDONG, TIEN_THANHTOAN,
VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, 
TTHD_ID, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_NGANHANG, MA_CHUNGTU, TIEN_KHOP, MA_TO, MA_PB, GOI_OLD_ID, KENHTHU, PB_THPHUC, TD_TH, TEN_HT_TRA, TEN_NGANHANG, 
NHANVIEN_XUATHD, NGAY_YC, case when manv_ob is not null then 1 else 0 end co_ipcc
from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405;
select* from ttkd_bsc.nhanvien_202405 where ma_nv = 'VNP019904';
create table tmp_ipcc as select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405 and to_number(to_char(ngay_tt,'yyyymmdd'))>= 20240602 and ht_Tra_id not in (2,4,5); 
commit;
merge into  ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a
using (select distinct khachhang_id, phieutt_id, nvtuvan_id ,  ma_nd_ob, nhanvien_id_ob, manv_ob from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405 and ob_id is not null) b
on (a.khachhang_id = b.khachhang_id and a.phieutt_id = b.phieutt_id and a.nvtuvan_id = b.nvtuvan_id)
when matched then update set a.oB_id = -1 ,a.ma_nd_ob = b.ma_nd_ob, a.nhanvien_id_ob = b.nhanvien_id_ob , a.manv_ob = b.manv_ob
where a.manv_ob is null and a.thang = 202405;
insert into nhuy_ct_Bsc_ipcc_obghtt
select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405;
with abc as
 (select distinct khachhang_id, phieutt_id, nvtuvan_id , ngay_ob, ma_nd_ob, nhanvien_id_ob, manv_ob from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt 
 where thang = 202405 and ob_id is not null) 
 select khachhang_id from abc group by khachhang_id having count(distinct phieutt_id )=1 and count(khachhang_id) > 1;
 select* from tmp3_ob where lan = 4 ;
 select* from v_thongtinkm_all where ma_Tb = 'nht1806';
 select* from ttkd_Bsc.nhuy_ct_ipcc_obghtt where thang = 202405;ten_nv ='Ngy?n Tr?n Qu?c V?';
 9881113
7342718
9905367
9561538
10034181
