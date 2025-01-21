insert into ct_Bsc_homecombo;
select* from ct_Bsc_homecombo where thang =202410 and ma_Tb in (select ma_Tb from ttkd_Bsc.ct_Bsc_homecombo where thang =202410 and ma_pb = 'VNP0703000');
minus ;
select* from ttkd_Bsc.ct_Bsc_homecombo where thang =202410 and ma_pb = 'VNP0703000'; ma_Tb in ('thanhtungm','lttri15','vgam070920','vantn8931675');
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006';
SELECT* FROM TTKD_bSC.NHANVIEN WHERE THANG = 202410 AND MA_TO IN ('VNP0703020','VNP0703004','VNP0703005');
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang =202410 and ma_nv = 'VNP017740';
select* from ttkd_bsc.nhanvien where thang = 202409 and ma_Nv in ('VNP017436','VNP016694','VNP016694','CTV082370');
update ttkd_Bsc.ct_Bsc_homecombo a 
set ma_To = (select ma_to from ttkd_Bsc.nhanvien where ma_nv = a.ma_nv and thang = a.thang),
 ma_pb = (select ma_pb from ttkd_Bsc.nhanvien where ma_nv = a.ma_nv and thang = a.thang)
 where thang = 202410 ;
 commit;
 rollback;
update rmp_Bsc_phong set ten_kpi ='PTM thuê bao gói Home Sành/Ch?t' where thang = 202410 and ten_kpi ='PTM thuê bao gói Home Sành/Ch?t';
select* from rmp_Bsc_phong where thang = 202410;  and ten_kpi='PTM thuê bao gói Home Sành/Ch?t' ;
update rmp_Bsc_phong set  thang = 2024101 where ten_kpi='PTM thuê bao gói Home Sành/Ch?t' and thang = 202410 ;
select* from ct_Bsc_Chungtu where chungtu_id =155169;
select* from ttkd_bct.bangiao_chungtu_tinhbsc where  thang = 202410;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where loai_ct = 5 ;and thang = 202410;
select a.da_giahan, b.da_giahan, a.phong_Giao
from rmp_Bsc_phong a join rmp_Bsc_phong b on a.ten_kpi = b.ten_kpi and a.ma_pb = b.ma_pb
where a.thang = 202410 and b.thang = 2024101 and  a.ten_kpi='PTM thuê bao gói Home Sành/Ch?t';
update ttkd_Bsc.ct_Bsc_Giahan_cntt set tien_khop = 1 where thang = 202410 and manv_Giao ='VNP017748' and ma_Tb ='hcm_ivan_00013758';and loaitb_id  in (147,148);
commit;
select* from css_hcm.bd_goi_dadv ;where ma_Tb='918831009';
select* from css
update ttkd_Bsc.ct_dongia_Tratruoc set tien_khop = 1, dongia = 0 where thang = 202410 and ma_Nv ='VNP017748' and ma_Tb ='hcm_ivan_00013758';and loaitb_id  in (147,148);

select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202410 and manv_Giao ='VNP017748' and ma_Tb ='hcm_ivan_00013758';and loaitb_id  in (147,148);
select* from ttkd_Bsc.nhanvien where thang = 202410 and ten_Nv like '%Thúy';
select* from ttkdhcm_ktnv.ghtt_Chotngay_271 where ma_Tb in ('hcm_tmvn_00005262', 'hcm_tmvn_00005270' );
select ton_dk+cuoc_tk, ton_ck from qltn_Hcm.db_Datcoc where  ton_dk+cuoc_tk != ton_ck;
select* from ttkd_Bsc.nhanvien where thang = 202411 and ten_vtcv like '%?i?u%'; (ten_pb like '%Doanh%' or ten_pb like '%Khu%') and ten_to like '%Lãnh ??o%';
SELECT* FROM  ttkd_Bsc.bangluong_Kpi where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_028';
select distinct thuebao_id from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202410 and thang_ktdc_cu = 202410
and ma_pb ='VNP0702400' and loaitb_id not in (147,148) and tien_khop = 1;
COMMIT;
rollback;
delete from ttkd_bsc.ct_bsc_giahan_Cntt where thang = 202410 and ma_Tb in ('hcm_ca_00043432','hcm_ca_00043277');
select* from ttkd_bsc.ct_Bsc_Giahan_Cntt where ma_tb='hcm_ca_00042212';
delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202410 and ma_Tb in ('hcm_ca_00043432','hcm_ca_00043277');
select* from ttkd_Bsc.nhanvien where ten_nv like '%Anh' and  thang = 202410; ma_nv = 'VNP016697' and thang = 202311;

select* from ttkd_Bsc.ct_dongia_TRATRUOC WHERE THANG =202410 and ma_tb='hcm_ca_00042212'; and loai_tinh in ('DONGIATRA','DONGIATRA30D');
select* from ttkd_bsc.ct_dongia_tratruoc where  ma_tb in ('hcm_ca_00045245');;manv_giao in ('VNP027560','VNP027966','VNP019958','VNP017462') and thang = 202410;
select* from ttkdhcm_ktnv.ghtt_Chotngay_271 where ;
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202410 and  ma_Tb ='hcm_ivan_00013758'; in ('hcm_ca_00045245', 'hcm_ca_00067371' ,'hcm_ca_00085885');('hcm_tmvn_00004731','hcm_tmvn_00004732','hcm_tmvn_00000795','hcm_tmvn_00003939','hcm_tmvn_00004032');
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202410 and ma_nv ='VNP030414';
select* from ttkd_Bsc.ct_Dongia_tratruoc where thang = 202410 and ma_Tb='hcm_ivan_00013758'; --VNP017861	VNP0702404	VNP0702400
commit;

rollback;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where chungtu_id =200748;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id =200748;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1  where chungtu_id =200748;

update ttkd_Bsc.ct_bsc_giahan_cntt set tien_khop = 1 where thang = 202410 and ma_Tb='hcm_tmqt_00000399';
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202410 and loai_tinh = 'DONGIATRU_CA' AND TIEN_KHOP = 1 AND DONGIA != 0;ma_Gd='HCM-TT/02994561';
delete from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202410 and ma_Tb='hcm_ca_00042212';
UPDATE ttkd_Bsc.bangluong_Kpi SET DIEM_cONG = NULL, DIEM_TRU = NULL where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_028' AND MA_vTCV ='VNP-HNHCM_KHDN_4' AND MA_nV != 'VNP017445';
delete from ttkd_Bsc.ct_Bsc_giahan_cntt where ma_Tb in ('hcm_tmvn_00005270','hcm_tmvn_00005262');
select* from ttkd_bct.bangiao_chungtu_tinhbsc where chungtu_id =427367;
select* from ct_Bsc_chungtu where  ma_chungtu='VCB_20240905_332843';
select * from ttkd_bct.bangiao_chungtu_tinhbsc where ghi_Chu is not null;
where ma_ct='VCB_20240905_332843';
select* from ttkd_Bsc.ct_Bsc_homecombo where thang = 202410 and ma_pb = '';VNP017853;
select* from qltn_hcm.bangphieutra partition for (20240901) where ma_tt= 'HCM004682190';
VNP017947
update ttkd_Bsc.bangluong_Kpi set thuchien =null where thang = 202410 and ma_Kpi = 'HCM_SL_COMBO_006' and ma_Nv ='VNP017947';
select* from ttkd_Bsc.nhanvien where thang = 202410 and ma_Nv='VNP017261';
select* from ttkd_Bsc.bangluong_Kpi  where thang = 202410 and ma_Nv in ('VNP017947','VNP017853')  and ma_Kpi = 'HCM_SL_COMBO_006' ;
select* from ttkd_Bsc.ct_bsc_Giahan_Cntt where thang = 202410 ;
select distinct chungtu_id from ttkd_Bct.bangiao_chungtu_tinhbsc where ghi_chu is not null 
and chungtu_id not in (select distinct chungtu_id from ct_Bsc_chungtu where thang = 202410)  ;
select* from ct_Bsc_chungtu where chungtu_id =383421;
select* from ttkd_bsc.nhanvien where thang = 202410 and ten_To='T? Thu C??c Qua Ngân Hàng';
select* from admin_Hcm.nhanvien where nhanvien_id = 13948;
delete from ttkd_Bsc.cT_dongia_tratruoc where thang = 202410 and ma_Tb='hcm_ca_00038514';
COMMIT;

create table bk_kpi as select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_026';
DELETE FROM TTKD_bSC.TL_gIAHAN_TRATRUOC where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_026';
select thungan_tt_id, ma_Gd from css_hcm.phieutt_Hd where ma_gd in ('HCM-TT/02903245','HCM-TT/02963362','HCM-TT/02994561');
select* from admin_hcm.nhanvien where nhanvien_id in (8372,6111,6111);
select* from ttkd_bsc.nhanvien where thang = 202410 and ma_nv in ('VNP017423','CTV021923');
INSERT INTO TTKD_bSC.CT_DONGIA_tRATRUOC
select 202410 THANG,'DONGIATRA_OB_BS_T9' LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, 
10000 DONGIA,
DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU,1 TIEN_KHOP,'SHOPEE' GHICHU,0.4 TYLE_THANHCONG, HESO_CHUKY,'VNP017423' NHANVIEN_XUATHD,5000 TIEN_XUATHD,5000 TIEN_THUYETPHUC, IPCC
from ttkd_Bsc.ct_Dongia_Tratruoc where thang IN( 202408,202409) and loai_Tinh = 'DONGIATRA_OB' and ma_Tb in (
select ma_tb from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where ma_gd in ('HCM-TT/02903245','HCM-TT/02963362','HCM-TT/02994561'));
select ngay_Ct,ngay_insert,ngay_ht from ttkdhcm_ktnv.ds_chungtu_Nganhang_oneb where ma_Ct in ('VCB_20240827_318462','VCB_20240926_369565','VCB_20241017_412537');
update ttkd_Bsc.bangluong_kpi set diem_Cong = 5, diem_tru = null;
select* from  ttkd_Bsc.bangluong_kpi  where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_024' AND TEN_nV LIKE '%Th? Thúy';
commit;
