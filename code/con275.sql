insert into ttkd_Bsc.ct_Bsc_giahan_cntt
select 202408 THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO, MANV_TH, PHONG_TH,
MANV_CN, PHONG_CN, MANV_THPHUC, PHONG_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC,
AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA,
NHOMTB_ID, GOI_OLD_ID, KHACHHANG_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU, THUEBAO_KHAC_ID, MA_TB_KHAC, KIEULD_ID 
from ct_Bsc_giahan_cntt where thang = 202409 and ma_Tb in 
('hcm_smartca_00001673','hcm_smartca_00072396','hcm_smartca_00025698','hcm_smartca_00025714','hcm_smartca_00072302');
select* from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202408 and ma_Tb in 
('hcm_smartca_00001673','hcm_smartca_00072396','hcm_smartca_00025698','hcm_smartca_00025714','hcm_smartca_00072302');
commit;
update ttkd_Bsc.ct_Bsc_giahan_cntt
set tien_khop = 1
where thang = 202408 and ma_Tb in 
('hcm_smartca_00001673','hcm_smartca_00072396','hcm_smartca_00025698','hcm_smartca_00025714','hcm_smartca_00072302');

delete from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202408 and ma_Tb in ('hcm_ca_ivan_00017310',
'hcm_smartca_00001082',
'hcm_smartca_00001083',
'hcm_smartca_00001668',
'hcm_smartca_00001653',
'hcm_smartca_00001589',
'hcm_smartca_00001583',
'hcm_smartca_00001572',
'hcm_smartca_00001664',
'hcm_smartca_00001622',
'hcm_smartca_00001579',
'hcm_smartca_00001670',
'hcm_smartca_00001627',
'hcm_smartca_00001651',
'hcm_smartca_00001656',
'hcm_smartca_00001657',
'hcm_smartca_00001629',
'hcm_smartca_00001596',
'hcm_smartca_00001488',
'hcm_smartca_00001667',
'hcm_smartca_00001618',
'hcm_smartca_00001654',
'hcm_smartca_00001590',
'hcm_smartca_00001484',
'hcm_smartca_00001652',
'hcm_smartca_00001577',
'hcm_smartca_00001666',
'hcm_smartca_00001624',
'hcm_smartca_00001663',
'hcm_smartca_00001665',
'hcm_smartca_00001660',
'hcm_smartca_00001662',
'hcm_smartca_00001661',
'hcm_smartca_00001658',
'hcm_smartca_00001620',
'hcm_smartca_00001578',
'hcm_smartca_00001655',
'hcm_smartca_00001573',
'hcm_smartca_00025710',
'hcm_smartca_00025691',
'hcm_smartca_00025704',
'hcm_smartca_00025671',
'hcm_smartca_00025695',
'hcm_smartca_00072343',
'hcm_smartca_00025703',
'hcm_smartca_00072356',
'hcm_smartca_00072317',
'hcm_smartca_00074379',
'hcm_smartca_00025713',
'hcm_smartca_00025702',
'hcm_smartca_00025694',
'hcm_smartca_00025670',
'hcm_smartca_00072082',
'hcm_smartca_00072328',
'hcm_smartca_00074406',
'hcm_smartca_00072191',
'hcm_smartca_00074418');
delete from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202408 and  ma_tb in ('hcm_tmvn_00001240' ,'hcm_tmvn_00001239'); ma_pb=  'VNP0701400' and tien_khop = 0;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi= 'HCM_TB_GIAHA_026' AND MA_PB in ( 'VNP0702400','VNP0701100');
update ttkd_Bsc.ct_bsc_giahan_cntt
set  ma_gd = '00930623' , phieutt_id=930623  , ngay_tt = to_Date('31/08/2024','dd/mm/yyyy'), tien_khop = 1
where thang = 202408 and ma_tb = 'hcm_ca_00037631' ;
select a.ma_nv, b.ten_vtcv from ttkd_Bsc.ct_dongia_tratruoc a
    left join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_Nv and a.thang = b.thang
where a.thang = 202408 and loai_tinh = 'DONGIATRA_OB' and a.ma_pb = 'VNP0702300';
commit;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_024';
select * from  ct_bsc_giahan_cntt where ma_tb in ('hcm_tmvn_00001240' ,'hcm_tmvn_00001239'); (select ma_tb from ttkdhcm_ktnv.kq_loaitru where user_import=61 )
and thang = 202408;
commit;