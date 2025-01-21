select A.THANG, A.MA_TB, A.SOTHANG_DC, A.GHICHU, A.HESO_DICHVU, A.HESO_CHUKY, A.TIEN_KHOP, B.MA_NV, B.MA_TO, B.MA_PB, A.TIEN_XUATHD
from ttkd_Bsc.ct_dongia_tratruoc a
    join ttkd_Bsc.nhanvien b on a.nhanvien_xuathd = b.ma_Nv and a.thang = b.thang
where a.loai_tinh = 'DONGIATRA_OB' AND A.TIEN_XUATHD > 0 AND A.THANG = 202408
;
select* from ttkd_bsc.nhanvien where TEN_nV LIKE '%T?ng';
select a.chungtu_id, b.* from ttkdhcm_ktnv.ds_Chungtu_nganhang_sub_oneb  a
 join  ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb b on a.chungtu_id = b.chungtu_id
 where a.ma_Gd = 'HCM-TT/02918460';
select* from where chungtu_id = 255764;
rollback;
-- tinh don gia chung tu cho chung tu tra truoc do to thu ngan hang gach
update ct_Bsc_chungtu c 
set tinh_dongia = 1 
where tinh_dongia=  0 and tra_truoc = 1 and not exists (select 1 from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a
            join ttkd_bsc.ct_dongia_Tratruoc b on a.thuebao_id = b.thuebao_id and a.thang = b.thang
            where a.thang = 202407 and b.loai_tinh = 'DONGIATRA_OB' and nvl(tien_xuathd,0) > 0 and a.ma_gd = c.ma_gd)
            and  nv_Gach in ((select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') );
--
select distinct ma_chungtu from ct_Bsc_chungtu where tinh_dongia = 1 and tra_truoc = 1 and  
nv_Gach in ((select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') );
update ct_Bsc_chungtu c 
set tinh_dongia = 0
where tinh_dongia=  1 and tra_truoc = 1 and exists (select 1 from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a
            join ttkd_bsc.ct_dongia_Tratruoc b on a.thuebao_id = b.thuebao_id and a.thang = b.thang
            where a.thang = 202407 and b.loai_tinh = 'DONGIATRA_OB' and nvl(tien_xuathd,0) > 0 and a.ma_gd = c.ma_gd)
            and  nv_Gach in ((select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') );
select distinct ma_Chungtu from ct_Bsc_chungtu where ma_chungtu='VCB_20240731_268970';
commit;
SELECT A.*, B.MA_gD FROM TTKD_bSC.CT_DONGIA_TRATRUOC A
    JOIN TTKD_bSC.NHUY_CT_bSC_IPCC_OBGHTT B 
    ON A.THANG = B.THANG AND A.THUEBAO_ID = B.THUEBAO_ID 
WHERE A.THANG = 202407 AND LOAI_tINH = 'DONGIATRA_OB' AND A.NHANVIEN_XUATHD IN ('CTV041777','VNP017668','CTV078251') AND TIEN_XUATHD>0;
select * from ct_Bsc_chungtu c where ma_chungtu in ( 'VCB_20240723_255742',
'VCB_20240722_255180',
'KB_20240724_262670',
'VCB_20240729_265649',
'VCB_20240729_266042',
'VCB_20240729_265412',
'VCB_20240728_264926',
'VCB_20240729_266495',
'VCB_20240731_270601',
'VCB_20240730_266747',
'VCB_20240804_277578',
'VCB_20240730_267134',
'VCB_20240730_267509',
'VCB_20240730_267140',
'VCB_20240729_266056',
'VCB_20240728_265014',
'VCB_20240728_264956',
'VCB_20240729_265655',
'VCB_20240723_255876',
'VCB_20240729_266487',
'VCB_20240731_270586',
'VCB_20240730_267535',
'VCB_20240729_266453',
'VCB_20240728_265016',
'VCB_20240730_267502',
'VCB_20240730_267541',
'VCB_20240722_254170',
'VCB_20240729_266719',
'VCB_20240728_264968',
'VCB_20240729_265716',
'VCB_20240729_265728',
'VCB_20240731_268958',
'VCB_20240730_267546',
'VCB_20240729_265375',
'VCB_20240731_270596',
'VCB_20240723_255859',
'VCB_20240731_268970',
'VCB_20240729_266055',
'VCB_20240731_269086',
'VCB_20240729_265644',
'VCB_20240729_266704',
'VCB_20240729_265402') ;and not exists (select 1 from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a
            join ttkd_bsc.ct_dongia_Tratruoc b on a.thuebao_id = b.thuebao_id and a.thang = b.thang
            where a.thang = 202407 and b.loai_tinh = 'DONGIATRA_OB' and nvl(tien_xuathd,0) > 0 and a.ma_gd = c.ma_gd)
            70 42 14 19
 and  nv_Gach in ((select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') );