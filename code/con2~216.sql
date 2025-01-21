select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' AND ma_pb = 'VNP0703000';
SELECT* FROM TTKD_bSC.NHANVIEN WHERE THANG = 202410;
select * from css_hcm.hd_thuebao where ma_Tb='hcm_bldt_00000456';
SELECT* FROM TTKD_bSC.BANGLUONG_KPI WHERE  thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006'   AND MA_nv in ('CTV086809','VNP017738');
SELECT* FROM TTKD_bSC.CT_bSC_HOMECOMBO WHERE  thang = 202410 AND ma_Tb in ('thanhtungm','lttri15','vgam070920','vantn8931675'); ma_to='VNP0701407'; MA_nv in ('CTV086809','VNP017738');

SELECT* FROM TTKD_bSC.BANGLUONG_KPI WHERE  thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' AND MA_TO ='VNP0703004';
SELECT* FROM TTKD_bSC.CT_bSC_HOMECOMBO WHERE  MA_tB IN (
SELECT ACCOUNT_FIBER FROM ct_homecombo_2024 WHERE THANG = 202410 AND CHU_NHOM IN ('84914746201',
'84889255002',
'84918008197',
'84855812358',
'84918163002',
'84918831009',
'84918464694',
'84914746175',
'84888482271') ) AND THANG = 202410; 
SELECT* FROM ttkd_Bsc.nhanvien where ma_Nv ='VNP017738';
select 202410 thang, 'Fiber_moi' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
       ,    null ghichu                                                                                           ----1-change----
--   , 'bsung n-1 ket goi, hoan cong n' ghichu                                                                                           ---or 2-change----
--from hocnq_ttkd.ct_homecombo_2023 a
from ct_homecombo_2024 a

, (select hdgoi.ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd
    from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
    where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
                    and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1
                    and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
                 --  and hdtb.ma_tb in ('lmh91')--, 'hieuliem92020')
    ) hd
where a.thang = 202409 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
        and trangthai_fiber = 'Moi' and loai_goi in  ('HOMECHAT4','HOMECHAT2','HOMECHAT6','HOMESANH1','HOMESANH2','HOMESANH3','HOMESANH4')
        and not exists (select 1 from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang >=202409)     ---change n-1------
;