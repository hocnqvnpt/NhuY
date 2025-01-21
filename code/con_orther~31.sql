select* from thuhoi_bsc_dongia;
;
insert into thuhoi_bsc_dongia
with
km0 as 
(  
            ----------------TT Thang tang tren 1 dong-------------
        select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                        , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                        , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
        from v_thongtinkm_all km 
        where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                        --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                        and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
       union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
        select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                        , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                        , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
        from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                                                        from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                                                                    ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
        where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                       -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                       and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
 ) 
select a.THANG, a.OB_ID, a.NGAY_OB, a.THUEBAO_ID, a.ma_tb, a.NGAY_BDDC_CU, a.NGAY_KTDC_CU, a.CUOC_DC_CU, a.SO_THANGDC_CU,
a.MA_GD,a.rkm_id, a.ngaY_bd, a.ngay_kt,a.tien_hopdong,
a.TIEN_THANHTOAN, a.VAT_THANHTOAN,a.NGAY_TT, a.NGAY_HD, a.NGAY_NGANHANG, a.SOSERI, a.SERI, a.KENHTHU, a.TEN_NGANHANG, a.TEN_HT_TRA, a.TD_TH, a.MA_ND_OB, a.NHANVIEN_ID_ob, a.MANV_OB, 
a.MA_TO, a.MA_PB,to_number(to_char(a.ngay_KTdc_cu,'yyyymm')), a.goi_old_id, a.HDTB_ID, a.HDKH_ID, a.NVTUVAN_ID, a.NVTHU_ID, a.THUNGAN_TT_ID, a.MANV_CN, a.PHONG_CN, a.MANV_THUYETPHUC, a.MANV_GT, a.MANV_THUNGAN,
a.SO_THANGDC_MOI, a.AVG_THANG, a.NGAY_YC, a.NVGIAOPHIEU_ID, a.DVGIAOPHIEU_ID, a.NHOMTB_ID, a.KHACHHANG_ID, a.PHIEUTT_ID, a.HT_TRA_ID, a.KENHTHU_ID, a.LOAITB_ID, a.THANHTOAN_ID, 
a.NHANVIEN_XUATHD, a.TIEN_KHOP, a.MA_CHUNGTU,b.dongia, b.tien_xuathd, b.tien_thuyetphuc
from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
    left join km0 on a.rkm_id =km0.rkm_id
    left join ttkd_Bsc.ct_dongia_tratruoc b on a.thuebao_id = b.thuebao_id and a.manv_thuyetphuc = b.ma_Nv and a.thang = b.thang and b.loai_tinh = 'DONGIATRA_OB'
where a.thang = 202405 and km0.rkm_id is null
order by a.thuebao_id;
select* from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202406 and ma_tb in ('hthanh04220','thanhthuybk');
select* from v_Thongtinkm_all where ma_tb = 'thanhthuybk';
select * from css_hcm.db_khachhang where so_Dt = '0795584625';
select* from thuhoi_bsc_dongia;
commit  ;
		select* from TTKD_BSC.blkpi_danhmuc_kpi_VTCV where ma_kpi ='HCM_SL_ORDER_001' and thang_kt is null;

select* from ttkd_Bsc.ct_dongia_tratruoc where thuebao_id = 11872909 and thang = 202405;