select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202403 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB ;
    select * from ttkd_Bsc.tl_giahan_tratruoc where thang = 202403 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') and ma_to is null;
select distinct ten_vtcv, ma_vtcv from ttkd_bsc.nhanvien_202403 --where ten_vtcv like '%u?n lý%'--in ('VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_18')

select * from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('02/04/2024','dd/mm/yyyy') and datcoc_csd =0 and thang_kt = 202403 and loaitb_id = 132
select* from css_hcm.ct_phieutt where hdtb_id in (
select hdtb_id from css_hcm.hd_Thuebao where thuebao_id in (9488897,9424513));
select* from v_Thongtinkm_All where  thuebao_id in (9488897,9424513)