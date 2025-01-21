--- 
ROLLBACK;
COMMIT;
DELETE FROM  ttkd_Bsc.tl_Giahan_tratruoc  WHERE --loai_tinh = 'KPI_TO'
 MA_KPI = 'HCM_TB_GIAHA_027' AND THANG = 202405;
select  * from ttkd_Bsc.bangluong_kpi_202405 where ma_vtcv = 'VNP-HNHCM_BHKV_48' ;and ma_pb 
update ttkd_Bsc.tl_Giahan_tratruoc a set ma_to = (select DISTINCT ma_to from ttkd_Bsc.bangluong_kpi_202405 where ma_vtcv = 'VNP-HNHCM_BHKV_48' and ma_DONVI = a.ma_pb) WHERE loai_tinh = 'KPI_TO'
AND MA_KPI = 'HCM_TB_GIAHA_027' AND THANG = 202405 AND MA_TO IS NULL;
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                            , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                            
select thang, 'KPI_NV' LOAI_TINH
 , 'HCM_TB_GIAHA_027' ma_kpi
 , a.ma_nv, a.ma_to, a.ma_pb
   , count(thuebao_id) tong
  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
  , sum(dthu) DTHU_thanhcong
  , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
  
from    (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                        , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                        , sum(a.tien_khop), heso_giao
                        from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day a
                        where a.thang = 202405                ------------n------------
                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, heso_giao
        ) a 
where ma_pb is not null 
group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
order by 2;
commit;
UPDATE ttkd_Bsc.tl_giahan_tratruoc SET MA_TO = (SELECT MA_TO FROM TTKD_BSC.BANGLUONG_KPI_202405 WHERE MA_VTCV = 'VNP-HNHCM_BHKV_48' and ma_pb = ma_donvi)
where thang = 202405 and ma_kpi ='HCM_TB_GIAHA_027' and loai_tinh = 'KPI_TO' AND MA_tO IS NULL;
select* from TTKD_BSC.blkpi_danhmuc_kpi where thang_kt is null;
	select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_027' and thang_kt is null;

insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                                  
select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round( sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc
where thang = 202405 and MA_KPI in ('HCM_TB_GIAHA_027')  and loai_tinh = 'KPI_NV' --and ma_TO = 'VNP0702219'
group by THANG, MA_KPI, MA_TO, MA_PB;   
UPDATE 

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
select a.THANG, 'KPI_PB', a.MA_KPI, b.ma_nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202401) b on a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202405 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_027') 
group by a.THANG, a.MA_KPI, b.ma_nv, a.MA_PB ;

commit;
select
select * from (
select THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, KHUVUC, DONGIA, DTHU, to_char(NGAY_TT,'dd/mm/yyyy') ngay_tt,  NHOMTB_ID, KHACHHANG_ID, HESO_CHUKY,hESO_DICHVU, TIEN_KHOP, GHICHU, TYLE_THANHCONG, 'Tinh tong tien don gia = cot dongia * cot heso * tien_khop ; tien_khop > 1 =>*1, tien_khop = 0 => *0 ' ghichu_cachtinh
from ttkd_bsc.ct_dongia_tratruoc
 WHERE LOAI_TINH NOT IN ('DONGIATRU_CA')
);
select* from (
  select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day a
                                        
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
) where thang = 202404;

select GH_ID, MA_TB, MANV_ql, PHONG_ql
                     , MANV_GIAO ma_nv, PHONG_GIAO
                     , MA_TO, MA_PB
                     , MA_NV_CN, PHONG_CN, MANV_THuyetPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO
                     , THANG_KTDC_CU, TIEN_DC_CU TIEN_DC_CU_VAT, MA_GD
                     , RKM_ID, THANG_BD_MOI, so_thangdc, TIEN_THANHTOAN, VAT, THANG, TEN_HT_TRA
                     , to_char(NGAY_TT, 'dd/mm/yyyy') ngay_tt, to_char(NGAY_NGANHANG, 'dd/mm/yyyy') NGAY_NGANHANG, nhomtb_id, ma_chungtu, tien_khop
            from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day;

select * 
from(
    SELECT MA_TB, CHUONG_TRINH, DTV_OB, to_char(NGAY_OB,''dd/mm/yyyy'') ngay_ob, MA_GOI_DV, DOANHTHU_GOI_DV, CHU_KY, to_char(NGAY_MO_DV,''dd/mm/yyyy'') ngay_mo_dv, MA_NV, MA_TO, MA_PB, TEN_NV, TEN_TO, TEN_PB, THANG, LOAI_OB
    FROM manpn.Z_HCM_CL_OB_CKN_CKD
    where LOAI_OB = 'CKN'
)
sel