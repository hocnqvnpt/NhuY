update ttkd_B;
select distinct * from ttkd_bsc.blkpi_dm_to_pgd where thang=202411 and ma_kpi = 'HCM_TB_GIAHA_029';
update ttkd_Bsc.ct_bsc_tratruoc_moi_30day x
set ma_To = (select ma_to_hrm from ttkd_bct.tobanhang a   
                                                        join ttkd_bct.db_Thuebao_ttkd_202410 b on a.tbh_id = b.tbh_ql_id and a.hieuluc = 1
                                                        where x.thuebao_id = b.thuebao_id)
where thang = 202411 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500');
select* from ttkd_bct.tobanhang;
commit;
update ttkd_Bsc.ct_bsc_tratruoc_moi_30day x
set ma_To = (select ma_to from ttkd_bsc.nhanvien a   
                        join ttkd_bct.db_Thuebao_ttkd_202410 b on a.ma_nv = b.ma_nv_am and a.thang = 202411
                                                        where x.thuebao_id = b.thuebao_id)
where thang = 202411 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to is null;
select* from ttkd_bct.tobanhang;
select* from  ttkd_Bsc.ct_bsc_tratruoc_moi_30day x
where thang = 202411 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500');
commit;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
--select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_kpi = 'HCM_TB_GIAHA_022'
--  insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
select thang, 'KPI_NV' LOAI_TINH
         , 'HCM_TB_GIAHA_029' ma_kpi
         , a.ma_nv, a.ma_to, a.ma_pb
           , count(thuebao_id) tong
          , sum(case when  tien_khop > 0 then 1 else 0 end) da_giahan
          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
          , sum(dthu) DTHU_thanhcong
          , round(sum(case when  tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, a.heso_giao--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                , sum(a.tien_khop)
                                from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                where a.thang = 202411 and ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')  --and not (khdn = 1 and thang_ktdc_cu >= 20240325 and rkm_id is null)     --and manv_giao = 'VNP017097'              ------------n------------
                               group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, a.heso_giao --, a.kq_dvi_cn, a.kq_popup
                ) a 
where ma_pb is not null
group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
order by 2
;

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)                                                   
select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                , sum(heso_giao) heso
--from ttkd_bsc.tl_giahan_tratruoc
from ttkd_bsc.tl_giahan_tratruoc
where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_029') AND LOAI_TINH = 'KPI_NV'
group by THANG, MA_KPI, MA_TO, MA_PB
;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, 1 MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
--from ttkd_bsc.tl_giahan_tratruoc a 
from ttkd_bsc.tl_giahan_tratruoc a 
left join (select distinct ma_nv, ma_to, thang, ma_pb  from ttkd_bsc.blkpi_dm_to_pgd where ma_kpi = 'HCM_TB_GIAHA_029') b on a.thang = b.thang and a.ma_to = b.ma_to 
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_029')
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv;

commit;
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select sum(DA_GIAHAN_DUNG_HEN)from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
    and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_029')
    ,giao = (select sum(TONG) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
    and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_029')
    , tyle_thuchien = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
    and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_029')
    
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_029')
    and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_029'
;
commit;
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select sum(DA_GIAHAN_DUNG_HEN)from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
    and  ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_029')
    ,giao = (select sum(TONG) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_029')
    , tyle_thuchien = (select  round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
    and ma_nv = a.ma_nv  and ma_kpi = 'HCM_TB_GIAHA_029')
    
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  giamdoc_phogiamdoc =1  and ma_kpi in ('HCM_TB_GIAHA_029')
    and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_029'
;
SELECT* FROM ttkd_bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029';
commit;
update ttkd_Bsc.bangluong_kpi set diem_cong = 5 where  tyle_thuchien >= 80 and thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029' ; 
update ttkd_Bsc.bangluong_kpi set diem_tru = 5 where  tyle_thuchien < 75 and thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029' ; 
update ttkd_Bsc.bangluong_kpi set diem_tru = 0, diem_Cong = 0 where  tyle_thuchien >= 75 and tyle_thuchien <80  and thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029' ; 
UPDATE TTKD_bSC.BANGLUONG_KPI SET GIAO = 0, DIEM_CONG = 5, MUCDO_HOANTHANH = 5 
WHERE THANG = 202411 AND MA_KPI = 'HCM_CL_TONDV_003' and ma_Nv in ('VNP017621','VNP017699');
commit;