DELETE FROM TTKD_bSC.TL_gIAHAN_TRATRUOC WHERE THANG = 202411 AND MA_PB = 'VNP0701500' AND MA_kPI = 'HCM_TB_GIAHA_026';
COMMIT;
ROLLBACK;
-- TEN MIEN 
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
             , 'HCM_TB_GIAHA_026' ma_kpi
             , a.ma_nv, a.ma_to, a.ma_pb
               , count(thuebao_id) tong
              , sum(case when dthu > 0  and tien_khop > 0 then 1 else 0 end) da_giahan
              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
              , sum(dthu) DTHU_thanhcong
              , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle
from (select thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                                    , sum(a.tien_khop)
                                    from ttkd_bsc.ct_bsc_giahan_cntt a
                                    where a.thang = 202411 and loaitb_id in (147,148) and thang_ktdc_cu = 202411 --and ma_pb ='VNP0702400'--  AND MANV_GIAO = 'VNP017400'------------n------------ 
                                    AND MA_PB = 'VNP0701500' 
                                   group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null 
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb ;
order by 2;
rollback;
commit;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang= 202411 and ma_kpi = 'HCM_TB_GIAHA_026' AND MA_PB='VNP0701400';
;
---------------Chay binh quan To, Phong -----

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_026')  AND MA_PB = 'VNP0701500' --and ma_pb ='VNP0702400'-- AND MA_TO = 'VNP0701603'
            group by THANG, MA_KPI, MA_TO, MA_PB
;

SELECT DISTINCT MA_PB FROM ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202411 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_TO';
---- PHO GIAM DOC
--- 9 phong bhkv
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_026' AND DICHVU = 'Phong'    )
            b on a.thang = b.thang 
and a.ma_pb = b.ma_pb
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026') AND   A.MA_PB = 'VNP0701500' AND A.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
COMMIT;


update TTKD_BSC.bangluong_kpi a 
set 
 tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang = 202411 and MA_VTCV = a.MA_VTCV) 
--and ma_pb in ('VNP0702400','VNP0701100')
and ma_kpi = 'HCM_TB_GIAHA_026' AND MA_PB ='VNP0701500';
--- TO TRUONG
update TTKD_BSC.bangluong_kpi a 
set  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
 , tyle_thuchien= (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')                                                                               
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_026') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026'  AND MA_PB ='VNP0701500';
;
--- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_026'
                and ma_nv = a.ma_nv  )
    ,  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
        , giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                and ma_kpi in ('HCM_TB_GIAHA_026')  
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026'  AND MA_PB ='VNP0701500';;
;

update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 2
                           when (tyle_Thuchien) >= 95 and tyle_thuchien < 100 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411  AND MA_PB ='VNP0701500';
;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 80 and tyle_thuchien < 95 then 1
                        when (tyle_Thuchien) < 80 then 2 end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411  AND MA_PB ='VNP0701500';
  ;
  SELECT* FROM  ttkd_Bsc.bangluong_kpi  where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411  AND MA_PB ='VNP0701500';

COMMIT;