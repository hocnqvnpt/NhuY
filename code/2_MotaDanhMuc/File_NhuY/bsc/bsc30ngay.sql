select distinct ma_to from tl_giahan_tratruoc where ma_kpi = 'HCM_TB_GIAHA_022'
SELECT MA_tO FROM TTKD_BSC.NHANVIEN_202402 WHERE MA_nV IN 
SELECT MA_TO FROM TTKD_BSC.CT_BSC_TRATRUOC_MOI WHERE THANG = 202402 AND MANV_gIAO IN (SELECT MA_NV FROM  ttkd_bsc.tl_giahan_tratruoc  WHERE thang = 202402 and loai_Tinh = 'KPI_NV' AND MA_KPI = 'HCM_TB_GIAHA_023')
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202402 and  ma_kpi = 'HCM_TB_GIAHA_023' and loai_Tinh = 'KPI_NV' AND MA_PB = 'VNP0702200'
delete from TTKD_BSC.tl_giahan_tratruoc where ma_kpi = 'HCM_TB_GIAHA_023' AND LOAI_TINH = 'KPI_TO' 
delete from 
update  ttkd_bsc.tl_giahan_tratruoc  a 
set MA_TO = (SELECT DISTINCT MA_TO FROM TTKD_BSC.CT_BSC_TRATRUOC_MOI WHERE THANG = 202402 AND MANV_GIAO = A.MA_NV)
where thang = 202402 and loai_Tinh = 'KPI_NV' AND MA_KPI = 'HCM_TB_GIAHA_023' AND MA_PB IN ('VNP0702200') AND MA_TO IS NULL

ROLLBACK;
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
--select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_kpi = 'HCM_TB_GIAHA_022'
--insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
select thang, 'KPI_NV' LOAI_TINH
         , case when ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_022'
                        when ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_022'
                    else null
            end ma_kpi
         , a.ma_nv, a.ma_to, a.ma_pb
           , count(thuebao_id) tong
          , sum(case when dthu > 0  and tien_khop > 0 then 1 else 0 end) da_giahan
          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
          , sum(dthu) DTHU_thanhcong
          , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, a.heso_giao--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                , sum(a.tien_khop)
                                from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                where a.thang = 202402 and not (khdn = 1 and thang_ktdc_cu >= 20240225 and rkm_id is null)     --and manv_giao = 'VNP017097'              ------------n------------
                               group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, a.heso_giao --, a.kq_dvi_cn, a.kq_popup
                ) a 
where ma_pb is not null
group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
order by 2
;
commit;
select sum(tong) from tl_giahan_tratruoc where loai_Tinh = 'KPI_NV' and thang = 202312 and ma_To = 'VNP0701150' --ma_nv is null
---------Chay binh quan To, Phong -----
select *  from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_022') and loai_tinh = 'KPI_PB';
-- delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202202 and MA_KPI not like 'DONG%';
select ma_kpi, ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202308 and MA_KPI in ('HCM_TB_GIAHA_022') group by ma_kpi, ma_nv having count(ma_to)>1;

---- KPI TO ----
delete from ttkd_bsc.tl_giahan_tratruoc where ma_kpi = 'HCM_TB_GIAHA_023' and thang = 202402-- and loai_Tinh = 'KPI_TO'
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
--insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)                                                   
select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                , sum(heso_giao) heso
--from ttkd_bsc.tl_giahan_tratruoc
from ttkd_bsc.tl_giahan_tratruoc
where thang = 202402 and MA_KPI in ('HCM_TB_GIAHA_022')
group by THANG, MA_KPI, MA_TO, MA_PB
;
commit;
------- KPI PHONG BAN
delete 
--select* 
from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'KPI_PB' and thang = 202402 and ma_kpi in ('HCM_TB_GIAHA_023')--,'HCM_TB_GIAHA_023')
rollback;
select* from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'KPI_PB' AND ma_kpi = 'HCM_TB_GIAHA_022' and thang = 202312--202401
--insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
--                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, 1 MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
--from ttkd_bsc.tl_giahan_tratruoc a 
from ttkd_bsc.tl_giahan_tratruoc a 
left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202402 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_022')
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv;
commit;
order by 6
select tyle from ttkd_bsc.tl_giahan_tratruoc  where ma_kpi=  'HCM_TB_GIAHA_022' and loai_tinh = 'KPI_TO' and thang = 202311;
select tyle from tl_giahan_tratruoc  where ma_kpi=  'HCM_TB_GIAHA_022' and loai_tinh = 'KPI_TO' and thang = 202311

;
delete from tl_giahan_tratruoc where thang =  202310 and loai_tinh = 'KPI_PB' and a.MA_KPI in ('HCM_TB_GIAHA_022')
select* from tl_giahan_tratruoc where ma_kpi in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023')
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202312
--Do bang luong DO BANG LUONGG 
---Buoc 5---gan BSC ghtt theo vi tri nvien, to truong, LDP
----Update nhan vien bang KPI----
drop table bangluong_kpi_202310
create table bangluong_kpi_202310 as select* from TTKD_BSC.bangluong_kpi_202310@dhsxkd 
update TTKD_BSC.bangluong_kpi_202310@dhsxkd a set 
HCM_TB_GIAHA_022 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
from tl_giahan_tratruoc@ttkddb
where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)

;
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202312 and ma_kpi = 'HCM_TB_GIAHA_022'

select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202312 and ma_pb = 'VNP0702400' 
and not (khdn = 1 and thang_ktdc_cu >= 20231225 and rkm_id is null) and tien_khop = 1
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202310 a 
set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc
where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
                and thang_kt is null and MA_VTCV = a.MA_VTCV)
;
select distinct thuebao_id from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202311  and rkm_id is not null and tien_khop = 1
and ma_To = 'VNP0701440' and not (khdn = 1 and thang_ktdc_cu >= 20231125 and rkm_id is null) 
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202310 a 
set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc@ttkddb 
where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                and ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)
select* from ttkd_bsc.tl_giahan_Tratruoc where thang = 202309 and loai_tinh = 'KPI_NV'
select distinct loai_Tinh from tl_giahan_tratruoc where thang = 202311 and ma_kpi = 'HCM_TB_GIAHA_022'
SELECT* FROM TTKD_BSC.CT_BSC_TRATRUOC_MOI WHERE THANG = 202402 AND TBH_GIAO_ID IN (321,322)