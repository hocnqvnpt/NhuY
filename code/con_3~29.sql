select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202408;
select* from ttkd_bsc.nhuy_Ct_Bsc_ipcc_obghtt    where ma_tb ='bht210726';
insert into ct_Bsc_giahan_cntt select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202408;
commit;
update ttkd_Bsc.ct_Bsc_giahan_cntt a set ma_to = (select ma_to from ttkd_Bsc.nhanvien where thang = 202408 and donvi = 'TTKD' and ma_nv = a.manv_giao )
where thang = 202408 and ma_to is null;
DELETE FROM TTKD_bSC.TL_GIAHAN_TRATRUOC WHERE THANG = 202408 AND MA_KPI IN ('HCM_TB_GIAHA_024','') AND LOAI_TINH = 'KPI_PB';
SELECT* FROM TTKD_bSC.TL_GIAHAN_TRATRUOC WHERE THANG = 202408 AND MA_KPI IN ('HCM_SL_COMBO_006','') AND LOAI_TINH = 'KPI_PB';
select* from 
COMMIT;
select* 
ROLLBACK;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, 1 MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), sum(heso_giao)
--from ttkd_bsc.tl_giahan_tratruoc a 
from ttkd_bsc.tl_giahan_tratruoc a 
left join (select distinct ma_nv, ma_to, thang, ma_pb from ttkd_bsc.blkpi_dm_to_pgd where nvl(dichvu,'Mega+Fiber')='Mega+Fiber' and thang = 202408 and (ma_to in ('VNP0702510','VNP0702407') or 
ten_to like '%T? Bán Hàng Online')) b on a.thang = b.thang and a.ma_pb = b.ma_pb 
where a.thang = 202408 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_023')
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv;
select* from  ttkd_bsc.blkpi_dm_to_pgd where thang = 202408 and ma_pb in ('VNP0702500','VNP0702400','VNP0702300');

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV,  MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv,  a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, B.MA_NV
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                            where dichvu is null or dichvu = 'Dich vu so doanh nghiep')
b on a.thang = b.thang and a.ma_to = b.ma_to 
and a.ma_pb = b.ma_pb
where a.thang = 202408 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
COMMIT;
select* from final_Ctu_Tien where thang = 202408;
-- chung tu
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202407 and ma_pb = 'VNP0700900' and loai_tinh = 'KPI_NV';
insert into  ttkd_bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV,MA_TO,  MA_PB,  DA_GIAHAN_DUNG_HEN)
select 202408, 'KPI_NV', 'HCM_SL_HOTRO_006' , MA_nV, MA_TO, MA_PB,SLCT_QUYDOI_BSC
from final_Ctu_Tien where thang = 202408;
COMMIT;
insert into  ttkd_bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV,MA_TO,  MA_PB,  tien)
select 202408, 'DONGIA_CHUNG_TU', 'DONGIA' , MA_nV, MA_TO, MA_PB,TIEN
from final_Ctu_Tien where thang = 202408;
select* from ttkd_bsc.tl_giahan_tratruoc where loai_tinh in ('THUHOI_DONGIA_GHTT') and thang = 202408;
delete from  ttkd_bsc.tl_giahan_tratruoc where   LOAI_TINH = 'DONGIA_CHUNG_TU' and thang = 202408 and ma_kpi = 'DONGIA';
delete FROM  ttkd_bsc.tl_giahan_tratruoc WHERE  LOAI_TINH = 'DONGIA_CHUNGTU' and thang = 202408 and ma_kpi = 'DONGIA';
--
  (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIA_CHUNG_TU' -- and ma_nv = a.MA_NV_HRM
--                                group by ma_nv 
                                having  sum(tien)  <> 0)