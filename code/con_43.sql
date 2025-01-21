select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202401 and ma_nv = 'VNP017819' 
select * from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB'  and
ma_kpi = 'HCM_TB_GIAHA_024'
select * from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_to in ('VNP0702410','VNP0702408','VNP0702409','VNP0702413');
 
select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202401 and ma_kpi ='HCM_TB_GIAHA_026'
select * from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 

 select sum(HCM_LUONG_QLDB_062) from ttkd_bsc.bangluong_dongia_qldb where thang = 202312;
select sum(DONGIA) from ttkd_bsc.ct_dongia_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA30D' AND MA_NV IN (SELECT MA_NV FROM TTKD_bSC.NHANVIEN_202312);

select * from ttkdhcm_ktnv.ghtt_chotngay_271 where trunc(ngay_chot)=to_date('02/02/2024','dd/mm/yyyy')  

select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, b.ma_to MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202401 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv,b.ma_to 
order by ma_kpi