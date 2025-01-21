delete
select* from ttkd_Bsc.ct_bsc_tratruoc_moi where ma_Tb= 'minhphuong_f80e' and thang = 202402;
select* from ttkd_Bsc.ct_dongia_tratruoc where ma_Tb= 'techcombankct' and thang = 202402;
SELECT COUNT(DISTINCT THUEBAO_ID) FROM ttkd_Bsc.ct_bsc_tratruoc_moi WHERE MANV_gIAO ='VNP017875' and thang = 202402 AND TIEN_KHOP = 1;
delete 
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023' and ma_nv ='VNP017875' and loai_Tinh = 'KPI_NV';
select A.*, 'Tinh tong tien don gia = cot dongia * cot heso * tien_khop ; DONGLUC thi tien_khop = 1' ghichu
from ttkd_bsc.ct_dongia_tratruoc A where thang = 202402 and loai_tinh = 'DONGIATRU'
delete 
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023' and ma_TO ='VNP0702407' and loai_Tinh = 'KPI_TO';

delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023' and ma_PB ='VNP0702400' and loai_Tinh = 'KPI_PB';
COMMIT;
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                            , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                            
select thang, 'KPI_NV' LOAI_TINH
 , case when ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_023'
                when ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_023'
            else null
    end ma_kpi
 , a.ma_nv, a.ma_to, a.ma_pb
   , count(thuebao_id) tong
  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
  , sum(dthu) DTHU_thanhcong
  , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
  
from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                        , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                        , sum(a.tien_khop), heso_giao
                        from ttkd_Bsc.ct_bsc_tratruoc_moi a
                        where a.thang = 202402  AND manv_giao = 'VNP017875'                 ------------n------------
                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, heso_giao
        ) a 
where ma_pb is not null 
group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
order by 2
commit;
update ttkd_bsc.ct_bsc_tratruoc_moi
select* from ttkd_bsc
delete ttkd_Bsc.tl_giahan_Tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023'
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023'  and ma_nv = 'VNP017875' and loai_tinh = 'KPI_NV' -- ma_to = 'VNP0702510'  and 
-- BSC 60 NGAY: TO
-- select* 
delete from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'KPI_TO' and thang = 202402 and ma_to = 'VNP0702407'  and ma_kpi = 'HCM_TB_GIAHA_023'
delete
-- insert into thang10 select*
from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202310 and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_TB_GIAHA_023'
commit;
CREATE TABLE BU AS SELECT* FROM TTKD_bSC.CT_BSC_TRATRUOC_mOI WHERE THANG = 202402;
COMMIT;
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                                    
select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round( sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc
where thang = 202402 and MA_KPI in ('HCM_TB_GIAHA_023')  and loai_tinh = 'KPI_NV' and ma_to = 'VNP0702407'
group by THANG, MA_KPI, MA_TO, MA_PB
select distinct ma_to from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310
-- BSC 60 NGAY: PHONG BAN
commit;
select *  from ttkd_bsc.tl_giahan_tratruoc a where loai_tinh = 'KPI_PB' and a.MA_KPI in ('HCM_TB_GIAHA_023')and  thang = 202401
commit;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
select a.THANG, 'KPI_PB', a.MA_KPI, b.ma_nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202401) b on a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202401 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_023') 
group by a.THANG, a.MA_KPI, b.ma_nv, a.MA_PB 



