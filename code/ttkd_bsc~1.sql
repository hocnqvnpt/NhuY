select* from 
UPDATE ttkd_Bsc.CT_BSC_GIAHAN_CNTT
SET MANV_GIAO = 'CTV081669'
where thang = 202403  and ma_Tb in ('hcm_ca_ivan_00015353',
'hcm_ca_00053375',
'hcm_ca_00074379',
'hcm_ca_ivan_00015227',
'hcm_ca_00053554',
'hcm_ca_00039152',
'hcm_ca_00074966',
'hcm_ca_ivan_00013559');

update  ttkd_bsc.ct_dongia_tratruoc set ma_nv = 'CTV081669' 
where thang = 202403 and loai_Tinh = 'DONGIATRU_CA' and ma_Tb in ('hcm_ca_ivan_00015353',
'hcm_ca_00053375',
'hcm_ca_00074379',
'hcm_ca_ivan_00015227',
'hcm_ca_00053554',
'hcm_ca_00039152',
'hcm_ca_00074966',
'hcm_ca_ivan_00013559');
commit;
create table ggggg as select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 and loai_tinh = 'DONGIATRU_CA' and ma_Nv in ('CTV081669','CTV082537');
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 and loai_tinh = 'DONGIATRU_CA' and ma_Nv in ('CTV081669','CTV082537');
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202403 and loai_tinh = 'DONGIATRU_CA' and ma_Nv in ('CTV081669','CTV082537');
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI
                                                                                                        , DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)       
--insert into TTKD_BSC.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)                                                                                                             
select thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                , count(thuebao_id) tong
                  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                  , sum(dthu) DTHU_thanhcong
                  , round(sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end) *100/count(thuebao_id), 2)  tyle
                  , round(-1*sum(dongia), 0) dongia
-- select * 
--from ttkd_bsc.ct_dongia_tratruoc  a
from ttkd_bsc.ct_dongia_tratruoc  a
where ma_kpi = 'DONGIA' 
        and loai_tinh = 'DONGIATRU_CA' and thang = 202403 and ma_Nv in ('CTV081669','CTV082537')
group by thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb;
select* from ttkd_Bsc.nhanvien_202402 where ma_nv in ('CTV081669','CTV082537')
delete from ttkd_bsc.tl_giahan_Tratruoc where thang = 202403 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_Nv in ('CTV081669','CTV082537');

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
             , case when THANG_KTDC_CU = thang then 'HCM_TB_GIAHA_024'
                            when THANG_KTDC_CU > thang then 'HCM_TB_GIAHA_025'
                        else null
                end ma_kpi
             , a.ma_nv, a.ma_to, a.ma_pb
               , count(thuebao_id) tong
              , sum(case when dthu > 0  and tien_khop > 0 then 1 else 0 end) da_giahan
              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
              , sum(dthu) DTHU_thanhcong
              , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle
from       (select thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                , sum(a.tien_khop)
                from ttkd_bsc.ct_bsc_giahan_cntt a
                where a.thang = 202403 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) AND THANG_KTDC_CU = 202403--(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
               group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null AND MA_NV IN ('CTV081669','CTV082537')
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb
order by 2;
COMMIT;
DROP TABLE BU_BL
CREATE TABLE BU_BL AS SELECT* FROM TTKD_BSC.bangluong_kpi_202403 WHERE   MA_NV IN ('CTV081669','CTV082537','CTV082081')
SELECT * FROM TTKD_bSC.NHANVIEN_202403 WHERE   MA_NV IN ('CTV081669','CTV082537','CTV082081')

);
update TTKD_BSC.bangluong_kpi_202403 a 
set 
 HCM_TB_GIAHA_024 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang_kt is null and MA_VTCV = a.MA_VTCV)
AND MA_NV IN ('CTV081669','CTV082537');

select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_TB_GIAHA_024 FROM TTKD_BSC.bangluong_kpi_202403 WHERE   MA_NV IN ('CTV081669','CTV082537','CTV082081')