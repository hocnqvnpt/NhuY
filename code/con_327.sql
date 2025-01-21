select ma_vtcv, ma_to,ma_nv,ten_Vtcv, ma_donvi, hcm_sl_order_001 from ttkd_bsc.bangluong_kpi_202405 where hcm_sl_order_001 is not null;
select* from ttkd_Bsc.ct_bsc_Tratruoc_moi_30day where thang = 202405 and ma_tb = 'lacnuithanh';loaitb_id in (147,148);
DELETE from ttkd_bsc.tl_giahan_Tratruoc where loai_Tinh ='DONGIATRU_CA' and thang = 202405;
select* from v_Thongtinkm_all where ma_Tb = 'lacnuithanh'; and rkm_id = 6768640;
select* from tmp3_30ngay where ma_Tb = 'lacnuithanh';
select* from 
select* from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02812518';
select* from css_hcm.ct_phieutt where phieutt_id =8460003;
seleCT DISTINCT GHICHU FROM TTKD_bSC.CT_DONGIA_tRATRUOC WHERE THANG = 202405 AND loai_Tinh ='DONGIATRU_CA'; and  MA_NV = 'VNP017576' AND DONGIA>0;

UPDATE TTKD_bSC.CT_DONGIA_tRATRUOC SET DONGIA=0 WHERE GHICHU IS NOT NULL AND THANG = 202405 AND loai_Tinh ='DONGIATRU_CA';
COMMIT;
select * from thuhoi_bsc_dongia where (rkm_id, thuebao_id ) not in (
select max(rkm_id) , thuebao_id from v_thongtinkm_all a where thuebao_id in (Select thuebao_id from thuhoi_bsc_dongia )group by thuebao_id
)
;
select * from ttkd_bsc.nhuY_ct_Bsc_ipcc_obghtt a where thuebao_id = 8615356;
;
select THUEBAO_ID from ttkd_Bsc.ct_dongia_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB' GROUP BY THUEBAO_ID HAVING COUNT(THUEBAO_ID) > 1;
select * from v_thongtinkm_all where rkm_id in (delete from thuhoi_bsc_dongia where thuebao_id not in 
(Select thuebao_id from ttkd_bsc.ct_dongia_tratruoc where (thang in (202404) and loai_tinh IN ( '','DONGIATRA_OB')) or (thang = 202405 and loai_tinh = 'DONGIATRA_OB_BS'))
) ;
create table a_2b_3c as select* from thuhoi_bsc_dongia;
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where ma_Gd= 'HCM-TT/02823312';
INSERT INTO  TL_GIAHAN_TRATRUOC SELECT* from ttkd_Bsc.TL_GIAHAN_TRATRUOC where thang = 202405 and loai_tinh = 'THUHOI_DONGIA_GHTT';
DELETE FROM ttkd_Bsc.TL_GIAHAN_TRATRUOC where thang = 202405 and loai_tinh = 'THUHOI_DONGIA_GHTT';
COMMIT;
INSERT INTO  TTKD_bSC.TL_GIAHAN_TRATRUOC(MA_NV, TIEN, MA_PB, MA_TO, THANG, MA_KPI, LOAI_TINH)
WITH TIEN AS (
    SELECT MANV_THUYETPHUC MA_nV, TIEN_THUYETPHUC TIEN
    FROM thuhoi_bsc_dongia
    UNION ALL
    SELECT NHANVIEN_XUATHD, TIEN_XUATHD
    FROM thuhoi_bsc_dongia
)
SELECT A.MA_NV,-SUM(TIEN) TIEN, B.MA_PB, B.MA_TO, 202405 THANG , 'DONGIA' MA_KPI, 'THUHOI_DONGIA_GHTT' LOAI_TINH
FROM TIEN A
    JOIN TTKD_BSC.NHANVIEN_202405 B ON A.MA_NV = B.MA_nV
GROUP BY A.MA_NV, B.MA_PB, B.MA_TO
(select thuebao_id from thuhoi_bsc_dongia where thuebao_id not in 
(Select thuebao_id from ttkd_bsc.NHUY_CT_bSC_IPCC_OBGHTT where thang = 202405 and RKM_ID IS NOT NULL)
) ;
select sum(tien;
commit;
update ttkd_bsc.tl_Giahan_Tratruoc set 
update bangluong_dongia_202405 a set
    luong_dongia_ghtt = (SELECT SUM(TIEN) FROM (select sum(tien) tien, c.ma_nv from ttkd_bsc.tl_giahan_tratruoc c
                            join ttkd_Bsc.nhanvien_202405 b on c.ma_nv = b.ma_nv
                            where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB' ) and b.ma_vtcv != 'VNP-HNHCM_BHKV_47'                          
                        group by c.ma_nv 
                        having  sum(tien)  <> 0
                        union all 
                        select sum(tien) tien, ma_nv from ttkd_bsc.tl_giahan_tratruoc a
                            where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIA_TS_TP_TT' )                          
                        group by ma_nv 
                        having  sum(tien)  <> 0
                        ) WHERE MA_NV = A.MA_NV_HRM and a.ma_donvi not in ())
                         ;
                                                            
     ,giamtru_ghtt_cntt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                                            where thang = 202405 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' and ma_nv = a.MA_NV_HRM
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0)
                                                            
;
update   ttkd_bsc.tl_giahan_tratruoc set tien = -tien
where thang = 202405 and ma_kpi = 'DONGIA' and loai_tinh = 'THUHOI_DONGIA_GHTT';
commit;