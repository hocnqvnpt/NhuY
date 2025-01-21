create table temp_mesh as;
select sum (ton_ck) --a.rkm_id,a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.TIEN_TD, a.CUOC_DC, a.SO_THANGDC, SO_THANGKM, b.ton_ck, b.ngay_cn, b.thang_bd,b.thang_kt
from tmp_mesh_ton a
    join qltn.v_db_Datcoc@dataguard b on  a.thuebao_id = b.thuebao_id and  b.kyhoadon = 20240901
where b.thang_Bd >= 202410 and to_char(ngay_cn,'yyyymmdd') = '20241001'
;
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202409 and ma_tb = 'bht210726';
select* from 
select* from temp_mesh where to_char(ngay_cn,'yyyymmdd') = '20241001' and thang_bd >=202410;
update  ttkd_Bsc.ct_Bsc_giahan_cntt set tien_khop = 1, ma_gd = '00921627' where thang = 202409 and manv_giao = 'VNP017748' and ma_tb ='hcm_smartca_00091363' ;
commit;
select* from ttkd_Bsc.ct_bsc_Giahan_cntt where thang = 202409 and manv_Giao = 'VNP028210' and loaitb_id in (147,148);
update ttkd_Bsc.tl_giahan_tratruoc set TIEN = 0 where ma_nv = 'VNP017748' and LOAI_TINH = 'DONGIATRU_CA' AND THANG = 202409;
UPDATE TTKD_bSC.BANGLUONG_KPI SET DIEM_CONG = 5, DIEM_tRU = 0 where ma_nv = 'VNP017748' and ma_kpi = 'HCM_TB_GIAHA_024' AND THANG = 202409;
UPDATE TTKD_bSC.CT_DONGIA_tRATRUOC SET DONGIA= 0, tien_khop = 1 WHERE ma_nv = 'VNP017748' and LOAI_TINH = 'DONGIATRU_CA' AND THANG = 202409;
ROLLBACK;
select* from ttkd_Bsc.CT_DONGIA_tRATRUOC where ma_tb ='hcm_ca_00082837' and thang = 202409; and ma_pb= 'VNP0701600'
;to_char(ngay_chot,'yyyymmdd')='20241002';
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_pb = 'VNP0701500';
SELECT* FROM TTKD_bSC.BLKPI_DM_TO_PGD WHERE THANG = 202409 AND MA_NV = 'VNP017014';
select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt where thang = 202409  and ma_tb in ('hcm_ca_00042917',
'hcm_ca_00043028',
'hcm_ca_00043063',
'hcm_ca_00042835',
'hcm_ca_00043066',
'hcm_ca_00042735',
'hcm_ca_00043048',
'hcm_ca_00042707',
'hcm_ca_00042825',
'hcm_ca_00043032',
'hcm_ca_00043059',
'hcm_ca_00043457',
'hcm_ca_00043476');
select so_Ct,ghichu from css.v_phieutt_hd@dataguard where ma_gd='00946833';
select* from ttkd_bsc.nhanvien where thang = 202409 and ten_Nv like 'B?ch%' ;
select* from ttkd_Bsc.bangluong_kpi where thang = 202409 and ma_kpi like '%021';
select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202409;
select* from ttkdhcm_ktnv.
select* from ttkd_Bsc.nhanvien where ten_Nv like '%Tú' ;
select* from ttkd_Bsc.blkpi_danhmuc_kpi_Vtcv where thang =202409 and ma_kpi = 'HCM_TB_GIAHA_028' ;
SELECT * FROM TTKD_bSC.BANGLUONG_KPI WHERE  thang =202409 and ma_kpi = 'HCM_TB_GIAHA_028' ;
-- insert
insert into ttkd_bsc.bangluong_kpi(THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG)
select thang , 'HCM_TB_GIAHA_028' ma_kpi, 'T? l? thuy?t ph?c khách hàng d?ch v? VNPT CA-IVAN gia h?n tr? c??c tr??c thành công tháng T_QL?L' ten_kpi,
MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, 19 ngaycong
from ttkd_Bsc.nhanvien where thang = 202409 and ma_vtcv = 'VNP-HNHCM_KHDN_3.1';
-- delete
delete from  ttkd_bsc.bangluong_kpi where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_028' and ma_vtcv = 'VNP-HNHCM_KHDN_3';
update ttkd_Bsc.blkpi_danhmuc_kpi_Vtcv set ma_vtcv = 'VNP-HNHCM_KHDN_4' where thang =202409 and ma_kpi = 'HCM_TB_GIAHA_028' and ten_vtcv = 'Tr??ng Line_VNP-HNHCM_KHDN_4';