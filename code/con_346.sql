select * from REPORT.BC_GHTT_BIEU1_CHOT@dataguard
where phanvung_id=28 and rownum = 1;
select kyhoadon,thuebao_id,ma_tb,rkm_id,cuoc_tk,ton_ck,ton_dk
from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241001 and rownum< 10 and (ton_ck) + (cuoc_tk) != (ton_dk) and tiengach is not null;
select  kyhoadon,thuebao_id,ma_tb,rkm_id,cuoc_tk,ton_ck,ton_dk from  qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241001 and ma_Tb = 'DI001028757';
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202410 and ma_Tb in ('hcm_ca_00043432','hcm_ca_00043277');

select* from ttkd_Bct.bangiao_chungtu_tinhbsc where  chungtu_id =383421;
select* from admin_Hcm.nguoidung where ma_Nd ='tnh_23';rownum = 1;
select* from admin_Hcm.nhanvien where nhanvien_id = 4753;
select* from ttkd_Bsc.nhanvien where thang = 202410 and ma_nv='VNP017349';
select* from ttkd_Bsc.nhanvien where ten_Nv like '%C?u';sdt='0914178999';
delete from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202410 and ma_Tb='hcm_ca_ivan_00019471';
delete from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202410 and ma_Tb='hcm_ca_ivan_00019471';
commit;

select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202410 and ma_Tb in ('hcm_tmvn_00005270','hcm_ca_00043432','hcm_ca_00043277 ','hcm_ca_00067371','hcm_ivan_00013758');
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202410 and ma_Tb in ('hcm_ca_00043432','hcm_ca_00085885','hcm_ca_00043277','hcm_ca_00067371','hcm_ivan_00013758');

select* from ttkd_Bsc.ct_dongia_Tratruoc where ma_Tb='pnt2024';
select* from css_Hcm.kenhthu where kenhthu_id in (22,23,29,26) ;
update ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day set tien_khop = 1 where thang = 202410 and kenhthu_id in (22,25,26) and tien_khop =0;
select distinct ht_tra_id, kenhthu_id from ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day where thang = 202410  and tien_khop =0;
;
select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt where thang = 202410 and ma_Tb= 'hcm_ca_ivan_00019440';
SELECT* FROM DS_GIAHAN_tRATRUOC2;
select* from ttkd_Bct.bangiao_chungtu_tinhbsc where  chungtu_id =383421;
select* from admin_hcm.nguoidung where ma_Nd ='tnh_30';
select ghichu, so_ct from css_hcm.phieutt_hd where phieutt_id in (
select phieutt_id from ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202410  and ma_pb = 'VNP0701400');
SELECT COUNT(DISTINCT THUEBAO_ID) from ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202410  and tien_khop = 1 and ma_pb = 'VNP0701400';
SELECT count(distinct thuebao_id) sl from ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202410    and ma_pb = 'VNP0701400' group by tien_khop;

UPDATE TTKD_bSC.ct_Bsc_Tratruoc_moi_30day a SET TIEN_KHOP = 1

--SELECT* FROM  TTKD_bSC.ct_Bsc_Tratruoc_moi_30day A
WHERE THANG = 202410 AND RKM_ID IS NULL  AND tien_khop is null and
EXISTS (SELECT 1 FROM DS_GIAHAN_tRATRUOC2 WHERE THUEBAO_ID = A.THUEBAO_ID AND TO_NUMBER(TO_CHAR(NGAY_KT_MG,'YYYYMM'))>202410);
commit;
update ttkd_bsc.ct_dongia_tratruoc  set dongia = 6000, tien_khop = 4, tien_thuyetphuc = 6000 , ghichu = 'internet'
 where thang =202410 and loai_Tinh = 'DONGIATRA_OB' AND THUEBAO_ID IN (
select THUEBAO_ID from ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202410 and ht_Tra_id = 214);
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202410 and ma_Tb in('hcm_ca_00047304','hcm_ca_00061060'); ( 'hcm_ca_ivan_00019395',
'hcm_ca_ivan_00018980',
'hcm_ca_ivan_00020483',
'hcm_ca_ivan_00019396',
'hcm_ca_ivan_00019161',
'hcm_ca_ivan_00019471');