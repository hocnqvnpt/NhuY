select distinct ma_kpi from ttkd_bsc.tl_giahan_tratruoc where ma_to = 'VNP0702216' and thang = 202402

update ttkd_bsc.ct_bsc_tratruoc_moi a
set a.ma_to = (select ma_to from ttkd_bsc.nhanvien_202402 where a.manv_Giao =ma_nv)
where exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi c
join ttkd_bsc.nhanvien_202402 b on c.manV_giao = b.ma_nv 
where c.ma_to != b.ma_to and thang = 202402 and c.ma_pb in ('VNP0702400','VNP0702300','VNP0702500') and c.manv_giao = a.manv_giao)
and thang = 202402 and ma_pb in ('VNP0702400','VNP0702300','VNP0702500');
rollback;
select tthd_id from css_Hcm.hd_thuebao
select distinct ma_vtcv , ten_vtcv from ttkd_bsc.nhanvien_202402 where ma_to = 'VNP0702306'
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_023' and thang_kt is null ;--and nvl(thang_kt,202401) = 202401;

update  TTKD_BSC.blkpi_danhmuc_kpi_vtcv a
set thang_kt = 202401
where a.ma_kpi ='HCM_TB_GIAHA_023' and ma_vtcv in ('VNP-HNHCM_KHDN_2','VNP-HNHCM_KHDN_4','VNP-HNHCM_KHDN_3') and thang_kt is null;
rollback;
select distinct ma_vtcv, ten_vtcv from ttkd_bsc.nhanvien_202402 where ma_to = 'VNP0702407';
commit;
insert into  TTKD_BSC.blkpi_danhmuc_kpi_vtcv (ma_kpi, ma_vtcv,to_truong_pho) values ('HCM_TB_GIAHA_023','VNP-HNHCM_KHDN_10',1)
delete from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  ma_kpi = 'HCM_TB_GIAHA_023' and ma_vtcv = 'VNP-HNHCM_KHDN_14' and to_truong_pho = 1
up
SELECT* 
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_022' and thang_kt is null

select ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202402 AND MA_KPI = 'HCM_TB_GIAHA_023' and ma_pb in ('VNP0702400','VNP0702300','VNP0702500')
group by ma_nv having count(ma_nv) > 1 
select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202402 and ma_to = 'VNP0702407';
SELECT* FROM TTKD_BSC.NHANVIEN_202401 WHERE MA_TO = 'VNP0702404'
delete from ttkd_bsc.tl
select* from ttkd_bsc.nhanvien_202402 where ma_vtcv = 'VNP-HNHCM_BHKV_14';
select distinct c.ma_to, c.ten_to from ttkd_bsc.ct_bsc_tratruoc_moi c
join ttkd_bsc.nhanvien_202402 b on c.manV_giao = b.ma_nv 
where thang = 202402 and c.ma_pb in ('VNP0702400','VNP0702300','VNP0702500') --and b-.ma_To;
select* from ttkd_bsc.
select* from ttkd_bsc.nhanvien_202402 where ma_nv in ('VNP017351','VNP017875','VNP017594');

SELECT distinct ma_To, manv_giao, ma_pb FROM ttkd_bsc.ct_bsc_tratruoc_moi WHERE THANG = 202402 and  manv_giao in ('VNP017351','VNP017875','VNP017594');
update 
ttkd_bsc.ct_bsc_tratruoc_moi a
set ma_to = 'VNP0702407'
WHERE THANG = 202402 and  manv_giao in ('VNP017351','VNP017875','VNP017594');
commit;
