select* from  ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202404 and ma_Tb in('hcm_tmvn_00000437','hcm_tmqt_00000039')-- ('hcm_ca_00054795',
'hcm_ca_00054801',
'hcm_ca_00054804',
'hcm_ca_00054810',
'hcm_ca_00054814',
'hcm_ca_00054819',
'hcm_ca_00054817',
'hcm_ca_00054809',
'hcm_ca_00054808',
'hcm_ca_00054806',
'hcm_ca_00054803'
);
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202404 ;
select a.* ,LUONG_DONGIA_GHTT, a.ma_nv from ttkd_bsc.tl_giahan_tratruoc a join ttkd_bsc.bangluong_dongia_202404 b on a.ma_nv = b.ma_nv 
where a.thang = 202404 and a.loai_tinh in ( 'DONGIATRU_CA') 
and a.tien <> -GIAMTRU_GHTT_CNTT;
select hcm_tb_giaha_024,ma_nv from ttkd_Bsc.bangluong_kpi_202404 where ma_nv in ('VNP017937','VNP019952');
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202404 and  ma_nv in ('VNP017937','VNP019952');
SELECT SUM
SELECT* FROM (
select SUM(a.tien)FF, LUONG_DONGIA_GHTT, a.ma_nv from ttkd_bsc.tl_giahan_tratruoc a join ttkd_bsc.bangluong_dongia_202404 b on a.ma_nv = b.ma_nv 
where a.thang = 202404 and a.loai_tinh in ( 'DONGIATRA_OB','DONGIA_TS_TP_TT') 
GROUP BY A.MA_nV ,LUONG_DONGIA_GHTT
) WHERE FF != LUONG_DONGIA_GHTT;
	select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_027' and thang_kt is null;
    select distinct ma_vtcv from ttkd_bsc.bangluong_kpi_202404 where HCM_TB_GIAHA_027 is not null;
;
select* from ttkd_bsc.bangluong_dongia_202404; where thang = 202404 ;
update  ttkd_Bsc.ct_dongia_Tratruoc
set dongia = 0 --|| '; ghi n? ??n giá tr? ??n T12/2024'
where thang = 202404 and loai_tinh = 'DONGIATRU_CA' and ma_Tb in (
select * from ttkd_Bsc.ct_dongia_tratruoc where ma_Tb in ('hcm_ca_00038021',
'hcm_ca_00038022',
'hcm_ca_00041450',
'hcm_ca_00041451',
'hcm_ca_00041452',
'hcm_ca_00054056',
'hcm_ca_00054087',
'hcm_ca_00054364',
'hcm_ca_00054365',
'hcm_ca_00054709',
'hcm_ca_00055574',
'hcm_ca_00055659',
'hcm_ca_00055662',
'hcm_ca_00055666',
'hcm_ca_00055671',
'hcm_ca_00055674',
'hcm_ca_00055677',
'hcm_ca_00055678',
'hcm_ca_00055679',
'hcm_ca_00055688',
'hcm_ca_00055738',
'hcm_ca_00055739',
'hcm_ca_00055740',
'hcm_ca_00055741',
'hcm_ca_00055742',
'hcm_ca_00055743',
'hcm_ca_00055744',
'hcm_ca_00055745',
'hcm_ca_00055747',
'hcm_ca_00075047',
'hcm_ca_00075180',
'hcm_ca_00075185',
'hcm_ca_00075258',
'hcm_ca_00075435',
'hcm_ca_00075575',
'hcm_ca_00075581',
'hcm_ca_00075582',
'hcm_ca_00075586',
'hcm_ca_00076097',
'hcm_ca_00076099',
'hcm_ca_00076580') and thang = 202404 and NVL(tien_khop,0) != 1);
commit;
select * from ttkd_bsc.blkpi_dm_to_pgd where thang=202404