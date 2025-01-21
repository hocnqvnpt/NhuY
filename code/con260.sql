select* from ttkd_Bct.db_thuebao_ttkd where rownum = 1;

with tien as (
    select ma_nv, sum(tien) tien
    from ttkd_Bsc.tl_giahan_tratruoc
    where thang = 202406 and loai_Tinh in ('DONGIATRU_CA')
    GROUP BY MA_nV
)
select a.*
from ttkd_bsc.bangluong_dongia_202406 a join tien b on a.ma_nv = b.ma_nv
where GIAMTRU_GHTT_cNTT !=- tien;
with ten_pb as (
    select distinct ma_pb, ten_pb
    from ttkd_bsc.nhanvien_202406
)
select THANG, LOAI_TINH, ma_nv, MA_KPI, a.MA_PB, b.ten_pb,TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN,SL_PHIEUTON, TYLE
from ttkd_Bsc.tl_Giahan_tratruoc a
    join ten_pb b on a.ma_pb = b.ma_pb
    where  loai_tinh = 'KPI_PB' AND MA_KPI=  'HCM_CL_TONDV_001';
select* from ttkd_Bsc.tl_Giahan_tratruoc where loai_Tinh = 'KPI_PB' and sl_phieuton is not null;
commit;
update ttkd_Bsc.tl_giahan_Tratruoc set tyle = round(sl_phieuton*100/tong,2) 
where thang = 202406 and loai_tinh in ('KPI_PB','KPI_TO') and ma_kpi = 'HCM_CL_TONDV_001';

update TTKD_BSC.bangluong_kpi_202406 a 
set HCM_CL_TONDV_001 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to 
and ma_pb = a.ma_donvi
and ma_kpi = 'HCM_CL_TONDV_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_CL_TONDV_001')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) --AND ma_TO ='VNP0702407'
;
create table bangluong_kpi_202406 as select* from ttkd_Bsc.bangluong_kpi_202406; where sdt = '0919171777';
commit;
rollback;
SELECT* FROM ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB'and ma_kpi = 'HCM_SL_COMBO_006'   ;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202406 a 
set HCM_CL_TONDV_001 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_CL_TONDV_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_CL_TONDV_001') and thang_kt is null and MA_VTCV = a.MA_VTCV) ;
select a.MA_NV, a.MA_NV_HRM, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_DONVI, a.TEN_DONVI, a.MA_TO, a.TEN_TO , a.HCM_CL_TONDV_001
from ttkd_Bsc.bangluong_kpi_202406 a join bangluong_kpi_202406 b on a.ma_nv = b.ma_nv
and a.HCM_CL_TONDV_001 != b.HCM_CL_TONDV_001;
select a.ma_gd, c.kenhthu,d.ht_Tra  from tmp_kt a join css_hcm.phieutt_hd b on a.ma_gd =b.ma_gd
    left join css_hcm.kenhthu c on b.kenhthu_id = c.kenhthu_id
    left join css_hcm.hinhthuc_Tra d on b.ht_Tra_id = d.ht_Tra_id;
    select* from ttkd_Bsc.ct_dongia_Tratruoc where ma_Tb = 'thuphuong_fb26';
    select* from ;
    select* from ttkd_Bsc.nhanvien_202406 where ma_Nv in ('CTV043447','VNP017364','VNP017805','VNP017226','VNP017163');
select* from ttkd_Bsc.ct_Dongia_tratruoc where thang = 202405 and ma_nv = 'VNP019523';
select* from ttkd_bsc.nhanvien_202406 where ma_Nv = 'VNP019523';

select* from css_Hcm.db_Thuebao where ma_Tb= 'quct6616819';
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202406 and thuebao_id not in (Select THUEBAO_ID from 
ttkd_Bsc.ct_dongia_Tratruoc where thang = 202406 and loai_Tinh ='DONGIATRA_OB');
select* from css_Hcm.dichvu_vt where  dichvuvt_id in (1,11,4,12);
select* from css_hcm.loaihinh_Tb where loaitb_id in(18, 58, 59, 61, 171, 210, 222, 224) ;
select* from ttkd_Bsc.nhanvien_202406 where ten_nv like ;
select* from ttkd_bsc.ct_Bsc_giahan_Cntt where thang = 202406 and ma_Tb in ('hcm_ca_00042834',
'hcm_ca_00042870',
'hcm_ca_00057807',
'hcm_ca_00057810',
'hcm_ca_00057811',
'hcm_ca_00057812',
'hcm_ca_00057814',
'hcm_ca_00057817',
'hcm_ca_00058082',
'hcm_ca_00058274',
'hcm_ca_00058482',
'hcm_ca_00058483',
'hcm_ca_00058543',
'hcm_ca_00078692',
'hcm_ca_00078770',
'hcm_ca_00078772',
'hcm_ca_00078868',
'hcm_ca_00078869',
'hcm_ca_00079317',
'hcm_ca_00079538',
'hcm_ca_00079754',
'hcm_ca_00041122',
'hcm_smartca_00005004',
'hcm_ca_00058887',
'hcm_ca_00058888',
'hcm_ca_00058889 ',
'hcm_smartca_00016857',
'hcm_ivan_00009938',
'hcm_ca_ivan_00027058',
'ca_ivan_00013306',
'hcm_ca_00045422',
'hcm_ca_00046075',
'hcm_tmvn_00004937',
'hcm_tmvn_00004938',
'hcm_tmvn_00004939',
'hcm_tmvn_00004940',
'hcm_tmvn_00004941',
'hcm_tmvn_00000643',
'hcm_tmvn_00001060');
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang  =202406 and ma_Tb in ('hcm_ca_00042834',
'hcm_ca_00042870',
'hcm_ca_00057807',
'hcm_ca_00057810',
'hcm_ca_00057811',
'hcm_ca_00057812',
'hcm_ca_00057814',
'hcm_ca_00057817',
'hcm_ca_00058082',
'hcm_ca_00058274',
'hcm_ca_00058482',
'hcm_ca_00058483',
'hcm_ca_00058543',
'hcm_ca_00078692',
'hcm_ca_00078770',
'hcm_ca_00078772',
'hcm_ca_00078868',
'hcm_ca_00078869',
'hcm_ca_00079317',
'hcm_ca_00079538',
'hcm_ca_00079754');
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202406 and ma_Tb in ('hcm_ca_00041122',
'hcm_smartca_00005004',
'hcm_ca_00058887',
'hcm_ca_00058888',
'hcm_ca_00058889 ',
'hcm_smartca_00016857',
'hcm_ivan_00009938',
'hcm_ca_ivan_00027058',
'ca_ivan_00013306',
'hcm_ca_00045422',
'hcm_ca_00046075',
'hcm_tmvn_00004937',
'hcm_tmvn_00004938',
'hcm_tmvn_00004939',
'hcm_tmvn_00004940',
'hcm_tmvn_00004941',
'hcm_tmvn_00000643',
'hcm_tmvn_00001060');