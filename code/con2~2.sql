select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_022' and thang_kt is null;

select * from css_Hcm.hd_khachhang where ma_gd = 'HCM-TD/00661698';
rollback;
select thuebao_id from css_hcm.db_Thuebao where ma_Tb = 'nguyenvanthuong21'
select* from css_hcm.db_datcoc where thuebao_id = 7460957;
select* from css_hcm.hd_Thuebao where hdkh_id = 21343381
select* from css_hcm.khuyenmai_hdtb where hdtb_id = 23591078-- thuebao_dc_id = 4840088;
SELECT* FROM TTKD_bSC.NHANVIEN_202402 WHERE ma_Nv in ('VNP017351','VNP017594','VNP017875')
select* from ttkd_Bsc.tl_
update ;
SELECT * from ttkd_bsc.tl_giahan_tratruoc
where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023' and ma_Nv in ('VNP017351','VNP017594','VNP017875');

select* from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where thang_KT IS NULL and  ma_kpi = 'HCM_TB_GIAHA_023'
UPDATE ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
SET THANG_KT = 202401
where thang_KT IS NULL and  ma_kpi = 'HCM_TB_GIAHA_023' AND MA_VTCV IN ('VNP-HNHCM_KHDN_2','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_4')
commit;
update ttkd_bsc.tl_giahan_tratruoc c

select distinct loai_tinh, ma_kpi from ttkd_bsc.ct_dongia_Tratruoc where thang = 202401-- group by thuebao_id having count (thuebao_id) > 1
set ma_to = (select ma_to from ttkd_bsc.nhanvien_202402 where ma_nv= c.ma_nv)
--select* from ttkd_bsc.tl_giahan_tratruoc c
where 
exists (select * from ttkd_bsc.tl_giahan_tratruoc A join ttkd_bsc.nhanvien_202402 b on a.ma_nv = b.ma_nv where thang =202402 and a.ma_to <> b.ma_to
and ma_kpi = 'HCM_TB_GIAHA_023' 
and a.ma_pb in ('VNP0702500','VNP0702400','VNP0702300') and c.ma_nv= a.ma_nv )
and thang = 202402 and ma_pb in ('VNP0702500','VNP0702400','VNP0702300') and ma_kpi = 'HCM_TB_GIAHA_023'
;

select distinct ma_vtcv 
from ttkd_bsc.tl_giahan_tratruoc A join ttkd_bsc.nhanvien_202402 b on a.ma_nv = b.ma_nv where thang =202402   and ma_kpi = 'HCM_TB_GIAHA_026' 
and loai_tinh = 'KPI_NV'


    rollback;

select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023' AND MA_PB in ('VNP0702300','VNP0702400','VNP0702500')