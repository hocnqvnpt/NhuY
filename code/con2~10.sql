update ct_Bsc_chungtu f
set tinh_Bsc = 0, tinh_dongia = 0
where exists  (
select 1 --distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.chungtu_chot b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 d
where a.chungtu_id=c.chungtu_id
and c.ma_ct = b.ma_chungtu
and c.chungtu_id = d.chungtu_id
and a.loai_db_id=3  
and b.ma_tt=d.ma
and c.hoantat=1
and b.tra_truoc=0
and a.nhanvien_id is null 
and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 group by chungtu_id having count(*) =1)
and c.nhandien_thanhtoan is not null --1574 989
and a.fcheck=0 AND A.CHUNGTU_ID = F.CHUNGTU_ID
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt)
)
;
ROLLBACK;
COMMIT;
select * from ttkd_Bsc.bangluong_kpi_202407 where hcm_tb_giaha_026 is not null;
select* from ttkd_bsc.blkpi_dm_to_pgd where thang =202407 and dichvu is null;
select a.*, ma_vtcv, ten_vtcv from ttkd_Bsc.ct_Dongia_tratruoc a 
join ttkd_Bsc.nhanvien_202406 b on a.ma_nv = b.ma_nv 
where thang = 202406 and loai_tinh = 'DONGIATRA_OB' and ipcc = 0 and ma_vtcv = 'VNP-HNHCM_BHKV_49';
select * from ttkd_Bsc.bangluong_kpi_202407 where HCM_CL_TONDV_001 is not null;
select * from ttkd_Bsc.bangluong_kpi_202407 where HCM_SL_COMBO_006 is not null;
SELECT DISTINCT LOAI_tINH, MA_KPI FROM TTKD_BSC.TL_gIAHAN_TRATRUOC WHERE THANG = 202407 AND  ma_kpi in ('HCM_SL_COMBO_006')  ;
select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') AND THANG = 202407;
select sum(tien_thuyetphuc*heso_chuky*heso_dichvu) from ttkd_bsc.ct_dongia_Tratruoc where thang = 202406 and ipcc = 0 and ma_Nv in (select ma_Nv from ttkd_bsc.nhanvien_202406 
where ma_Vtcv = 'VNP-HNHCM_BHKV_49');
select* from ttkd_Bsc.nhanvien_202406;