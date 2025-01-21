select * from ttkd_Bsc.bangluong_dongia_Qldb where thang in (202403,202404);
--- hcm_
select* from TTKD_BSC.blkpi_danhmuc_kpi where thang_kt is null
select * from ttkd_Bsc.bldg_danhmuc_qldb_luong_dhanh_kh;

; -- 53
update ttkd_bsc.bangluong_Dongia_qldb a
set HCM_LUONG_QLDB_053 =  (select tytrong from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_053' and ma_vtcv = a.ma_Vtcv)
where thang = 202404;
commit;
-- 54
update ttkd_bsc.bangluong_Dongia_qldb a
set HCM_LUONG_QLDB_054 =  round((select LUONG_DHANH_KHOANH from ttkd_Bsc.bldg_danhmuc_qldb_luong_dhanh_kh where a.mapb= ma_pb)* HCM_LUONG_QLDB_053/100)
where thang = 202404;
-- 55
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_055 = (select tytrong from  TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_055' and ma_Vtcv = a.ma_vtcv)
where thang = 202404;
-- 56
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_056 =(select round(sum(heso_giao), 0) from ttkd_bsc.tl_giahan_tratruoc
                            where MA_KPI in ('HCM_TB_GIAHA_022') and loai_tinh = 'KPI_NV' and thang = a.thang and a.mapb = ma_pb)
where thang = 202404;
-- 57
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_057 = 1
where thang = 202404;
-- 58
update ttkd_Bsc.bangluong_dongia_qldb a
set hcm_luong_qldb_058 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc a 
                                                                        where a.MA_KPI in ('HCM_TB_GIAHA_022') and loai_Tinh = 'KPI_NV' and ma_pb = a.mapb)
where thang = 202404;
-- 59
update ttkd_bsc.bangluong_dongia_qldb a
set hcm_luong_qldb_059 = round(HCM_LUONG_QLDB_058*100/(select tytrong from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai = 'dongia' and thang_kt is null and ma_kpi ='HCM_LUONG_QLDB_059' and a.ma_vtcv = ma_vtcv))
where thang = 202404;
-- 60
update ttkd_Bsc.bangluong_dongia_qldb a
set hcm_luong_qldb_060 = (select round(sum(heso), 0)
                from ttkd_bsc.ct_dongia_tratruoc a1
                    join ttkd_Bsc.ct_bsc_tratruoc_moi_30day b on a1.ma_tb = b.ma_tb and a1.thang = b.thang and a1.thang = 202404
                where a1.ma_kpi = 'DONGIA' and a1.loai_tinh = 'DONGIATRA30D' and a1.dthu > 0 and a1.tien_khop > 0 and a1.manv_giao <> a1.ma_nv 
                            and a.thang = a1.thang  and a.mapb = b.ma_pb
                 )
where thang = 202404;
-- 61
update ttkd_bsc.bangluong_dongia_qldb a
set hcm_luong_qldb_061 = (select tytrong from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_061' and ma_Vtcv = a.ma_Vtcv)
where thang = 202404;
-- 62
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_062 = round((nvl(HCM_LUONG_QLDB_054,0) * nvl(HCM_LUONG_QLDB_057,0) * nvl(HCM_LUONG_QLDB_059,0)/100) - (nvl(HCM_LUONG_QLDB_060,0) * nvl(HCM_LUONG_QLDB_061,0)))
where thang = 202404;

update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_062 = case when exists (select * from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  >= 0.9 and manv = a.manv)
            then greatest(0.5 * nvl(HCM_LUONG_QLDB_054, 0),nvl(HCM_LUONG_QLDB_062,0))
        when exists (select 1 from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  < 0.9 and manv = a.manv)
                then greatest(0.2 *  nvl(HCM_LUONG_QLDB_054, 0) , nvl(HCM_LUONG_QLDB_062,0))
        end
where thang = 202404;
select sum (hcm_luong_qldb_071) from ttkd_bsc.bangluong_dongia_qldb  where thang=202404;
update ttkd_bsc.bangluong_dongia_qldb
   set hcm_luong_qldb_071=round((nvl(hcm_luong_qldb_009,0)+nvl(hcm_luong_qldb_024,0)+nvl(hcm_luong_qldb_039,0)
                                +nvl(hcm_luong_qldb_052,0)+nvl(hcm_luong_qldb_062,0)+nvl(hcm_luong_qldb_070,0))
                               *(nctt/(select nctt from bldg_danhmuc_qldb_nctt where thang=202404 and ma_nv is null)))
 where thang=202404;
 rollback;
update ttkd_Bsc.bangluong_dongia_qldb
   set hcm_luong_qldb_071=null
 where thang=202404 and hcm_luong_qldb_071=0;

commit;