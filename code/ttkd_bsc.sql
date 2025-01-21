select * from ttkd_bsc.bldg_danhmuc_qldb_luong_dhanh_kh
blkpi_danhmuc_kpi_vtcv where donvi = 'tyle' and loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_053';

update bangluong_dongia_qldb
   set hcm_luong_qldb_071=round((nvl(hcm_luong_qldb_009,0)+nvl(hcm_luong_qldb_024,0)+nvl(hcm_luong_qldb_039,0)
                                +nvl(hcm_luong_qldb_052,0)+nvl(hcm_luong_qldb_062,0)+nvl(hcm_luong_qldb_070,0))
                               *(nctt/(select nctt from bldg_danhmuc_qldb_nctt where thang=202401 and ma_nv is null)))
 where thang=202401;
commit;

select distinct ma_to, ten_to from ttkd_Bsc.nhanvien_202401 where ma_pb = 'VNP0702200'
select hcm_luong_qldb_071,HCM_LUONG_QLDB_041,HCM_LUONG_QLDB_064,hcm_luong_qldb_024,hcm_luong_qldb_009,hcm_luong_qldb_052,hcm_luong_qldb_070
HCM_LUONG_QLDB_053,HCM_LUONG_QLDB_054,HCM_LUONG_QLDB_055,HCM_LUONG_QLDB_056,HCM_LUONG_QLDB_057,HCM_LUONG_QLDB_058,HCM_LUONG_QLDB_059
,HCM_LUONG_QLDB_060,HCM_LUONG_QLDB_061,HCM_LUONG_QLDB_062
from ttkd_bsc.bangluong_dongia_qldb  where thang = 202401  and ma_vtcv = 'VNP-HNHCM_BHKV_2' -- and manv = 'VNP000000'  ;
-- 53
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_053 =  18.75
where manv = 'VNP000000' and thang = 202402  ;
-- 54
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_054 =  round((select LUONG_DHANH_KHOANH from ttkd_Bsc.bldg_danhmuc_qldb_luong_dhanh_kh where a.mapb= ma_pb)*
            (18.75/100),0)
where manv = 'VNP000000' and thang = 202402  ;
-- 55 
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_055 =  4783
where manv = 'VNP000000' and thang = 202402  ;
-- 56
update ttkd_bsc.bangluong_dongia_qldb a1 set HCM_LUONG_QLDB_056 =  ( select round(sum(a.heso_giao), 0) from ttkd_bsc.tl_giahan_tratruoc a
                                                            join ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach b on b.thang = a.thang and a.ma_pb = b.ma_pb
                            where a.MA_KPI in ('HCM_TB_GIAHA_022') and a.loai_tinh = 'KPI_NV'
                                                and a.thang = 202402 and b.manv_pgd = a1.manv
                     )
-- select * from backup_tonghop_dongia a1
WHERE  exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = 'HCM_LUONG_QLDB_056')
            and exists (select 1 from ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach x2 where x2.thang = a1.thang and x2.manv_pgd = a1.MANV)
and manv = 'VNP000000' and thang = 202402  ;
-- 57
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_057 =  1
where manv = 'VNP000000' and thang = 202402  ;
-- 58
update ttkd_bsc.bangluong_dongia_qldb a1 set HCM_LUONG_QLDB_058 = ( select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                from ttkd_bsc.tl_giahan_tratruoc a 
                                                join ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach b on b.thang = a.thang and a.ma_pb = b.ma_pb
                where a.MA_KPI in ('HCM_TB_GIAHA_022') and a.loai_tinh = 'KPI_NV'
                                    and a.thang = a1.thang and b.manv_pgd = a1.manv
         )
--select * from TONGHOP_DONGIA a1
WHERE
                 exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                                and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = 'HCM_LUONG_QLDB_057')
                and exists (select * from ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach x2 where x2.thang = a1.thang and x2.manv_pgd = a1.MANV)
            and manv = 'VNP000000' and thang = 202402  ;

-- 59
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_059 =  round(HCM_LUONG_QLDB_058*100/65,0)
where manv = 'VNP000000' and thang = 202402;
-- 60
commit;
update ttkd_bsc.bangluong_dongia_qldb a1 set HCM_LUONG_QLDB_060 = (SELECT sum(GIATRI)
                          FROM nhuy.backup_tonghop_dongia a
                          WHERE exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho is null and giamdoc_phogiamdoc is null
                                                                                    and thang_kt is null and MA_VTCV = a.MA_VTCV and ma_kpi = a.ma_kpi)
                                            and a.ma_pb = a1.mapb and a.thang = a1.thang and 'HCM_LUONG_QLDB_060' = a.ma_kpi
                     )
       --              select * from TONGHOP_DONGIA a1
WHERE  exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                    and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = 'HCM_LUONG_QLDB_060')
and exists (select 1 from ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach x2 where x2.thang = a1.thang and x2.manv_pgd = a1.MANV)
and manv = 'VNP000000' and thang = 202402;
-- 61
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_061 =  0
where manv = 'VNP000000'  and thang = 202402;
-- 62
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_062 =  
round((HCM_LUONG_QLDB_054 * HCM_LUONG_QLDB_057 * HCM_LUONG_QLDB_059/100) - (HCM_LUONG_QLDB_060 * HCM_LUONG_QLDB_061),0)
where manv = 'CTV085345'  and thang = 202402;
-- 62
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_062 = case when exists (select * from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  >= 0.9 and manv = a.manv)
            then greatest(0.5 * nvl(HCM_LUONG_QLDB_054, 0)
                                            , HCM_LUONG_QLDB_062
                                        )
        when exists (select 1 from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  < 0.9 and manv = a.manv)
                then greatest(0.2 * nvl(HCM_LUONG_QLDB_054, 0)
                                            , HCM_LUONG_QLDB_062
                                        )
        end
    where manv = 'CTV085345'  and thang = 202402;

update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_062 =  greatest(0.5 * nvl(HCM_LUONG_QLDB_054, 0)
                                            , HCM_LUONG_QLDB_062
                                        )
--        when exists (select 1 from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  < 0.9 and manv = a.manv)
--                then greatest(0.2 * nvl(HCM_LUONG_QLDB_054, 0)
--                                            , HCM_LUONG_QLDB_062
--                                        )
--        end
    where manv = 'CTV085345'  and thang = 202402;

commit;
rollback;
update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_055 = null where manv = 'VNP000000';

update ttkd_bsc.bangluong_dongia_qldb a set HCM_LUONG_QLDB_056 = null
where manv = 'VNP000000' and thang = 202401;
select* from ttkd_bsc.bangluong_dongia_qldb where thang = 202401
select ma_gd, ma_Tb, tien_thanhtoan from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202401 and loaitb_id not in (147,148)
