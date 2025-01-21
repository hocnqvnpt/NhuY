update 

insert into nhuy.final_Ctu_tien
with chot as (select thang,nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia,
        COUNT( CASE WHEN tra_truoc = 0 and tinh_dongia = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_dongia
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc,
        COUNT( CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_bsc
        from nhuy.ct_Bsc_Chungtu ct
        where thang = 202410
        group by nv_Gach, ma_chungtu,thang
) 
, final as (
    select thang,a.*, case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_dongia > 0 and sl_matb_dongia > 0 then 1.5
                    when sl_matb_dongia >= 5 or sl_magd_dongia > 1 then 1.5
                    when sl_matb_dongia between 1 and 4 or sl_magd_dongia = 1 then 1
                    else null end heso_dongia, 4000 dongia
                ,case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_bsc > 0 and sl_matb_bsc > 0 then 1.5
                    when sl_matb_bsc >= 5 or sl_magd_bsc > 1 then 1.5
                    when sl_matb_bsc between 1 and 4 or sl_magd_bsc = 1 then 1
                    else null end heso_bsc
    from chot a
)
,tt as (select case when nv_gach = 'dl_mailinh' then 'VNP016966' else nv_Gach end ma_Nv , heso_Dongia, NVL(heso_Dongia,0)*dongia tien,ma_chungtu,dongia, heso_bsc
            from final
            where heso_bsc is not null
)
        
select  a.thang, TT.MA_NV,TEN_NV, TEN_TO,TEN_PB, MA_PB, MA_TO, SUM(TIEN) TIEN,count(ma_chungtu) so_chungtu_Gach
,sum(heso_bsc) slct_quydoi_bsc
, sum(heso_Dongia) slct_quydoi_dongia, null
from tt 
    left join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202410 and donvi = 'TTKD'
where ma_pb = 'VNP0700900'
GROUP BY TT.MA_NV, MA_PB, MA_TO,TEN_NV, TEN_TO,TEN_PB, a.thang
having SUM(TIEN) > 0;