insert into final_ctu_Tien ;
flashback table final_ctu_Tien to before drop ;
drop table tinh_Bsc_ctu;
create table tinh_Bsc_ctu as 
with chot as (select thang,nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia
      ,  COUNT(distinct CASE WHEN tra_truoc = 0 and tinh_dongia = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_dongia
--        , ct.sl_tb sl_matb_dongia , ct.sl_tb sl_matb_bsc
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc
      ,  COUNT( distinct CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_bsc
        from nhuy.ct_Bsc_Chungtu_temp ct
        where thang = 202410 and nvl(ghichu,'a') !='bo sung theo ra soat PNVC'
        group by nv_Gach, ma_chungtu,thang--,sl_tb
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
, sum(heso_Dongia) slct_quydoi_dongia
from tt 
    left join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202410 and donvi = 'TTKD'
where ma_pb = 'VNP0700900'
GROUP BY TT.MA_NV, MA_PB, MA_TO,TEN_NV, TEN_TO,TEN_PB, a.thang
having SUM(TIEN) > 0;
select distinct ghichu from nhuy.ct_Bsc_Chungtu_temp ct
        where  ghichu is not null and tra_Truoc = 0;
commit;
update ct_Bsc_Chungtu_temp a set tinh_Dongia = 0 where exists (select 1 from ttkd_bct.bangiao_chungtu_tinhbsc  where thang = 202410 and ma = a.ma_gd and 
chungtu_id = a.chungtu_id and xuly_tudong = 1);
insert into tinh_Bsc_ctu 
with chot as (select thang,nv_Gach, ma_chungtu, COUNT( CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_tt ELSE NULL END) AS sl_magd_dongia
      ,  COUNT( CASE WHEN tra_truoc = 0 and tinh_dongia = 1 THEN ct.ma_tt ELSE NULL END) AS sl_matb_dongia
--        , ct.sl_tb sl_matb_dongia , ct.sl_tb sl_matb_bsc
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc
      ,  COUNT( distinct CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_tt ELSE NULL END) AS sl_matb_bsc
        from nhuy.ct_Bsc_Chungtu_temp ct
        where  ghichu ='bo sung theo ra soat PNVC' and tra_Truoc = 0
        group by nv_Gach, ma_chungtu,thang--,sl_tb
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
, sum(heso_Dongia) slct_quydoi_dongia
from tt 
    left join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202410 and donvi = 'TTKD'
where ma_pb = 'VNP0700900'
GROUP BY TT.MA_NV, MA_PB, MA_TO,TEN_NV, TEN_TO,TEN_PB, a.thang
having SUM(TIEN) > 0;
delete from final_ctu_Tien where thang = 202410;

commit;
select a.*, 't' from TINH_BSC_CTU a
union all;
select sum(tien) from final_ctu_Tien where thang = 202410
union all
--select* from ttkd_Bsc.tl_giahan_Tratruoc where ma_Nv = 'VNP016966';
select  sum(tien)  from TINH_BSC_CTU where thang = 202410;
select  *  from TINH_BSC_CTU where thang = 202410;

update ct_Bsc_Chungtu_temp a set tinh_Dongia = 0 where exists (select 1 from ttkd_bct.bangiao_chungtu_tinhbsc  where thang = 202410 and ma = a.ma_gd and 
chungtu_id = a.chungtu_id and xuly_tudong = 1)