    --- xoa du lieu nhan dien duoc 
drop table final_Ctu;
select* from ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb w
select distinct ma_chungtu from ct_bsc_Chungtu where ma_Nv = 'CTV078251' and tinh_dongia = 0 and tinh_bsc = 1;
select* from ttkd_bsc.nhanvien_202406 where ten_Nv like '%T?ng';
select* from ttkdhcm_ktnv.nhanvien where mail_vnpt = 'phuongtt.hcm@vnpt.vn';
select* from  ct_Bsc_chungtu where thang = 202410 and tinh_Bsc = 1 and tinh_dongia = 0;
update ct_Bsc_Chungtu set tinh_dongia = 1 where tinh_bsc = 0;
update ct_Bsc_Chungtu set tinh_bsc = 1 where tinh_bsc = 0;
commit;
select* from ct_Bsc_Chungtu where tinh_bsc = 0;
DROP TABLE final_Ctu;
select* from final_ctu where thang =202407;
alter table final_ctu add thang number(6);
update final_ctu set thang = 202407;
create table final_Ctu as ;
delete from final_Ctu where thang = 202410;
insert into final_Ctu
---- lay thong tin nguoi thanh toan
with 
--nguoi_Tt as (
--select distinct chungtu_id, ma_Nv, ma from (
--    select chungtu_id,a.nhanvien_id, b.ma_nv, ma
--    from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 a
--        join admin_hcm.nhanvien_onebss b on a.nhanvien_id = b.nhanvien_id
--    union all
--        select chungtu_id,a.nhanvien_id, b.ma_nv, ma
--    from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a
--        join admin_hcm.nhanvien_onebss b on a.nhanvien_id = b.nhanvien_id)
--)
--, loai as (
--     select  a.ma_ct,a.nd_ct,a.nhandien_thanhtoan ID600_nhan_dien,b.ma_tt ma_gach_one,a.tien_ct,b.tongtra,decode(a.hoantat,1,'Hoan_tat') trangthai 
--    from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a,nhuy.chungtu_chot b 
--    where a.ma_ct = b.ma_chungtu
--    and b.tra_truoc=0
--    and a.nhandien_thanhtoan = b.ma_tt
--    and a.hoantat = 1
--    and a.tien_ct = b.tongtra
--    and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt)
--    and not exists (Select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id = a.chungtu_id)
--)
--, tt_chungtu as (
--    select a.*, b.chungtu_id--, case when ma_gd in (select ma_Gd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where t)
--    from chungtu_chot a
--       join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.ma_chungtu = b.ma_ct
--    where b.hoantat = 1
--)
--
--, tt_chungtu as (
--    select a.*, b.chungtu_id, c.ma_Nv nv_Gach , case
--        when nvl(a.ma_gd,'s') in (select ma_Gd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202407 and tien_khop = 1) then 0 
--                    when a.ma_Gd is not null and c.ma_nv not in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') then 0
--                    when b.chungtu_id in (select chungtu_id from loai ) then 0
--                    else 1 end tinh_dongia
--                    , case when b.chungtu_id in (select chungtu_id from loai ) then 0 else 1 end tinh_bsc
--
--    from chungtu_chot a
--       join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.ma_chungtu = b.ma_ct
--       join nguoi_tt c on b.chungtu_id =  c.chungtu_id and nvl(a.ma_tt, a.ma_gd)  = c.ma
--
--    where b.hoantat = 1 --and c.ma_nv not in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') 
--) 
--,
chot as (select nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia,
        COUNT(DISTINCT CASE WHEN tra_truoc = 0 and tinh_dongia = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_dongia
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc,
        COUNT(DISTINCT CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_bsc
        from ct_Bsc_Chungtu ct
        where thang = 202410
        group by nv_Gach, ma_chungtu
) 
, final as (
    select a.*, case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_dongia > 0 and sl_matb_dongia > 0 then 1.5
                    when sl_matb_dongia >= 5 or sl_magd_dongia > 1 then 1.5
                    when sl_matb_dongia between 1 and 4 or sl_magd_dongia = 1 then 1
                    else null end heso_dongia, 4000 dongia
                ,case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_bsc > 0 and sl_matb_bsc > 0 then 1.5
                    when sl_matb_bsc >= 5 or sl_magd_bsc > 1 then 1.5
                    when sl_matb_bsc between 1 and 4 or sl_magd_bsc = 1 then 1
                    else null end heso_bsc, 202410 thang
    from chot a
)
select* from final;
select* from final_ctu where nvl(heso_dongia,0) != heso_Bsc and nv_Gach in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') ;
select* from ct_Bsc_chungtu where tra_truoc = 1 and tinh_bsc = 1; 
select* from ttkd_Bsc.nhanvien_202406 where ma_Nv = 'VNP017434';
select distinct ma_chungtu
from chungtu_chot where tra_truoc = 1 and ma_nv  in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') 
;
select*from final_Ctu;
select* from ct_Bsc_chungtu where tinh_Bsc = 0 and tinh_dongia = 1;
update 
select* from nhanvien_202407 where ten_Nv like '%h?m';
select* from final_ctu;
COMMIT;
SELECT* FROM ttkd_Bsc.tl_Giahan_tratruoc WHERE MA_KPI = 'HCM_SL_HOTRO_006';
insert into ttkd_Bsc.tl_Giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_PB, MA_TO, TIEN, DA_GIAHAN_DUNG_HEN) 
;
with tt as (select case when nv_gach = 'dl_mailinh'then 'VNP016966' else nv_Gach end ma_Nv , heso_Dongia, NVL(heso_Dongia,0)*dongia tien,ma_chungtu,dongia, heso_bsc
            from final_Ctu
            where heso_bsc is not null
            )
select 202407 thang, 'DON_GIA_CHUNG_TU' LOAI_TINH , 'DONGIA' MA_KPI ,TT.MA_NV, MA_PB, MA_TO, SUM(TIEN) TIEN--,count(ma_chungtu) so_chungtu_Gach,  
--,sum(heso_bsc) slct_quydoi_bsc
, sum(heso_Dongia) slct_quydoi_dongia
from tt 
    left join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202407 and donvi = 'TTKD'
where ma_pb = 'VNP0700900'
GROUP BY TT.MA_NV, MA_PB, MA_TO
having SUM(TIEN) > 0;
commit;
---- 
DELETE FROM ttkd_Bsc.tl_Giahan_tratruoc WHERE THANG = 202407 AND MA_KPI = 'HCM_SL_HOTRO_006';
insert into ttkd_Bsc.tl_Giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_PB, MA_TO,  DA_GIAHAN_DUNG_HEN) 

with tt as (select case when nv_gach = 'dl_mailinh'then 'VNP016966' else nv_Gach end ma_Nv , heso_Dongia, NVL(heso_Dongia,0)*dongia tien,ma_chungtu,dongia, heso_bsc
            from final_Ctu
            where heso_bsc is not null
            )
select 202407 thang, 'KPI_NV' LOAI_TINH , 'HCM_SL_HOTRO_006' MA_KPI ,TT.MA_NV, MA_PB, MA_TO--, SUM(TIEN) TIEN--,count(ma_chungtu) so_chungtu_Gach,  
,sum(heso_bsc) slct_quydoi_bsc
--, sum(heso_Dongia) slct_quydoi_dongia
from tt 
    left join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202407 and donvi = 'TTKD'
where ma_pb = 'VNP0700900'
GROUP BY TT.MA_NV, MA_PB, MA_TO;
having SUM(TIEN) > 0;
insert into ttkd_Bsc.tl_Giahan_tratruoc (THANG, MA_NV, MA_TO, MA_PB, MA_KPI, LOAI_TINH, DA_GIAHAN_DUNG_HEN, TIEN ) 
select thang, ma_nv, ma_to, ma_pb, 'DONGIA' MA_KPI,'DONGIA_CHUNG_TU' LOAI_TINH, SLCT_QUYDOI_DONGIA, TIEN
from final_Ctu_tien where thang = 202410;
COMMIT;

drop table final_Ctu_tien;
create table final_Ctu_tien as ;
update final_Ctu_tien set thang = 102024 where thang = 202410;
select so_Ct , thungan_Tt_id, thungan_hd_id from css_hcm.phieutt_hd where ma_Gd ='HCM-TT/02894536';
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202410 and ma_Tb in ('thai3909','hcmthai3309','kimhng93882126');
update ttkd_bsc.ct_dongia_tratruoc set nhanvien_xuathd = 'CTV078251' where thang = 202410 and ma_Tb in ('hcmthai3309','kimhng93882126');
commit;
select* from admin_hcm.nhanvien where nhanvien_id = 451341;
delete from final_Ctu_tien where thang >= 202410;
select distinct chungtu_id from ct_bsc_chungtu where  ghichu is not null and thang = 202410;
select sum(tien) from final_Ctu_tien where thang = 202410;
update  ct_Bsc_Chungtu set tinh_dongia = tinh_bsc where thang = 202410;
delete from ttkd_Bsc.ct_Dongia_tratruoc where thang = 202410 and ma_Tb='hcm_signsv_00008720';
select distinct ma_ct from ttkd_Bct.bangiao_chungtu_tinhbsc where thang=202410 or ghi_chu is not null;
---- tong hop
insert into final_Ctu_tien
with chot as (select thang,nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia,
        COUNT( CASE WHEN tra_truoc = 0 and tinh_dongia = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_dongia
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc,
        COUNT( CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_tb ELSE NULL END) AS sl_matb_bsc
        from ct_Bsc_Chungtu ct
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
commit;
select* from 
select* from ttkd_bsc.tl_giahan_Tratruoc where thang = 202410 and ma_kpi = 'HCM_SL_ORDER_001';
select* from  ct_Bsc_Chungtu ct
        where thang = 202410 ;and tinh_dongia = 2;
        alter table final_Ctu_tien add ghichu varchar2(100);
select distinct ma_Chungtu from ct_Bsc_Chungtu ct
        where thang = 202409  and NV_GACH = 'CTV078251' and tinh_Bsc= 1; TINH_DONGIA= 1 ;
--- bo sung 
--insert into final_Ctu_tien(THANG, MA_NV, TEN_NV, TEN_TO, TEN_PB, MA_PB, MA_TO, TIEN, SO_CHUNGTU_GACH, SLCT_QUYDOI_BSC, SLCT_QUYDOI_DONGIA, GHICHU);
with chot as (select thang,nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia,
     COUNT( CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS  sl_matb_dongia
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_Tt ELSE NULL END)  AS sl_magd_bsc,
       COUNT( CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.ma_tt ELSE NULL END) AS sl_matb_bsc
        from ct_Bsc_Chungtu ct
        where thang = 202410 
        group by nv_Gach, ma_chungtu,thang
) --select* from chot;
, final as (
    select a.*, case when nv_Gach like 'dl_mailinh%' then 2
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
--            where heso_bsc is not null
)
        
select  a.thang, TT.MA_NV,TEN_NV, TEN_TO,TEN_PB, MA_PB, MA_TO, SUM(TIEN) TIEN,count(ma_chungtu) so_chungtu_Gach
,sum(heso_bsc) slct_quydoi_bsc
, sum(heso_Dongia) slct_quydoi_dongia, 'bosung' ghichu
from tt 
    left join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202410 and donvi = 'TTKD'
where ma_pb = 'VNP0700900'
GROUP BY TT.MA_NV, MA_PB, MA_TO,TEN_NV, TEN_TO,TEN_PB, a.thang
having SUM(TIEN) > 0;

select* from ttkd_Bsc.nhanvien 
where ma_pb = 'VNP0700900' and  thang = 202410 and donvi = 'TTKD';
select* from css.giahhan_tt_chuyendoi@dataguard;
select* from final_Ctu where heso_dongia is not null and sl_magd_dongia > 0 and nv_Gach  in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') ;
select* from ttkd_bsc.nhanvien where ma_Nv = 'VNP017603';
update ct_BSC_chungtu c set nv_Gach = nvl((select ma_nv
                                        from  css_hcm.phieutt_hd a 
                                            join admin_hcm.nhanvien_onebss b on a.thungan_Tt_id = b.nhanvien_id
                                        where a.ma_gd = c.ma_Gd
), nv_Gach)
where tra_Truoc = 1 and ma_Gd !=  'HCM-TT/02864039';
a;
commit;
update ct_BSC_chungtu c set nv_Gach = 'CTV086814' where  ma_Gd =  'HCM-TT/02864039';
rollback;
select count(distinct chungtu_id) from ct_BSC_chungtu where tra_Truoc = 1 and tinh_dongia = 1;
commit;
update ct_BSC_chungtu set tinh_dongia = 0 where ma_Gd in (select ma_Gd from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202407 and tien_khop = 1) and tra_Truoc = 1 and tinh_dongia = 1;
update ct_BSC_chungtu c set tinh_dongia = 1 where tra_truoc = 1 and nv_Gach in ((select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940') );
update ct_BSC_chungtu c set tinh_dongia = 0 where tra_Truoc = 1 and exists  (select 1  -- a.ma_ct,a.nd_ct,a.nhandien_thanhtoan ID600_nhan_dien,b.ma_tt ma_gach_one,a.tien_ct,b.tongtra,decode(a.hoantat,1,'Hoan_tat') trangthai 
    from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a,nhuy.chungtu_chot b 
    where a.ma_ct = b.ma_chungtu
    and b.tra_truoc=0
    and a.nhandien_thanhtoan = b.ma_tt
    and a.hoantat = 1
    and a.tien_ct = b.tongtra
    and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt) and chungtu_id = c.chungtu_id);
)
rollback;
select* from ct_BSC_chungtu where tra_Truoc = 1 and tinh_dongia = 1;
select ma_gd
from  css_hcm.phieutt_hd a 
    join admin_hcm.nhanvien_onebss b on a.thungan_Tt_id = b.nhanvien_id
where  ma_gd in (select ma_Gd from ct_BSC_chungtu where tra_Truoc = 1 )
group by ma_gd having count(Distinct ma_nv) > 1 ;
select ma_nv, ten_nv
                                        from  css_hcm.phieutt_hd a 
                                            join admin_hcm.nhanvien_onebss b on a.thungan_Tt_id = b.nhanvien_id
                                        where a.ma_gd =  'HCM-TT/02864039';

select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202407 and loai_tinh = 'DONGIA_CHUNGTU';

commit;
update 
select* from final_Ctu a where nv_Gach not in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_pb = 'VNP0700900') ;and heso_dongia != heso_Bsc;

select * from ttkd_bsc.ct_dongia_Tratruoc where thang = 202407 and nhanvien_xuathd  in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and ma_To = 'VNP0700940')
;
insert into ttkd_Bsc.bangluong_kpi (thang) values (-1);
update ttkd_Bsc.bangluong_kpi 
set ma_kpi =;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 AND MA_NV = 'VNP017445'; AND MA_KPI = 'HCM_TB_GIAHA_028';
UPDATE ttkd_Bsc.tl_Giahan_Tratruoc SET THANG = 102024 where thang = 202410 and loai_tinh ='DONGIA_CHUNG_TU';
INSERT INTO ttkd_Bsc.tl_Giahan_Tratruoc (THANG, MA_NV, MA_TO, MA_PB, MA_KPI, LOAI_TINH, TIEN);
SELECT --THANG, MA_nV,MA_TO,MA_PB,'DONGIA' MA_KPI, 'DONGIA_CHUNG_TU' LOAI_tINH, 
SUM(TIEN) TIEN FROM FINAL_CTU_TIEN
 where thang = 202410 --AND MA_TO='VNP0700940'
 GROUP BY THANG;, MA_nV,MA_TO,MA_PB;
 COMMIT;