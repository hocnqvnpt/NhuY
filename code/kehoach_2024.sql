select* from ds_hethan_2024
DROP TABLE ds_2024_nvc;
create table ds_2024_nvc as

with hddc as (
    select  g.hdtb_id, h.thuebao_id,h.rkm_id ,   h.ngay_bddc , h.ngay_ktdc ,  'datcoc' nguon, h.nhom_Datcoc_id, to_Number(to_char(h.ngay_ktdc,'yyyymm'))thang_kt,
    a.huong_dc
        from css_hcm.hdtb_datcoc g
        join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
        left join css_hcm.ct_khuyenmai a on h.chitietkm_id = a.chitietkm_id
    where h.hieuluc = 1 and ttdc_id = 0  and loaitb_id in (58,59)  
    and g.cuoc_dc is not null 
    
    union all
    select  b.hdtb_id,b.thuebao_id, b.rkm_id ,   b.ngay_bddc , b.ngay_ktdc ,  'khuyenmai' nguon, c.nhom_Datcoc_id,  to_Number(to_char(b.ngay_ktdc,'yyyymm'))thang_kt
    ,c.huong_dc
        from css_Hcm.khuyenmai_dbtb b
        left join css_hcm.ct_khuyenmai c on b.chitietkm_id = c.chitietkm_id
    where hieuluc = 1 and ttdc_id = 0 and b.datcoc_csd > 0  and  loaitb_id in (58,59)
)-- select rkm_id from hddc group by rkm_id having count (rkm_id) > 1 ,
,tt_ttoan as (
    select a.hdtb_id, b.ngay_Tt,b.kenhthu_id, b.ht_tra_id,c.kenhthu,d.ht_Tra,ma_gd
    from css_hcm.ct_phieutt a 
        join css_hcm.phieutt_hd b on a.phieutt_id = b.phieutt_id
        join css_hcm.kenhthu c on b.kenhthu_id = c.kenhthu_id
        join css_hcm.hinhthuc_tra d on b.ht_Tra_id = d.ht_tra_id
    where a.khoanmucTt_id =11 and b.trangthai = 1 and a.tien > 0 and b.ht_Tra_id <> 6 and b.kenhthu_id <> 6 
),
goi_dadv as (
    select nhomtb_id, thuebao_id, goi_id
    from css_hcm.bd_goi_dadv a
    where trangthai = 1 and exists (select 1 from css_hcm.goi_dadv_lhtb where goi_id = a.goi_id 
    group by GOI_ID having count(distinct loaitb_id)>1)   and goi_id < 10000 and goi_id not between  1715 and 1726
),
--create table sldc as 
--(
--    select thuebao_id, sum(so_luong) as solan_dc from (
--    select thuebao_id, count(rkm_id) as so_luong
--    from css_hcm.db_datcoc where hieuluc = 1 and ttdc_id = 0
--    and thang_kt  >= thang_Bd + 2
--    group by thuebao_id
--    union all 
--    select thuebao_id, count(rkm_id) as so_luong
--    from css_Hcm.khuyenmai_dbtb where hieuluc = 1 and ttdc_id = 0
--    and thang_kt >= thang_Bd + 2
--    group by thuebao_id
--    )
--    group by thuebao_id
--),
slbh as (
    select thuebao_id, count(baohong_id)
    from baohong_hcm.baohong where to_number(to_char(ngay_bh,'yyyy')) = 2023
    group by thuebao_id
),
luuluong as (
select ma_tb, sum(luu_luong)/1000 luuluong from bcss_hcm.thftth partition for (20231101) group by ma_tb
)
--,bangtan as (select a.thuebao_id, case when vattu_id in  (892, 891, 893) then 'I-240W'
--                    when vattu_id in  (890, 889,  896, 884,  895, 883) then 'HG8xx'
--                    when vattu_id in (888, 14914, 14123, 14890) then 'GWxx0-HiB'
--                    when vattu_id in (894, 886, 885, 11713, 887, 14699, 13781, 14125, 14098, 14124) then 'GWxx0'
--                    when vattu_id in (16456) then 'F671Y-HiB'
--        end ONT_TYPE
--    from hocnq_ttkd.dulieu_kn_ont a where rnk = 1
--)
select hddc.hdtb_id,hddc.thuebao_id,ds.MA_TB, ds.rkm_id,hddc.ngay_bddc,hddc.ngay_ktdc,hddc.nhom_Datcoc_id,hddc.thang_kt, a.ngay_Tt, a.kenhthu_id,a.ht_tra_id
    ,a.kenhthu,a.ht_tra, b.nhomtb_id, 1 fiber_donle, hddc.huong_Dc so_thangdc,
    case when to_Number(to_char(hddc.ngay_bddc,'yyyymm')) = to_Number(to_char(a.ngay_tt,'yyyymm')) + 1 then 1 else 0 end tt_thang_t,
    sldc.solan_dc, c.luuluong, ds.khachhang_id
from ds_hethan_2024 ds 
    left join hddc  on ds.rkm_id = hddc.rkm_id
    left join tt_ttoan a on hddc.hdtb_id = a.hdtb_id
    left join goi_dadv b on hddc.thuebao_id = b.thuebao_id
    left join sldc on hddc.thuebao_id = sldc.thuebao_id
    left join luuluong c on ds.ma_tb = c.ma_tb

--    left join solan_hu d on hddc.thuebao_id = d.thuebao_id
;
DROP TABLE nvc_ghtt_2024;
CREATE TABLE nvc_ghtt_2024 AS 
with bangtan as (select a.thuebao_id, case when vattu_id in  (892, 891, 893) then 'I-240W'
                    when vattu_id in  (890, 889,  896, 884,  895, 883) then 'HG8xx'
                    when vattu_id in (888, 14914, 14123, 14890) then 'GWxx0-HiB'
                    when vattu_id in (894, 886, 885, 11713, 887, 14699, 13781, 14125, 14098, 14124) then 'GWxx0'
                    when vattu_id in (16456) then 'F671Y-HiB'
        end ONT_TYPE
    from hocnq_ttkd.dulieu_kn_ont a where rnk = 1
) 
select a.*, case when b.ont_type like '%HiB' then 1 when b.ont_type is null then 1 else 0 end ONT_HiB
from ds_2024_nvc a
    left join solan_hu d on a.thuebao_id = d.thuebao_id
    left join bangtan b on a.thuebao_id = b.thuebao_id 
    ;
    
--drop table ds_2024_nvc
-- FIBER DON LE
update nvc_ghtt_2024 a set fiber_donle = 0
--- select * from ds_giahan_tratruoc a
    where fiber_donle = 1
    and exists (select 1 from css.v_db_thuebao@dataguard xj, css.v_db_adsl@dataguard xk 
        where xj.thuebao_id = xk.thuebao_id and xj.loaitb_id in (61, 171, 210, 222, 224)
                            and xk.matb_tn = a.ma_tb)
;
update nvc_ghtt_2024 a set fiber_donle = 0
--- select * from ds_giahan_tratruoc a
    where fiber_donle = 1
    and exists (select 1 from css.v_db_thuebao@dataguard xj
            where xj.loaitb_id in (61, 171, 210, 222, 224)
            and xj.khachhang_id = a.khachhang_id)
;
commit;
select * from hocnq_ttkd.nvc_ghtt_2024 where rkm_id not in (select rkm_id from nvc_ghtt_2024 )
select rkm_id from hocnq_ttkd.nvc_ghtt_2024 where rkm_id not in (select rkm_id from nvc_ghtt_2024 )
select cuoc_Dc from css_Hcm.hdtb_Datcoc where thuebao_dc_id in (
select hieuluc,ttdc_id, ngay_ktdc,ngay_tt, loaitb_id,rkm_id from css_hcm.db_datcoc where rkm_id in (select rkm_id from hocnq_ttkd.nvc_ghtt_2024 where rkm_id not in (select rkm_id from nvc_ghtt_2024 ))
))
select* from final where thuebao_id  = 1235786
create table final as
Select * from(
Select a.*, row_number() over(partition by thuebao_id order by rkm_id desc) ra
From nvc_ghtt_2024 a)
Where ra = 1 and thuebao_id  =10385711
select * from ds_hethan_2024 where thuebao_id = 10705836
select ngay_tt,rkm_id from nvc_ghtt_2024 where rkm_id in (
select rkm_id,thuebao_id, ngay_bddc,ngay_ktdc from ds_hethan_2024 where thuebao_id not in (select thuebao_id from hocnq_ttkd.nvc_ghtt_2024 ) and
rkm_id not in (select rkm_id from hocnq_ttkd.nvc_ghtt_2024))
select rkm_id from nvc_ghtt_2024 group by rkm_id having count(rkm_id) >1