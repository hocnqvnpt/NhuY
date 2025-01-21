create table khac_sdt as select a.ma_tb,to_number(to_char(ngay_kt_mg,'yyyymm')) thang_kt,b.ma_tt, d.ma_kh, b.so_Dt sdt_tt, d.so_dt sdt_kh 
from nhuy.ds_Giahan_tratruoc2 a
    left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
    left join css_hcm.db_thanhtoan b on c.thanhtoan_id = b.thanhtoan_id
    left join css_hcm.db_khachhang d on c.khachhang_id = d.khachhang_id
where to_number(to_char(ngay_kt_mg,'yyyymm')) between 202406 and 202412 and nvl(b.so_Dt,0) != nvl(d.so_dt,0);
create table cung_sdt as 
select a.ma_tb,to_number(to_char(ngay_kt_mg,'yyyymm')) thang_kt, b.ma_tt,d.ma_kh, b.so_Dt sdt_tt, d.so_dt sdt_kh 
from ds_Giahan_tratruoc2 a
    left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
    left join css_hcm.db_thanhtoan b on c.thanhtoan_id = b.thanhtoan_id
    left join css_hcm.db_khachhang d on c.khachhang_id = d.khachhang_id
where to_number(to_char(ngay_kt_mg,'yyyymm')) between 202406 and 202412 and nvl(b.so_Dt,0) = nvl(d.so_dt,0);
drop table cung_sdt;
select* from ttkd_bsc.bangluong_dongia_202404 where ten_Nv like '%Vinh' ;
select b.so_Dt from css_hcm.dB_thuebao a join css.v_db_Thanhtoan@dataguard b on a.thanhtoan_id = b.thanhtoan_id where a.ma_Tb = 'tanquoc54';
31196625;
with tien as 
(
    select ma_Nv, tien_dongia
    from ttkd_bsc.dongia_vttp
    where thang = 202404
--    where  loai_tinh in ('DONGIA_OB_VTTP','DONGIA_VTTP') and (tien_thuyetphuc > 0 or tien_xuathd > 0)
    union all
    select MANV_XHDON, TIEN_XHDON
    from ttkd_bsc.dongia_vttp
    where thang = 202404
--    where  loai_tinh in ('DONGIA_OB_VTTP','DONGIA_VTTP') and (tien_thuyetphuc > 0 or tien_xuathd > 0)
)
select   a.ma_nv, b.ten_nv, b.ten_pb,sum(tien_dongia) tien 
from tien a
join ttkd_bsc.nhanvien_vttp b on a.ma_nv = b.ma_nv where  b.thang = 202404 
and tien_dongia > 0
group by  a.ma_nv, b.ten_nv, b.ten_pb
order by b.ten_pb;
delete from ttkd_bsc.dongia_vttp where thang = 202404;
commit;
insert into  ttkd_Bsc.dongia_vttp
select a.thang, a.loai_tinh, 'DONGIA' MA_KPI, A.MA_NV, A.THUEBAO_ID, A.MA_tB, A.TIEN_DC_cu, null, null,a.ma_nv, a.sothang_Dc, null, null, a.dongia, a.dthu, a.ngay_tt,null, 
a.khachhang_id, a.heso_dichvu, a.tien_khop , a.tien_thuyetphuc, b.ten_nv, b.ten_to, b.ten_pb, c.donvi_id, a.heso_chuky, a.nhanvien_xuathd, a.tien_xuathd
from dongia_ob_final a
    join ttkd_bsc.nhanvien_vttp b on a.ma_nv = b.ma_nv and a.thang = b.thang
    left join admin_Hcm.donvi c on b.ma_pb = c.ma_dv;
    where heso_dichvu = 1 and heso_chuky = 1 and ;
with luyke as (
select  a.* , to_number(to_char(b.ngay_kt,'yyyymm')) thang_ktdc_moi
from ttkd_bsc.ct_bsc_giahan_cntt a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    where loaitb_id not in (147,148) and ((thang between 202401 and 202404
    and thang_ktdc_cu = thang) or thang_ktdc_cu = 202405) and tien_khop is null and  a.thang_ktdc_Cu < to_number(to_char(b.ngay_kt,'yyyymm'))
) 
, chot as (
select A.*, NULL DD from ttkd_bsc.ct_bsc_giahan_cntt A
    where loaitb_id not in (147,148) and ((thang between 202401 and 202404
    and thang_ktdc_cu = thang) or thang_ktdc_cu = 202405) AND THUEBAO_ID NOT IN (SELECT THUEBAO_ID FROM LUYKE)
UNION  ALL
SELECT* FROM LUYKE
)
select thang_ktdc_cu, sum(case when tien_khop = 1 or dd is not null then 1 else 0 end) da_giahan, 'luyke' nguon

    ,count(distinct thuebao_id) giao
    ,round(sum(case when tien_khop = 1 or dd is not null then 1 else 0 end)*100 / count(distinct thuebao_id),2) tyle
from chot 
group by thang_ktdc_cu;
union all
select  
--thang_ktdc_cu, 
sum(case when tien_khop = 1 then 1 else 0 end) da_giahan--, 'bsc' nguon

    ,count(distinct thuebao_id) giao
    ,round(sum(case when tien_khop = 1  then 1 else 0 end)*100 / count(distinct thuebao_id),2) tyle
from ttkd_bsc.ct_bsc_giahan_cntt A
where loaitb_id not in (147,148) and ((thang between 202401 and 202404
    and thang_ktdc_cu = thang) or thang_ktdc_cu = 202405) 
group by thang_ktdc_cu
;

select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202404 and thang_ktdc_cu = 202404 and loaitb_id not in (147,148);