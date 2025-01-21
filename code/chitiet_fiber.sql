select *  from   ttkdhcm_ktnv.ID114_FIBER_SMARTTV where taikhoanfiber ='chiemtiem_family'
SELECT thuonghieu, tocdoThuc from  css_hcm.tocdo_adsl 
FROM DBA_TAB_COLS 
WHERE column_name = 'NHOMGOI_ID'
select* from assss where dthu is not null;
1265217	hanh182	Fiber	111, ???ng Tr??ng Chinh, Ph??ng 12, Qu?n Tân Bình, TP H? Chí Minh	6323431	HCM006540024	9762571	CÔNG TY TNHH B?NH VI?N THÚ Y PET-PRO	HCM015009617	29/04/2009 00:00:00		6068513	01/11/2023 00:00:00	30/04/2024 00:00:00	2862720	6		1	473.148	G	??i Vi?n Thông C?ng Hòa - TTVT Tân Bình	TTVT Tân Bình	HCM015660	H? Thanh Duy	TB032	0916219211	0915390041	1	1	1	348781	18/10/2017 15:30:40	VP5	VP5
;
create table gd_vp as select* from (
with xi2 as (select a.thuebao_id, case when vattu_id in  (892, 891, 893) then 'I-240W'
            when vattu_id in  (890, 889,  896, 884,  895, 883) then 'HG8xx'
            when vattu_id in (888, 14914, 14123, 14890) then 'GWxx0-HiB'
            when vattu_id in (894, 886, 885, 11713, 887, 14699, 13781, 14125, 14098, 14124) then 'GWxx0'
            when vattu_id in (16456) then 'F671Y-HiB'
    end ONT_TYPE
    from hocnq_ttkd.dulieu_kn_ont a where rnk = 1 
),
kvuc as (
    select a.thuebao_id, d.ma_kv, e.ma_nv,e.ten_nv tennv_ttvt, c.ten_dv ten_tovt, c.ten_dvql ten_ttvt
    from css_hcm.dbtb_kv a
        left join css_hcm.khuvuc d on a.khuvuc_id = d.khuvuc_id 
        left join css_hcm.khuvuc_nv b on a.khuvuc_id = b.khuvuc_id 
        left join admin_hcm.nhanvien e on b.nhanvien_id = e.nhanvien_id
        left join admin_hcm.donvi c on e.donvi_id = c.donvi_id
    where b.loaikv_id = 4 and b.nhiemvu = 1 and b.loainv_id = 51 and C.donvi_cha_id IN (283453,283452,283454,283467,283455,283468,283466,283469,283451)
)
,luuluong as (
    select ma_tb, sum(luu_luong)/1000 luuluong 
    from bcss_hcm.thftth partition for (20240101) group by ma_tb ), 
sm as (
    select taikhoanfiber, row_number() over  (partition by taikhoanfiber order by kygiao desc) rnk
    from  ttkdhcm_ktnv.ID114_FIBER_SMARTTV 
)
select 
    a.thuebao_id, a.ma_tb, lh.loaihinh_tb ,a.diachi_ld, b.thanhtoan_id, b.ma_tt, c.khachhang_id,c.ten_kh, c.ma_kh , a.ngay_sd, a.ngay_td, km.rkm_id,km.ngay_bddc, km.ngay_ktdc 
    ,km.cuoc_dc,ct.huong_dc,  th.tenthoihan thoihan,  case when xi2.ont_type like '%HiB' then 1 when xi2.ont_type is null then 1 else 0 end ONT_HiB,  ll.luuluong
    ,decode(b.httt_id, 2, 'H', 5, 'H', 'G') HT_thutien, kv.ten_tovt, kv.ten_ttvt,kv.ma_nv manv_ttvt, kv.tennv_ttvt,kv.ma_kv, b.so_dt sdt_tt, c.so_dt sdt_kh
    ,case when sm.taikhoanfiber is not null then 1 else 0 end sd_smarttv, lkh.khdn, km.nhom_datcoc_id
    ,goi.nhomtb_id, goi.ngay_dk, dm.ma_goi,dm.ten_goi
from css_hcm.db_Thuebao a
    join css_hcm.db_Thanhtoan b on a.thanhtoan_id = b.thanhtoan_id 
    join css_hcm.db_khachhang c on a.khachhang_id = c.khachhang_id
    left join v_thongtinkm_all km on a.thuebao_id = km.thuebao_id
    join css_hcm.db_adsl db on a.thuebao_id = db.thuebao_id 
    left join xi2 on a.thuebao_id = xi2.thuebao_id
    left join css_hcm.thoihan th on db.thoihan_id = th.thoihan_id
    left join css_hcm.toanha tn on db.toanha_id = tn.toanha_id
    left join kvuc kv on a.thuebao_id = kv.thuebao_id
    left join luuluong ll on ll.ma_Tb = a.ma_tb
    left join css_hcm.trangthai_tb tt on a.trangthaitb_id = tt.trangthaitb_id
    left join css_hcm.loaihinh_tb lh on a.loaitb_id = lh.loaitb_id
    left join sm on a.ma_Tb = sm.taikhoanfiber and rnk = 1
    left join css_hcm.loai_kh lkh on c.loaikh_id = lkh.loaikh_id
    left join css_hcm.ct_khuyenmai ct on km.chitietkm_id = ct.chitietkm_id
    join css_hcm.bd_goi_dadv goi on a.thuebao_id = goi.thuebao_id
    join css_hcm.goi_dadv_ccbs dm on goi.goi_id = dm.goi_id
where a.loaitb_id in (58,59) and km.hieuluc = 1 and km.ttdc_id = 0 and db.chuquan_id = 145
    and sysdate < least(nvl(km.ngay_thoai - 1, sysdate + interval '50' year ),nvl(km.ngay_thoai - 1, sysdate + interval '50' year ),km.ngay_ktdc) 
    and a.trangthaitb_id not in(7,8,9) and goi.trangthai = 1 and dm.nhomgoi_id in (1,2,4) 
    and goi.goi_id not between 1715 and 1726 and goi.goi_id not in (15414, 16221) and goi.goi_id < 100000
);
select * from gd_vp;-- where thuebao_id in (select thuebao_id from gd_vp group by thuebao_id having count(thuebao_id) > 1)
with lsu as (
    select thuebao_id, ngay_ht, row_number() over (partition by thuebao_id order by hdtb_id desc) rnk
    from css_hcm.hd_thuebao
    where kieuld_id in (13171, 11 ,13212 ,13213, 815 )
),
skm as (select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id from v_thongtinkm_all
        where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
        and 202401 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
) ,
dothu as (
    select sum(dthu) dthu, thuebao_id
    from TTKD_BCT.cuoc_thuebao_ttkd 
    group by thuebao_id
),
slvnp as (
    select a.nhomtb_id, count(distinct a.thuebao_id) mem_vnp
    from css_hcm.bd_goi_dadv a
    where  a.trangthai = 1 and a.dichvuvt_id = 2
    group by nhomtb_id
)
select gd_vp.*, lsu.ngay_ht lsu_dchuyen_fiber ,dothu.dthu, slvnp.mem_vnp, e.thuonghieu,e.tocdothuc
         , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - 
         skm.cuoc_sd, (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm     
         
from gd_vp
    left join css_hcm.db_adsl d on gd_vp.thuebao_id = d.thuebao_id
    left join css_hcm.tocdo_adsl e on d.tocdo_id = e.tocdo_id
    left join css_hcm.muccuoc_tb f on d.tocdo_id = f.tocdo_id
    left join lsu on gd_Vp.thuebao_id = lsu.thuebao_id and rnk = 1
    left join dothu on gd_vp.thuebao_id = dothu.thuebao_id
    left join slvnp on gd_vp.nhomtb_id  = slvnp.nhomtb_id
    left join css_hcm.bd_goi_dadv goi on gd_vp.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi_id not between 1715 and 1726
    left join css_hcm.goi_dadv_lhtb goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
    left join skm on gd_vp.thuebao_id = skm.thuebao_id and skm.loaitb_id in (58, 59)