select chuquan_id from css_hcm.db_thuebao-- where thang = 202312 and ma_tb = 'congbinha203'
select* from css_hcm.khuyenmai_hdtb where rkm_id = 6208932
select* from admin_hcm.loai_dvi
select* from css_hcm.thoihan-- where khuvuc_id = 160 -- bang khu vuc va don vi theo id
select* from css_Hcm.khuvuc -- danh muc khu vuc (1 khu vuc thuoc quan ly cua 1 trung tam vien thong)
select* from css_hcm.khuvuc_nv  -- bang the hien nhan vien vien thong nao quan ly khu vuc nao (loainv_id = 51 va loaikv_id = 4)
select* from css_hcm.khuvuc_px -- dia chi theo ID quan phuong cua khu vuc ;
;
select *from hocnq_ttkd.tmp-- where thuebao_id = 8582949
where thuebao_id in (select thuebao_id from hocnq_ttkd.tmp group by thuebao_id having count(thuebao_id) > 1);-- phan loai khu vuc 

select* from css_hcm.toanha where ten_toanha like '%One%'-- like 'LOAI%' 

select a.thuebao_id, a.ma_tb, a.ten_tb, 
from css_Hcm.db_Thuebao a

select* from v_Thongtinkm_all-- where quan_moi_id = 1144
;
with xi2 as (select a.thuebao_id, case when vattu_id in  (892, 891, 893) then 'I-240W'
                when vattu_id in  (890, 889,  896, 884,  895, 883) then 'HG8xx'
                when vattu_id in (888, 14914, 14123, 14890) then 'GWxx0-HiB'
                when vattu_id in (894, 886, 885, 11713, 887, 14699, 13781, 14125, 14098, 14124) then 'GWxx0'
                when vattu_id in (16456) then 'F671Y-HiB'
            end ONT_TYPE
            from hocnq_ttkd.dulieu_kn_ont a where rnk = 1
),
luuluong1 as (
    select sum(luu_luong) luuluonggn1, ma_Tb from bcss_hcm.thftth partition for (20231201)  group by ma_tb 
),
luuluong2 as (
    select sum(luu_luong) luuluonggn2, ma_tb from bcss_hcm.thftth partition for (20231101)  group by ma_tb 
),
tt_tb as (
    select thuebao_id, thoihan_id, toanha_id, chuquan_id
    from css_hcm.db_adsl
    union all
    select thuebao_id, thoihan_id, toanha_id, chuquan_id
    from css_hcm.db_cd 
    union all
    select thuebao_id, thoihan_id, toanha_id, chuquan_id
    from css_hcm.db_ims
    union all 
    select thuebao_id, thoihan_id, toanha_id, chuquan_id
    from css_hcm.db_mgwan
),
ttvt as
(
    select a.thuebao_id, d.ma_kv, e.ma_nv,e.ten_nv, c.ten_dv ten_to, c.ten_dvql ten_pb
    from css_hcm.dbtb_kv a
        left join css_hcm.khuvuc d on a.khuvuc_id = d.khuvuc_id 
        left join css_hcm.khuvuc_nv b on a.khuvuc_id = b.khuvuc_id 
        left join admin_hcm.nhanvien e on b.nhanvien_id = e.nhanvien_id
        left join admin_hcm.donvi c on e.donvi_id = c.donvi_id
    where b.loaikv_id = 4 and b.nhiemvu = 1 and b.loainv_id = 51 
),
tvdd as
(
    select a.nhomtb_id , count(b.thuebao_id) sltv
    from css_hcm.bd_goi_dadv a
        join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
    where a.trangthai = 1 and b.loaitb_id in (20,21)
    group by a.nhomtb_id
),
dch as (
    select hd.thuebao_id, hd.ngay_ht, row_number() over (partition by hd.thuebao_id order by hd.hdtb_id desc) rnk
    from css_hcm.hd_Thuebao hd 
    join css_hcm.db_Thuebao a on a.thuebao_id = hd.thuebao_id 
    where hd.kieuld_id in (11,31,556,13264,13265)
),
skm as (
    select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id from v_thongtinkm_all
    where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
    and 202312 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
    and loaitb_id in (58, 59)
)
-- where tocdo_id = al.tocdo_id
select 
    a.thuebao_id,c.khachhang_id, tt.thanhtoan_id, a.ma_tb,a.ngay_sd,a.ngay_td,a.ngay_cat, a.ten_tb, a.loaitb_id, c.ma_kh, d.loaihinh_Tb,
    a.diachi_ld, e.trangthai_tb, e.trangthaitb_id, f.muccuoc, g.tocdo, g.thuonghieu, km.rkm_id, km.ngay_bddc, km. ngay_ktdc, km.ngay_huy, km.ngay_thoai,
    ctkm.huong_dc, km.cuoc_dc datcoc_csd ,h.khdn, case when xi2.ont_type like '%HiB' then 1 when xi2.ont_type is null then 1 else 0 end ONT_HiB,
    da.ma_duan, da.ten_duan, tn.ten_toanha, tn.sdt_bql, tn.sl_tang, tn.sl_canho,tn.quy_mo, c.ten_kh, g.tocdothuc, th.TENTHOIHAN,ttvt.ma_nv manv_ttvt
    , ttvt.ten_nv tennv_ttvt, ttvt.ten_to to_ttvt, ttvt.ten_pb ten_ttvt, ttvt.ma_kv, c.so_dt sdt_kh, tt.so_dt sdt_tt, c.mst mst_kh, tt.mst mst_tt,
    ll.luuluonggn2, l.luuluonggn1,case when tdo.tocdo_id is null then 0 else 1 end tocdo_old, case when tv.taikhoanfiber is not null then 1 else 0 end sd_smarttv,
    dv.nhomtb_id ,tvdd.sltv sl_mem_vnp,decode(tt.httt_id, 2, 'H', 5, 'H', 'G') HT_thutien, dch.ngay_ht LSU_DCHUYEN_FIBER
   ,((100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100)  cuoc_saukm
from css_hcm.db_thuebao a
    left join css_hcm.db_khachhang c on a.khachhang_id = c.khachhang_id
    left join css_hcm.loaihinh_tb d on a.loaitb_id = d.loaitb_id
    left join css_hcm.muccuoc_Tb f on a.mucuoctb_id = f.mucuoctb_id
    left join tt_tb b on a.thuebao_id = b.thuebao_id 
    left join css_hcm.tocdo_adsl g on f.tocdo_id = g.tocdo_id
    left join css_hcm.trangthai_Tb e on a.trangthaitb_id =e.trangthaitb_id
    left join css_hcm.loai_kh h on c.loaikh_id = h.loaikh_id
    left join css_hcm.db_Thanhtoan tt on a.thanhtoan_id = tt.thanhtoan_id
    left join css_hcm.toanha tn on b.toanha_id = tn.toanha_id
    left join css_hcm.duan da on tn.duan_id = da.duan_id
    left join css_hcm.diachi_tb dctb on a.thuebao_id = dctb.thuebao_id 
    left join css_hcm.diachi dc on dctb.diachild_id = dc.diachi_id
    left join css_hcm.tinh t on dc.tinh_id = t.tinh_id
    left join css_hcm.quan q on dc.quan_id = q.quan_id
    left join css_hcm.pho p on dc.pho_id = p.pho_id
    left join xi2 on a.thuebao_id = xi2.thuebao_id
    left join v_Thongtinkm_all km on a.thuebao_id = km.thuebao_id and km.hieuluc = 1 and km.ttdc_id = 0 and
        least(nvl(ngay_huy,sysdate + interval '50' year),nvl(ngay_thoai,sysdate + interval '50' year),ngay_ktdc) >= sysdate
    left join css_hcm.ct_khuyenmai ctkm on km.chitietkm_id = ctkm.chitietkm_id
    left join css_hcm.thoihan th on b.THOIHAN_ID = th.thoihan_id
    left join luuluong2 ll on a.ma_Tb = ll.ma_Tb 
    left join luuluong1 l on a.ma_tb = l.ma_tb
    left join ttvt on a.thuebao_id = ttvt.thuebao_id
    left join HOCNQ_TTKD.tocdo_fiber_old tdo on g.tocdo_id = tdo.tocdo_id
    left join  (select taikhoanfiber from ttkdhcm_ktnv.ID114_FIBER_SMARTTV group by taikhoanfiber) tv on a.ma_Tb = tv.taikhoanfiber
    left join css_Hcm.bd_goi_dadv dv on a.thuebao_id = dv.thuebao_id and dv.trangthai = 1
    left join tvdd on dv.nhomtb_id = tvdd.nhomtb_id
    left join CSS_HCM.DB_THANHTOAN TT on A.THANHTOAN_ID = tt.THANHTOAN_ID 
    left join  dch on a.thuebao_id = dch.thuebao_id and dch.rnk = 1
    left join css_hcm.goi_dadv_lhtb goi1 on dv.goi_id = goi1.goi_id and g.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
    left join  skm on a.thuebao_id = skm.thuebao_id 
where b.chuquan_id = 145 and b.toanha_id = 1354;
  -- and a.thuebao_id not in (select thuebao_id from hocnq_ttkd.tmp group by thuebao_id having count(thuebao_id) > 1) --and a.thuebao_id =10789603
; -- to         -- pb       nv
--- VNP07012H0	VNP0701200	VNP017400
select*  from ttkd_bsc.ct_bsc_giahan_cntt  where  MANV_GIAO = 'VNP017400' AND thang = 202312;
update ttkd_bsc.ct_bsc_giahan_cntt set TIEN_THANHTOAN = 756000 where  ma_Tb = 'hcm_tmqt_00000528' and thang = 202312;
commit;
 select* from v_Thongtinkm_all