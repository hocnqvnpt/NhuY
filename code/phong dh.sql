drop table ds;
create table ds as (
select thuebao_id, sum(dthu) dthu, 202301 thang from ttkd_bct.cuoc_thuebao_ttkd_202301
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202302 thang from ttkd_bct.cuoc_thuebao_ttkd_202302
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202303 thang from ttkd_bct.cuoc_thuebao_ttkd_202303
group by thuebao_id
union all 
select thuebao_id, sum(dthu) dthu, 202304 thang from ttkd_bct.cuoc_thuebao_ttkd_202304
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202305 thang from ttkd_bct.cuoc_thuebao_ttkd_202305
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202306 thang from ttkd_bct.cuoc_thuebao_ttkd_202306
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202307 thang from ttkd_bct.cuoc_thuebao_ttkd_202307
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202308 thang from ttkd_bct.cuoc_thuebao_ttkd_202308
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202309 thang from ttkd_bct.cuoc_thuebao_ttkd_202309
group by thuebao_id
union all 
select thuebao_id, sum(dthu) dthu, 202310 thang from ttkd_bct.cuoc_thuebao_ttkd_202310
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202311 thang from ttkd_bct.cuoc_thuebao_ttkd_202311
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202312 thang from ttkd_bct.cuoc_thuebao_ttkd_202312
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202401 thang from ttkd_bct.cuoc_thuebao_ttkd_202401
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202402 thang from ttkd_bct.cuoc_thuebao_ttkd_202402
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202403 thang from ttkd_bct.cuoc_thuebao_ttkd_202403
group by thuebao_id
union all 
select thuebao_id, sum(dthu) dthu, 202404 thang from ttkd_bct.cuoc_thuebao_ttkd_202404
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202405 thang from ttkd_bct.cuoc_thuebao_ttkd_202405
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202406 thang from ttkd_bct.cuoc_thuebao_ttkd_202406
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202407 thang from ttkd_bct.cuoc_thuebao_ttkd_202407
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202408 thang from ttkd_bct.cuoc_thuebao_ttkd_202408
group by thuebao_id
union all
select thuebao_id, sum(dthu) dthu, 202409 thang from ttkd_bct.cuoc_thuebao_ttkd_202409
group by thuebao_id

) 
;
with chot as (
select thuebao_id  from ds 
group by thuebao_id
having sum(dthu)>0
)
, km as (
    select thuebao_id, ngay_kt_mg, so_thangDc,cuoc_Dc datcoc_csd, row_Number() over (partition by thuebao_id order by ngay_bddc ) rn
    from ds_giahan_tratruoc2 
    where to_Number(to_char(ngay_kt_mg,'yyyymmdd'))>=20241031
)
, tdo as (
    select tocdo_id, thuebao_id
    from css_hcm.db_adsl
    union all
    select tocdo_id, thuebao_id
    from css_hcm.db_cntt
    union all
    select tocdo_id, thuebao_id
    from css_hcm.db_mgwan
    union all
    select tocdo_id, thuebao_id
    from css_hcm.db_tsl
   
)

, tocdo as (
    select distinct thuebao_id, b.thuonghieu , b.tocdothuc
    from tdo 
        join css_hcm.tocdo_adsl b on tdo.tocdo_id = b.tocdo_id
) 
, idc as 
(
    select thuebao_id, ma_dc 
    from css.v_Db_cntt@dataguard a
        join css.v_diachi_idc@dataguard b on a.idc_id =b.idc_id
)
select b.khachhang_id, d.ma_kh, c.ma_tt, b.ma_tb,b.ten_tb, e.loaihinh_Tb, f.ten_Dvvt dichvu, d.ma_hd, b.ngay_sd, b.ngay_cat, km.ngay_kt_mg ngay_kt, km.so_thangdc,
        d.mst, td.tocdothuc,td.thuonghieu, idc.ma_Dc idc_lapdat,dvct.dv_congthem goi_dvct, tt.trangthai_Tb,  dt.ten_dt doituong_Tb, lkh.ten_loaikh loai_kh, nn.tennganhnghe , dv.ten_dv ten_ttvt
        ,pb.ten_pb ten_pb_quanly, t.tento to_ql, db.ma_nv, NV.TEN_NV ten_nv_ql, db.manv_ptm manv_tiepthi, dv2.ten_Dv to_tiepthi , dv2.ten_Dv nhom_gt, dv3.ten_dv ten_pb_tiepthi
        , ds.dthu dthu_202301, ds1.dthu dthu_202302, ds2.dthu dthu_202303, ds3.dthu dthu_202304, ds4.dthu dthu_202305, ds5.dthu dthu_202306
        , ds6.dthu dthu_202307, ds7.dthu dthu_202308, ds8.dthu dthu_202309, ds9.dthu dthu_202310, ds10.dthu dthu_202311, ds11.dthu dthu_202312
        , ds12.dthu dthu_202401, ds13.dthu dthu_202402, ds14.dthu dthu_202403, ds15.dthu dthu_202404, ds16.dthu dthu_202405, ds17.dthu dthu_202406
        , ds18.dthu dthu_202407, ds19.dthu dthu_202408, ds20.dthu dthu_202409
from chot a
    left join css_hcm.db_Thuebao b on a.thuebao_id =b.thuebao_id
    left join css_hcm.db_thanhtoan c on b.thanhtoan_id = c.thanhtoan_id
    left join css_hcm.db_khachhang d on b.khachhang_id = d.khachhang_id
    left join css_hcm.loaihinh_Tb e on b.loaitb_id = e.loaitb_id
    left join css_hcm.dichvu_Vt f on b.dichvuvt_id = f.dichvuvt_id
    left join km on a.thuebao_id = km.thuebao_id and rn = 1
    left join tocdo td on a.thuebao_id = td.thuebao_id
    left join idc on a.thuebao_id = idc.thuebao_id
    left join css_hcm.trangthai_Tb tt on b.trangthaitb_id = tt.trangthaitb_id
    left join css_hcm.doituong dt on b.doituong_id = dt.doituong_id
    left join css_hcm.loai_kh lkh on d.loaikh_id = lkh.loaikh_id
    left join css_hcm.nganhnghe nn on d.nganhnghe_id = nn.nganhnghe_id
    left join admin_hcm.donvi dv on b.donvi_id = dv.donvi_id
    left join ttkd_bct.db_thuebao_ttkd db on a.thuebao_id = db.thuebao_id
    left join ttkd_bsc.dm_phongban pb on db.mapb_ql = pb.ma_pb
    left join  (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) t on t.tbh_id = db.tbh_ql_id and db.pbh_ql_id = t.pbh_id 
    left join ttkd_bsc.nhanvien nv on db.ma_Nv = nv.ma_nv and thang = 202409 and donvi = 'TTKD'
    left join dvct on a.thuebao_id = dvct.thuebao_id
    left join admin_hcm.nhanvien nv2 on db.manv_ptm = nv2.ma_nv
    left join admin_hcm.donvi dv2 on nv2.donvi_id = dv2.donvi_id
    left join admin_hcm.donvi dv3 on dv2.donvi_cha_id = dv3.donvi_id
    left join ds on a.thuebao_id = ds.thuebao_id and ds.thang  = 202301
    left join ds ds1 on a.thuebao_id = ds1.thuebao_id and ds1.thang  = 202302
    left join ds ds2 on a.thuebao_id = ds2.thuebao_id and ds2.thang  = 202303
    left join ds ds3 on a.thuebao_id = ds3.thuebao_id and ds3.thang  = 202304
    left join ds ds4 on a.thuebao_id = ds4.thuebao_id and ds4.thang  = 202305
    left join ds ds5 on a.thuebao_id = ds5.thuebao_id and ds5.thang  = 202306
    left join ds ds6 on a.thuebao_id = ds6.thuebao_id and ds6.thang  = 202307
    left join ds ds7 on a.thuebao_id = ds7.thuebao_id and ds7.thang  = 202308
    left join ds ds8 on a.thuebao_id = ds8.thuebao_id and ds8.thang  = 202309
    left join ds ds9 on a.thuebao_id = ds9.thuebao_id and ds9.thang  = 202310
    left join ds ds10 on a.thuebao_id = ds10.thuebao_id and ds10.thang  = 202311
    left join ds ds11 on a.thuebao_id = ds11.thuebao_id and ds11.thang  = 202312
    left join ds ds12 on a.thuebao_id = ds12.thuebao_id and ds12.thang  = 202401
    left join ds ds13 on a.thuebao_id = ds13.thuebao_id and ds13.thang  = 202402
    left join ds ds14 on a.thuebao_id = ds14.thuebao_id and ds14.thang  = 202403
    left join ds ds15 on a.thuebao_id = ds15.thuebao_id and ds15.thang  = 202404
    left join ds ds16 on a.thuebao_id = ds16.thuebao_id and ds16.thang  = 202405
    left join ds ds17 on a.thuebao_id = ds17.thuebao_id and ds17.thang  = 202406
    left join ds ds18 on a.thuebao_id = ds18.thuebao_id and ds18.thang  = 202407
    left join ds ds19 on a.thuebao_id = ds19.thuebao_id and ds19.thang  = 202408
    left join ds ds20 on a.thuebao_id = ds20.thuebao_id and ds20.thang  = 202409;

select ma_hd from css_hcm.db_thuebao ;
select idc_id from css_hcm.db_tsl ;where idc_id = 2;
)
select * from ttkd_bct.db_thuebao_ttkd ;
select owner , table_name from all_tab_columns where column_name = upper('goi_dvct');

select idc_id from css_hcm.db_cntt ;where idc_id = 2;
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202406 and ma_nv ='VNP020230';
select LUONG_DONGIA_GHTT from ttkd_bsc.bangluong_dongia_202406 where ma_nv ='VNP020230' ;
select* from  ttkd_bct.khkt_bc_hoahong;
insert into  ttkd_bct.khkt_bc_hoahong;

    with ttin as (
        select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'B?ng r?ng c? ??nh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong', case when ipcc = 0 then 0 else tien_thuyetphuc*heso_Chuky*heso_dichvu end tien_Thuyetphuc, tien_xuathd*heso_Chuky*heso_dichvu,
        'ghtt_nhuy',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang = 202406 and loai_tinh = 'DONGIATRA_OB' and A.ma_pb in ('VNP0701100','VNP0701200','VNP0701300','VNP0701400','VNP0701500','VNP0701600',
            'VNP0701800','VNP0702100','VNP0702200')
    ;
    select* from admin_hcm.nhanvien where ma_nv='25063';
    select distinct ma_pb from ttkd_bsc.tl_giahan_tratruoc where thang = 202406 and loai_tinh = 'DONGIATRA_OB';
   delete from  ttkd_bct.khkt_bc_hoahong where thang_ptm = 202406  and nguon ='ghtt_nhuy'