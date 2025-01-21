select* from ttkd_Bsc.nhanvien where thang = 202411 and donvi = 'TTKD'; 
commit;
update ttkd_Bsc.nhanvien set thaydoi_Vtcv=0 where thang = 202411 and donvi = 'TTKD' and thaydoi_Vtcv is null;ma_nv in (select ma_Nv from nhanvien_202411 where tinh_Bsc=  1);
and tinh_Bsc = 0 ;and ma_nv in (
select ma_nv from nhanvien_202410_l2 where tinh_Bsc = 1);

update ttkd_bsc.nhanvien  A SET (a.user_ccos,trangthai_ccos)=(select user_ccos, trangthai from ttkd_bsc.dm_ccos where manv_hrm=a.ma_nv and thang='202410' )
--;
--CTV088099
--CTV088118
--CTV088125
--CTV088162
--CTV088163
--CTV088165;
--SELECT* FROM ttkd_bsc.nhanvien  A
where a.user_ccos is null and thang = 202410 and donvi = 'TTKD' AND EXISTS (select user_ccos, trangthai from ttkd_bsc.dm_ccos where manv_hrm=a.ma_nv and thang='202410' )
;
ROLLBACK;
COMMIT;
select* from css_hcm.db_Thuebao where loaitb_id in (2117,149);
select* from css_Hcm.loaihinh_Tb where upper(loaihinh_Tb ) like 'VCC%';
select* from giao_bhkv_pb;
select* from ttkd_Bct.db_Thuebao_ttkd where thuebao_id =10712291; loaitb_id in (2117,149);
    select * from tuyenngo.cps_vnp_202311_tong 
    where vcc > 0 
group by tien;
drop table ds;
create table ds as (

select thuebao_id , sum(tien) dthu ,202311 thang, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202311_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu , 202312, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202312_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all 
select thuebao_id , sum(tien) dthu , 202401, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202401_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu , 202402, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202402_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu, 202403, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202403_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu, 202404, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202404_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu, 202405, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202405_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu, 202406 , ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202406_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all 
select thuebao_id , sum(tien) dthu, 202407, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202407_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu , 202408, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202408_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all 
select thuebao_id , sum(tien) dthu , 202409, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202409_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
union all
select thuebao_id , sum(tien) dthu, 202410, ma_kh, ma_tb, ten_tb, ma_tt from tuyenngo.cps_vnp_202410_tong 
where vcc > 0 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
) 
;
with chot as (
select thuebao_id , ma_kh,ma_tt,ma_Tb,ten_tb from ds 
group by thuebao_id, ma_kh, ma_tb, ten_tb, ma_tt
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
select b.khachhang_id, a.ma_kh, a.ma_tt, a.ma_tb,a.ten_tb, e.loaihinh_Tb, f.ten_Dvvt dichvu, d.ma_hd, b.ngay_sd, b.ngay_cat, km.ngay_kt_mg ngay_kt, km.so_thangdc,
        d.mst, td.tocdothuc,td.thuonghieu, idc.ma_Dc idc_lapdat,dvct.dv_congthem goi_dvct, tt.trangthai_Tb,  dt.ten_dt doituong_Tb, lkh.ten_loaikh loai_kh, nn.tennganhnghe , dv.ten_dv ten_ttvt
        ,pb.ten_pb ten_pb_quanly, t.tento to_ql, db.ma_nv, NV.TEN_NV ten_nv_ql, db.manv_ptm manv_tiepthi, dv2.ten_Dv to_tiepthi , dv2.ten_Dv nhom_gt, dv3.ten_dv ten_pb_tiepthi
       , ds10.dthu dthu_202311, ds11.dthu dthu_202312
        , ds12.dthu dthu_202401, ds13.dthu dthu_202402, ds14.dthu dthu_202403, ds15.dthu dthu_202404, ds16.dthu dthu_202405, ds17.dthu dthu_202406
        , ds18.dthu dthu_202407, ds19.dthu dthu_202408, ds20.dthu dthu_202409, ds21.dthu dthu_202410
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
    left join ds ds20 on a.thuebao_id = ds20.thuebao_id and ds20.thang  = 202409
    left join ds ds21 on a.thuebao_id = ds21.thuebao_id and ds21.thang  = 202410
where ma_Tb= '84911153705';

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