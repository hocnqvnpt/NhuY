select * from vietanhvh.khkt_bc_hoahong ;
select* from ttkd_Bsc.nhanvien_202406;
select* from css_hcm.trangthai_tb;
select* from css_hcm.phieutt_hd;
select * from ttkdhcm_ktnv.DM_MVIEW; where MVIEW_NAME= upper('duan')

select owner , table_name from all_tab_columns where TABLE_NAME LIKE 'TRANGTHAI%';

select* from css_Hcm.doituong where ten_Dt like 'Y%';
doituong_id in (11,12,27,41);
select* from css_hcm.db_khachhang;
select* from css_hcm.loai_kh;
LOAIKH_ID IN (72,73,71,69,70,10);
select* from css_hcm.nganhnghe;
select* from css_hcm.kh_lon;
desc ttkd_Bsc.nhanvien_202407_lan1;

with skm as (
    select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id, rkm_id, nhom_datcoc_id from v_thongtinkm_all
    where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
    and 202405 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
    and loaitb_id in (58, 59) and thang_bddc > 202001 and thuebao_id = 9908934
),
km as (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu
                            from v_thongtinkm_all km 
                            where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nhom_datcoc_id not in (8, 9, 11, 22)
                                --Thay doi thang                                                              
                                               and 202405 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                           union all
                            ----------------Trong thoi gian datcoc---------------
                            select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu
                            from v_thongtinkm_all km
                            where --Thay doi thang
                                        khuyenmai_id not in (8731, 1977, 2056, 2150) and nhom_datcoc_id not in (8, 9, 11, 22)
                                        and 202405 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
)
, ds_fiber as (
    select khachhang_id, thuebao_id
    from css_hcm.db_Thuebao 
    where loaitb_id in (58,59) and trangthaitb_id not in (7,8,9)
)
select a.thuebao_id,a.ma_tb,a.ten_tb, a.ngay_sd, e.thuonghieu, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) cuoc_tg
            , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - skm.cuoc_sd, 
            (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm--, nvl(tt.times, 0) tb_lan_tt from css_Hcm.db_thuebao a
            , e.tocdothuc, e.iptinh, nvl2(km.thuebao_id, 1, 0) tratruoc_fiber, pb.ma_pb, pb.ten_pb, a.loaitb_id
from css_hcm.db_thuebao a
    join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    join css_hcm.db_adsl c on a.thuebao_id = c.thuebao_id
    join css_hcm.db_adsl d on a.thuebao_id = d.thuebao_id
    join css_hcm.tocdo_adsl e on d.tocdo_id = e.tocdo_id
    join css_hcm.muccuoc_tb f on a.mucuoctb_id = f.mucuoctb_id
    left join css_hcm.bd_goi_dadv goi on a.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi_id not between 1715 and 1726
    left join css_hcm.goi_dadv ten_goi on goi.goi_id = ten_goi.goi_id
    left join css_hcm.goi_dadv_lhtb goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
    left join  skm on a.thuebao_id = skm.thuebao_id 
    left join ttkd_bct.db_thuebao_ttkd h on a.thuebao_id = h.thuebao_id
    left join ttkd_Bsc.dm_phongban pb on h.pbh_ql_id = pb.pbh_id and pb.active = 1
    left join admin_hcm.donvi dv on h.ttvt_id = dv.donvi_id
    left join km on a.thuebao_id = km.thuebao_id
    left join ds_fiber ds on a.khachhang_id = ds.khachhang_id
where a.trangthaitb_id not in (7,8,9)  and (a.doituong_id in (11,12,27,41) or b.LOAIKH_ID IN (72,73,71,69,70,10)) and a.loaitb_id not in (58,59)
and ds.thuebao_id is  null
--and not exists (Select 1 from css_hcm.db_thuebao where khachhang_id = a.khachhang_id and loaitb_id in (58,59) and trangthaitb_id not in (7,8,9))
;

with fib as (
    select thuebao_id, khachhang_id
    from css_hcm.db_Thuebao
    where trangthaitb_id not in (7,8,9) and loaitb_id in (58,59)
)
,dvk as (
    select thuebao_id,ma_Tb, khachhang_id, doituong_id, ten_tb, loaitb_id
    from css_hcm.db_Thuebao
    where trangthaitb_id not in (7,8,9) and loaitb_id not in (58,59)
)
select dvk.*, k.ma_kh,ten_kh, l.loaihinh_Tb, pb.ma_pb, pb.ten_pb
from dvk 
    left join fib on dvk.khachhang_id = fib.khachhang_id
    left join css_hcm.db_khachhang k on dvk.khachhang_id = k.khachhang_id
    left join css_hcm.loaihinh_tb l on dvk.loaitb_id = l.loaitb_id
    left join ttkd_bct.db_thuebao_ttkd h on dvk.thuebao_id = h.thuebao_id
    left join ttkd_Bsc.dm_phongban pb on h.pbh_ql_id = pb.pbh_id and pb.active = 1
--where dvk.thuebao_id = 1464139;   
where fib.thuebao_id is null  and (dvk.doituong_id in (11,12,27,41) or k.LOAIKH_ID IN (72,73,71,69,70,10));