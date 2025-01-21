with skm as (
    select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id, rkm_id, nhom_datcoc_id from v_thongtinkm_all
    where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
    and 202403 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
    and loaitb_id in (58, 59) --and thang_bddc > 202001
),
dchi as (
    select  /*+ RESULT_CACHE */ diachi_id,quan_id, phuong_id, tinh_id from css_hcm.diachi
)
,
tp as (
    select a.goi_id,    LISTAGG( distinct b.loaihinh_tb, ',') WITHIN GROUP (ORDER BY a.loaitb_id) AS thanhphan_goi
    from css_hcm.goi_dadv_lhtb a
        left join css_hcm.loaihinh_Tb b on a.loaitb_id = b.loaitb_id
    group by a.goi_id
)
select a.thuebao_id,skm.rkm_id,a.ma_tb, lh.loaihinh_tb, kh.ten_kh, tt.trangthai_Tb, a.diachi_ld,q.ten_quan, e.tocdo_id, 
    e.thuonghieu ten_tocdo,e.tocdo,goi.goi_id, ten_goi.ten_goi,tp.thanhphan_goi, f.cuoc_tg muc_cuoc, goi1.tien_goi cuoc_goi --nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) cuoc_tg
            , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - skm.cuoc_sd, (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm--, nvl(tt.times, 0) tb_lan_tt
--        , nvl(ten_goi.ma_goi, 0) goi_tichhop, d.madoicap, d.tocdo_id, e.tocdothuc, e.iptinh, e.soluong_ip, d.muccuoc_id, d.trangbi_id, a.mucuoctb_id, a.doituong_id
--        , goi.nhomtb_id, a.khachhang_id, tn.ma_toanha ma_toanha, g.quan_id,q.ten_quan, g.phuong_id,dv.ten_dv ttvt
from css_hcm.db_thuebao a
            join css_hcm.db_adsl d on a.thuebao_id = d.thuebao_id
            join css_hcm.tocdo_adsl e on d.tocdo_id = e.tocdo_id
            join css_hcm.muccuoc_tb f on a.mucuoctb_id = f.mucuoctb_id
            left join css_hcm.diachi_tb i on a.thuebao_id = i.thuebao_id
            left join dchi g on i.diachild_id = g.diachi_id
            left join css_hcm.bd_goi_dadv goi on a.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi_id not between 1715 and 1726
            left join css_hcm.goi_dadv ten_goi on goi.goi_id = ten_goi.goi_id
            left join css_hcm.goi_dadv_lhtb goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
            left join  skm on a.thuebao_id = skm.thuebao_id 
            left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
            left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
            left join admin_hcm.donvi dv on b.ttvt_id = dv.donvi_id
            left join css_hcm.quan q on g.quan_id = q.quan_id
            left join css_hcm.loaihinh_Tb lh on a.loaitb_id = lh.loaitb_id
            left join css_hcm.trangthai_Tb tt on a.trangthaitb_id = tt.trangthaitb_id
            left join tp on goi.goi_id = tp.goi_id
            left join css_hcm.loai_kh l on kh.loaikh_id = l.loaikh_id
where a.trangthaitb_id not in (7, 8, 9) and a.loaitb_id in (58, 59) and d.chuquan_id in (266, 145, 264) and l.khdn = 0 and a.ma_tb ='haidang130720'
;