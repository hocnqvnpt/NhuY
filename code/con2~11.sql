create table final_T2 as
with mesh as (
    SELECT khachhang_id,
           LISTAGG(ma_tb, ',') WITHIN GROUP (ORDER BY ma_tb) AS matb_mesh,
           count(*) as sl_mesh,
           sum(dthu) as dthu_mesh
      FROM (
           SELECT unique
                  b.khachhang_id,
                  b.ma_tb,
                  b1.dthu
             FROM TTKD_BCT.db_thuebao_ttkd b
             left join TTKD_BCT.cuoc_thuebao_ttkd  b1 on b.tb_id = b1.tb_id

             where b.loaitb_id = 210 
           )
     GROUP BY khachhang_id
), --select mesh.* from mesh join css_hcm.db_thuebao a on mesh.khachhang_id = a.khachhang_id
km as (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, km.thang_bddc, km.thang_ktdc
        from v_thongtinkm_all km 
        where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nhom_datcoc_id not in (8, 9, 11, 22)
            --Thay doi thang                                                              
                           and 202402 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                                   union all
                                                                    ----------------Trong thoi gian datcoc---------------
        select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, km.thang_bddc, km.thang_ktdc
        from v_thongtinkm_all km
        where --Thay doi thang
                    khuyenmai_id not in (8731, 1977, 2056, 2150) and nhom_datcoc_id not in (8, 9, 11, 22)
                    and 202402 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
),
km1 as (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu
        from v_thongtinkm_all km 
        where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nhom_datcoc_id not in (8, 9, 11, 22)
            --Thay doi thang                                                              
                           and 202402 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
       union all
        ----------------Trong thoi gian datcoc---------------
        select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu
        from v_thongtinkm_all km
        where --Thay doi thang
                    khuyenmai_id not in (8731, 1977, 2056, 2150) and nhom_datcoc_id not in (8, 9, 11, 22)
                    and 202402 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
),
b as (select distinct khachhang_id, aa.tocdo_id, aa.ma_tb, aa.tb_id, aa.thuebao_id , cc.thuonghieu
            from TTKD_BCT.db_thuebao_ttkd aa, css_hcm.db_adsl bb , css_Hcm.tocdo_adsl cc
            where aa.thuebao_id = bb.thuebao_id and aa.loaitb_id = 61 and aa.tocdo_id = cc.tocdo_id
            and trangthaitb_id not in (7, 8, 9) )
select dbtb.ma_tb, dbtb.ten_Tb, dbtb.diachi_ld, tenphong , dbtb.ma_nv, a.cuoc_saukm, dbtb.ma_dt_kh, dbtb.trangthaitb_id , a.ngay_Sd,round(a.tien_td/1.1)tien_td_khong_VAT
        ,nvl2(km.thuebao_id, 1, 0) tratruoc_Fiber,km.thang_bddc, km.thang_ktdc, km.thang_bd_mg, km.thang_kt_mg kt_tratruoc_fiber, a.tocdothuc, a.thuonghieu Fiber, a.goi_tichhop, a.quan_id,
        b.ma_tb Acc_MyTV, b.thuonghieu MyTV, b1.dthu_MyTV, nvl2(km1.thuebao_id, 1, 0) tratruoc_MyTV, matb_mesh, sl_mesh, dthu_mesh tong_dhu_mesh
        ,dbtb.khachhang_id,a.ma_toanha, a.ma_duan, dv.ten_dv ttvt, a.iptinh, q.ten_Quan
from fiber_hh_thang2 a
        join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id
        join ttkd_bct.phongbanhang ph on dbtb.pbh_ql_id = ph.pbh_id       
         left join  km on a.thuebao_id = km.thuebao_id and km.loaitb_id = 58
        left join  b on b.khachhang_id = a.khachhang_id
        left join (select tb_id, sum(dthu) dthu_MyTV from TTKD_BCT.cuoc_thuebao_ttkd group by tb_id) b1 on b.tb_id = b1.tb_id
        left join  km1 on b.thuebao_id = km.thuebao_id and km.loaitb_id = 61
        left join mesh on a.khachhang_id = mesh.khachhang_id
        left join ttkd_bct.db_thuebao_ttkd db on a.thuebao_id = db.thuebao_id
        left join admin_hcm.donvi dv on db.ttvt_id = dv.donvi_id
        left join css_hcm.quan q on a.quan_id = q.quan_id;
        
        select * from FINAL_T2;
    
with skm as (
    select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id from v_thongtinkm_all
    where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
    and 202402 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
    and loaitb_id in (58, 59)
),
dchi as (
    select  /*+ RESULT_CACHE */ diachi_id,quan_id, phuong_id, tinh_id from css_hcm.diachi
)
select a.thuebao_id, a.ma_tb, a.ngay_sd, e.thuonghieu, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) cuoc_tg
            , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - skm.cuoc_sd, (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm--, nvl(tt.times, 0) tb_lan_tt
        , nvl(ten_goi.ma_goi, 0) goi_tichhop, d.madoicap, d.tocdo_id, e.tocdothuc, e.iptinh, e.soluong_ip, d.muccuoc_id, d.trangbi_id, a.mucuoctb_id, a.doituong_id
        , goi.nhomtb_id, a.khachhang_id, tn.ma_toanha ma_toanha,da.ma_duan , g.quan_id, g.phuong_id--,dv.ten_dv ttvt
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
            left join css_hcm.toanha tn on d.toanha_id = tn.toanha_id
            left join css_hcm.duan da on tn.duan_id = da.duan_id
--            left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
--            left join admin_hcm.donvi dv on b.ttvt_id = dv.donvi_id
where a.trangthaitb_id not in (7, 8, 9) and a.loaitb_id = 61 and d.chuquan_id in (266, 145, 264) and to_number(to_char(a.ngay_sd, 'yyyymm')) <= 202402 --and e.tocdothuc <=90
;