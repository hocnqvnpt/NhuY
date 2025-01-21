select q.quan_id, q.ten_quan, p.phuong_id, p.ten_phuong from css_hcm.quan q 
    left join css_hcm.phuong p on q.quan_id = p.quan_id
    where q.tinh_id  = 28;
    select * from ttkdhcm_ktnv.DM_MVIEW where MVIEW_NAME= upper('db_khachhang');

     select khachhang_id, khdn from  css_hcm.db_khachhang kh 
        left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id ;
        
        select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202402;
        select cuoc_dc, tien_td,  from css_hcm.db_Datcoc a
        select* from v_Thongtinkm_all 
        where rkm_id = 6312142;
        create table aaarrrrghh as
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
km as (select km.ma_tb, km.thuebao_id, km.loaitb_id,km.thang_bddc, km.thang_ktdc, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, ct.huong_dc, km.tien_td
        from v_thongtinkm_all km 
            left join css_hcm.ct_khuyenmai ct on km.chitietkm_id = ct.chitietkm_id
        where (km.tyle_sd = 100 or km.tyle_tb = 100) and km.khuyenmai_id not in (1977, 2056, 2998, 2999) and km.nhom_datcoc_id not in (8, 9, 11, 22)
            --Thay doi thang                                                              
                           and 202402 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
       union all
        ----------------Trong thoi gian datcoc---------------
        select km.ma_tb, km.thuebao_id, km.loaitb_id,km.thang_bddc, km.thang_ktdc, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu,ct.huong_dc, km.tien_td
        from v_thongtinkm_all km
                    left join css_hcm.ct_khuyenmai ct on km.chitietkm_id = ct.chitietkm_id

        where --Thay doi thang
                    km.khuyenmai_id not in (8731, 1977, 2056, 2150) and km.nhom_datcoc_id not in (8, 9, 11, 22)
                    and 202402 between km.thang_bddc and least(km.thang_ktdc, nvl(km.thang_kt_dc, 999999), nvl(km.thang_huy, 999999))
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
            and trangthaitb_id not in (7, 8, 9) ),
kh as (
    select khachhang_id, khdn from  css_hcm.db_khachhang a
        left join css_hcm.loai_kh lkh on a.loaikh_id = lkh.loaikh_id 
)
select dbtb.ma_tb, dbtb.ten_Tb, dbtb.diachi_ld, tenphong , dbtb.ma_nv, a.cuoc_saukm,  dbtb.trangthaitb_id , a.ngay_Sd,
        nvl2(km.thuebao_id, 1, 0) tratruoc_Fiber,km.tien_td, km.thang_bddc, km.thang_ktdc,km.thang_bd_mg, km.thang_kt_mg ,km.huong_dc, a.tocdothuc, a.thuonghieu Fiber, a.goi_tichhop, a.quan_id,
        b.ma_tb Acc_MyTV, b.thuonghieu MyTV, b1.dthu_MyTV, nvl2(km1.thuebao_id, 1, 0) tratruoc_MyTV, matb_mesh, sl_mesh, dthu_mesh tong_dhu_mesh, q.ten_quan
        ,dbtb.khachhang_id,a.ma_toanha, a.ma_duan, dv.ten_dv ttvt, a.iptinh, kh.khdn
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
        left join css_hcm.quan q on a.quan_id = q.quan_id
        left join kh on a.khachhang_id = kh.khachhang_id
    where km.thang_kt_mg in (202403,202404,202405)