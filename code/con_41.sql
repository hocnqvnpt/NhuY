dbtb.ma_tb, dbtb.ten_tb, dbtb.diachi_ld, tenphong, dbtb.ma_nv, a.CUOC_SAUKM, dbtb.ma_dt_kh, dbtb.trangthaitb_id, a.ngay_sd
                , nvl2(km.thuebao_id, 1, 0) tratruoc_Fiber, km.thang_kt_mg kt_tratruoc_fiber, a.tocdothuc, a.thuonghieu Fiber, a.goi_tichhop, a.quan_id
                , b.ma_tb Acc_MyTV, ad1.thuonghieu MyTV, dthu_MyTV, nvl2(km1.thuebao_id, 1, 0) tratruoc_MyTV,  matb_mesh, sl_mesh, tong_dhu_mesh
                select tenphong from ttkd_bct.db_Thuebao_ttkd 
select ma_tb from (            
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
) --select mesh.* from mesh join css_hcm.db_thuebao a on mesh.khachhang_id = a.khachhang_id
select dbtb.ma_tb, dbtb.ten_Tb, dbtb.diachi_ld, tenphong , dbtb.ma_nv, a.cuoc_saukm, dbtb.ma_dt_kh, dbtb.trangthaitb_id , a.ngay_Sd,
        nvl2(km.thuebao_id, 1, 0) tratruoc_Fiber,km.thang_kt_mg kt_tratruoc_fiber, a.tocdothuc, a.thuonghieu Fiber, a.goi_tichhop, a.quan_id,
        b.ma_tb Acc_MyTV, b.thuonghieu MyTV, b1.dthu_MyTV, nvl2(km1.thuebao_id, 1, 0) tratruoc_MyTV, matb_mesh, sl_mesh, dthu_mesh tong_dhu_mesh
        ,dbtb.khachhang_id
from fiber_hienhuu a
        join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id
        join ttkd_bct.phongbanhang ph on dbtb.pbh_ql_id = ph.pbh_id       
         left join (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu
                                                                    from v_thongtinkm_all km 
                                                                    where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nhom_datcoc_id not in (8, 9, 11, 22)
                                                                        --Thay doi thang                                                              
                                                                                       and 202310 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                                   union all
                                                                    ----------------Trong thoi gian datcoc---------------
                                                                    select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu
                                                                    from v_thongtinkm_all km
                                                                    where --Thay doi thang
                                                                                khuyenmai_id not in (8731, 1977, 2056, 2150) and nhom_datcoc_id not in (8, 9, 11, 22)
                                                                                and 202310 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                    ) km on a.thuebao_id = km.thuebao_id and km.loaitb_id = 58
        left join (select distinct khachhang_id, aa.tocdo_id, aa.ma_tb, aa.tb_id, aa.thuebao_id , cc.thuonghieu
            from TTKD_BCT.db_thuebao_ttkd aa, css_hcm.db_adsl bb , css_Hcm.tocdo_adsl cc
            where aa.thuebao_id = bb.thuebao_id and aa.loaitb_id = 61 and aa.tocdo_id = cc.tocdo_id
            and trangthaitb_id not in (7, 8, 9)) b on b.khachhang_id = a.khachhang_id
        left join (select tb_id, sum(dthu) dthu_MyTV from TTKD_BCT.cuoc_thuebao_ttkd group by tb_id) b1 on b.tb_id = b1.tb_id
        left join (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu
                                                                from v_thongtinkm_all km 
                                                                where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nhom_datcoc_id not in (8, 9, 11, 22)
                                                                    --Thay doi thang                                                              
                                                                                   and 202310 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                               union all
                                                                ----------------Trong thoi gian datcoc---------------
                                                                select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu
                                                                from v_thongtinkm_all km
                                                                where --Thay doi thang
                                                                            khuyenmai_id not in (8731, 1977, 2056, 2150) and nhom_datcoc_id not in (8, 9, 11, 22)
                                                                            and 202310 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                ) km1 on b.thuebao_id = km.thuebao_id and km.loaitb_id = 61
        left join mesh on a.khachhang_id = mesh.khachhang_id
  ) group by ma_Tb having count(ma_tb) > 1      
        
        
Select* from css_hcm.db_Thuebao where loaitb_id = 61