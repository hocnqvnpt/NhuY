select * from hocnq_ttkd.fiber_hh where cuoc_saukm is null-- 0
select doituong_id from css_hcm.dichvu_Vt
select * from css_Hcm.diachi_tb
select* from hocnq_ttkd.fiber_hh
drop table fiber_hh_t11
commit;
rollback;
drop table fiber_hh_t12
select NGAY_CAT from css_hcm.db_Thuebao where ma_Tb= 'taco_bthanh' ;
select* from ttkd_Bsc.ct_bsc_tratruoc_moi where ma_Tb= 'taco_bthanh' and thang = 202402;
select* from v_thongtinkm_all where thuebao_id = 2891961
delete from ttkd_b;
drop table fiber_hh_t2;
select* from css_Hcm.muccuoc_tb;
select * from fiber_hh_t9; where ma_Tb ='thitu063';
flashback table nhuy.fiber_hh_t11 to before drop;
create table fiber_hh_t12 as
with skm as (
    select thuebao_id, loaitb_id, ma_Tb,tyle_sd, cuoc_sd, congvan_id, rkm_id, nhom_datcoc_id,chitietkm_id from v_thongtinkm_all
    where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
    and 202412 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
    and loaitb_id in (58, 59) --and thang_bddc > 202001
),
dchi as (
    select  /*+ RESULT_CACHE */ diachi_id,quan_id, phuong_id, tinh_id from css_hcm.diachi
)
select a.thuebao_id,skm.rkm_id,skm.nhom_datcoc_id, a.ma_tb, a.ngay_sd, e.thuonghieu, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) cuoc_tg
            , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - skm.cuoc_sd, (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm--, nvl(tt.times, 0) tb_lan_tt
        , nvl(ten_goi.ma_goi, 0) goi_tichhop, d.madoicap, d.tocdo_id, e.tocdothuc, e.iptinh, e.soluong_ip, d.muccuoc_id, d.trangbi_id, a.mucuoctb_id, a.doituong_id
        , goi.nhomtb_id, a.khachhang_id, tn.ma_toanha ma_toanha, g.quan_id,q.ten_quan, g.phuong_id,dv.ten_dv ttvt, cq.chuquan_id, cq.tenchuquan
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
            left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
            left join admin_hcm.donvi dv on b.ttvt_id = dv.donvi_id
            left join css_hcm.quan q on g.quan_id = q.quan_id
            left join css_hcm.chuquan cq on d.chuquan_id = cq.chuquan_id
where a.trangthaitb_id not in (7, 8, 9) and a.loaitb_id in (58, 59) and d.chuquan_id in (266, 145, 264) and to_number(to_char(a.ngay_sd, 'yyyymm')) <= 202412 -- and a.ma_Tb = 'thitu063' --and e.tocdothuc <=90
;
drop table fina;
select* from css_hcm.db_thuebao where  ma_Tb = 'thanhdiep_dbl';
select* from v_Thongtinkm_all where ma_Tb = 'thanhdiep_dbl'  and nhom_datcoc_id not in (8, 9, 11, 22);
select* from fina a where ma_Tb = 'thanhdiep_dbl'
 ; where mytv_b2b = 1;
create table fina as
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
km as (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, chitietkm_id,khuyenmai_id
            from v_thongtinkm_all km 
            where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                --Thay doi thang                                                              
                               and 202412 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
           union all
            ----------------Trong thoi gian datcoc---------------
            select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu,chitietkm_id,khuyenmai_id
            from v_thongtinkm_all km
            where --Thay doi thang
                        khuyenmai_id not in (8731, 1977, 2056, 2150) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                        and 202412 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
            union all
            -- uu dai gi do khong biet
            select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, chitietkm_id,khuyenmai_id
            from v_thongtinkm_all km
            where --Thay doi thang
                        khuyenmai_id  in (9192 ,   10524  ,   11263) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                         and 202412 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
),
km1 as (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu
            from v_thongtinkm_all km 
            where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                --Thay doi thang                                                              
                        and 202412 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
           union all
            ----------------Trong thoi gian datcoc---------------
            select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu
            from v_thongtinkm_all km
            where --Thay doi thang
                        khuyenmai_id not in (8731, 1977, 2056, 2150) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                        and 202412 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
             -- uu dai gi do khong biet
             union all
            select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu
            from v_thongtinkm_all km
            where --Thay doi thang
                        khuyenmai_id  in (9192 ,   10524  ,   11263) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                        and 202412 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
),
b as (select distinct kh.khachhang_id, aa.tocdo_id, aa.ma_tb, aa.tb_id, aa.thuebao_id , cc.thuonghieu, kh.ten_kh,kh.ma_kh,
        case when kh.ma_kh in ('HCM014465780','HCM014822516','HCM002508784','HCM002604557','HCM015035303','HCM015423736','HCM015332638','HCM015726182') then 1 else 0 end MyTV_B2B
            from TTKD_BCT.db_thuebao_ttkd aa, css_hcm.db_adsl bb , css_Hcm.tocdo_adsl cc, css_hcm.db_khachhang kh 
            where aa.thuebao_id = bb.thuebao_id and aa.loaitb_id = 61 and aa.tocdo_id = cc.tocdo_id and aa.khachhang_id = kh.khachhang_id
            and trangthaitb_id not in (7, 8, 9))
select 
   dbtb.thuebao_id, dbtb.ma_tb, dbtb.ten_Tb, dbtb.diachi_ld, tenphong , dbtb.ma_nv, a.cuoc_saukm, dbtb.ma_dt_kh, dbtb.trangthaitb_id , a.ngay_Sd,
        nvl2(km.thuebao_id, 1, 0) tratruoc_Fiber,km.thang_kt_mg kt_tratruoc_fiber, a.tocdothuc, a.thuonghieu Fiber, a.goi_tichhop, 
        b.ma_tb Acc_MyTV, b.mytv_b2b, b.thuonghieu MyTV, b1.dthu_MyTV, nvl2(km1.thuebao_id, 1, 0) tratruoc_MyTV, matb_mesh, sl_mesh, dthu_mesh tong_dhu_mesh, a.iptinh, 
        dbtb.khachhang_id,kh.ten_kh, kh.ma_kh,a.ma_toanha, dv.ten_dv ttvt, l.khdn,  a.chuquan_id, a.tenchuquan,a.tocdo_id,ct.ten_km
        , kh.so_dt sdt_kh,tt.so_dt sdt_Tt
from FIBER_HH_T12 a
        join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id
        join ttkd_bct.phongbanhang ph on dbtb.pbh_ql_id = ph.pbh_id       
         left join  km on a.thuebao_id = km.thuebao_id and km.loaitb_id = 58
        left join  b on b.khachhang_id = a.khachhang_id
        left join (select tb_id, sum(dthu) dthu_MyTV from TTKD_BCT.cuoc_thuebao_ttkd group by tb_id) b1 on b.tb_id = b1.tb_id
        left join  km1 on b.thuebao_id = km.thuebao_id and km.loaitb_id = 61
        left join mesh on a.khachhang_id = mesh.khachhang_id
        left join ttkd_bct.db_thuebao_ttkd db on a.thuebao_id = db.thuebao_id
        left join admin_hcm.donvi dv on db.ttvt_id = dv.donvi_id
        left join css_hcm.db_thuebao tb on a.thuebao_id = tb.thuebao_id
        left join css_hcm.db_khachhang kh on tb.khachhang_id = kh.khachhang_id
        left join css_hcm.loai_kh l on kh.loaikh_id = l.loaikh_id
        left join css_hcm.quan q on a.quan_id = q.quan_id
        left join css_Hcm.khuyenmai ct on km.khuyenmai_id = ct.khuyenmai_id
        left join css_hcm.db_Thanhtoan tt on tb.thanhtoan_id = tt.thanhtoan_id
;
select  a.*, q.quan_id, q.ten_quan, da.ma_duan ,case when KT_TRATRUOC_FIBER is not null then
                                        MONTHS_BETWEEN(TO_DATE(KT_TRATRUOC_FIBER, 'YYYYMM'), TO_DATE(202412, 'YYYYMM')) end AS sothang_conlai
from fina a
    left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
    left join css_Hcm.diachi_Tb c on a.thuebao_id= c.thuebao_id
    left join css_hcm.diachi d on c.diachild_id = d.diachi_id
    left join css_hcm.quan q on d.quan_id = q.quan_id
    left join css_hcm.toanha t on b.toanha_id = t.toanha_id
    left join css_hcm.duan da on t.duan_id = da.duan_id
    
    ;
    select* from fina;
select * from css_hcm.diachi_tb ;from css_hcm.db_thuebao;where ma_Tb = 'thanhdinh395lqd';
select* from v_Thongtinkm_all  where ten_km like '228/%';ma_Tb = 'phiyen1974';
select* from css_hcm.tocdo_adsl where tocdo_id = 17187;