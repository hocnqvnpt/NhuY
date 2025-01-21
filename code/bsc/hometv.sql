insert into ttkd_Bsc.ct_Bsc_homecombo 
with hd as(
--    select 1
----    nvl(hdgoi.ctv_id,hdkh.ctv_id) ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd, dbgoi.ngay_dk, dbgoi.nhomtb_id
--    from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
--        ,css_hcm.hd_khachhang hdkh
--    where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
--                    and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1 and hdtb.hdkh_id = hdkh.hdkh_id
--                    and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
--                    and to_number(to_char(dbgoi.ngay_dk,'yyyymm')) = 202405 and to_number(to_char(db.ngay_sd,'yyyymm')) = 202405
                 --  and hdtb.ma_tb in ('lmh91')--, 'hieuliem92020')
 
 select  nvl(hdgoi.ctv_id,hdkh.ctv_id) ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd, dbgoi.ngay_dk, dbgoi.nhomtb_id
    from css_hcm.hd_Thuebao hdtb
        join css_hcm.hdtb_goi_dadv hdgoi on  hdgoi.hdtb_id = hdtb.hdtb_id 
        join  css_hcm.bd_goi_dadv dbgoi on  hdgoi.nhomtb_id = dbgoi.nhomtb_id 
        join css_hcm.hdtb_adsl hdtd on  hdtd.hdtb_id = hdtb.hdtb_id 
        join  css_hcm.tocdo_adsl td on  hdtd.tocdo_id = td.tocdo_id
        join css_hcm.db_thuebao db on hdtb.thuebao_id = db.thuebao_id
        join css_hcm.hd_khachhang hdkh on  hdtb.hdkh_id = hdkh.hdkh_id  
    where  hdtb.kieuld_id in (51, 280)  and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58  and dbgoi.trangthai = 1  and to_number(to_char(dbgoi.ngay_dk,'yyyymm')) = 202405
    and to_number(to_char(db.ngay_sd,'yyyymm')) = 202405 --and rownum =2

--    select  nvl(hdgoi.ctv_id,hdkh.ctv_id) ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd, dbgoi.ngay_dk, dbgoi.nhomtb_id
--    from css_hcm.hd_Thuebao hdtb
--        join css.hdtb_goi_dadv@dataguard hdgoi on  hdgoi.hdtb_id = hdtb.hdtb_id 
--        join  css.bd_goi_dadv@dataguard dbgoi on  hdgoi.nhomtb_id = dbgoi.nhomtb_id 
--        join css.hdtb_adsl@dataguard hdtd on  hdtd.hdtb_id = hdtb.hdtb_id 
--        join  css_hcm.tocdo_adsl td on  hdtd.tocdo_id = td.tocdo_id
--        join css.v_db_thuebao@dataguard db on hdtb.thuebao_id = db.thuebao_id
--        join css.v_hd_khachhang@dataguard hdkh on  hdtb.hdkh_id = hdkh.hdkh_id
--    where  hdtb.kieuld_id in (51, 280)  and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58  and dbgoi.trangthai = 1  and to_number(to_char(dbgoi.ngay_dk,'yyyymm')) = 202405
--    and to_number(to_char(db.ngay_sd,'yyyymm')) = 202405--
),
--SELECT * FROM ALL_IND_COLUMNS WHERE TABLE_NAME = upper('hd_khachhang');
homecb as (
    select distinct a.goi_id, a.loaitb_id, b.loaitb_id
    from css_hcm.goi_dadv_lhtb a
        join css_hcm.goi_dadv_lhtb b on a.goi_id = b.goi_id
    where a.loaitb_id = 58 and b.loaitb_id = 20
),
hometv as (
    select distinct a.goi_id, a.loaitb_id, b.loaitb_id
    from css_hcm.goi_dadv_lhtb a
        join css_hcm.goi_dadv_lhtb b on a.goi_id = b.goi_id
    where a.loaitb_id = 58 and b.loaitb_id = 61
),
mytv as (
    select a.thuebao_id, a.nhomtb_id--, row_number() over (partition by a.nhomtb_id  order by a.thuebao_id desc) rnk
    from css_Hcm.bd_goi_dadv a
        join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
    where a.trangthai = 1 and b.loaitb_id = 61 and b.trangthaitb_id not in (7,8,9) and to_number(to_char(b.ngay_sd,'yyyymm')) = 202405
)
select 
    202405 thang, 'HomeTV' Loai_kpi, hd.ma_tb, d.ma_goi, hd.ngay_dk, hd.ngay_sd, hd.thuonghieu, c.ma_nv, c.ma_to, c.ma_pb , hd.hdtb_id ,hd.thuebao_id, 
    hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id, null ghichu
from hd
    join admin_hcm.nhanvien_onebss b on hd.ctv_id = b.nhanvien_id
    left join ttkd_Bsc.nhanvien_202405 c on b.ma_nv = c.ma_nv
    join css_hcm.goi_Dadv d on hd.goi_id = d.goi_id 
    join mytv on hd.nhomtb_id = mytv.nhomtb_id --and rnk = 1
where hd.goi_id in (select goi_id from hometv) and hd.goi_id not in (select goi_id from homecb) and  ma_pb ='VNP0702200';
select* from css_hcm.Goi_dadv;

select distinct loai_kpi from ttkd_bsc.ct_Bsc_homecombo where thang = 202404 and loai_kpi in ('Fiber_hh','Fiber_moi','HomeTV') ;group by thuebao_id having count(thuebao_id) > 1;
select* from ttkd_bsc.nhanvien_202404 where ten_nv like '%Vân';
commit;
select ctv_id from css_hcm.hd_khachhang 