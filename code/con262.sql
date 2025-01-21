select ma_nv, ten_Nv, ma_to, ma_pb, ma_vtcv, ten_to, ten_pb , ten_vtcv
from nhanvien_202407_l2
minus;
select ma_nv, ten_Nv, ma_to, ma_pb, ma_vtcv, ten_to, ten_pb, ten_Vtcv
from ttkd_bsc.nhanvien where thang = 202407 and donvi = 'TTKD' and nhomld_id = 4;
delete from nhanvien_202407_l2 where ma_nv is null;
commit;
select* from css_hcm.loaihinh_Tb where loaihinh_Tb like '%MyTV%'    ;
select distinct ten_Vtcv from nhanvien_202407_l2;
select a.rkm_id, b.ma_tt, a.ma_Tb,c.ten_dvvt, lh.loaihinh_Tb , tt.trangthai_Tb, a.thang_bd, a.thang_kt, a.cuoc_tk,km.ngay_ktdc
     from qltn.v_db_datcoc@dataguard a
       join css.v_db_Thanhtoan@dataguard b on a.thanhtoan_id = b.thanhtoan_id
            left join css.dichvu_vt@dataguard c on a.dichvuvt_id = c.dichvuvt_id 
            left join css.loaihinh_Tb@dataguard lh on a.loaitb_id = lh.loaitb_id
            left join css.v_db_Thuebao@dataguard db on a.thuebao_id = db.thuebao_id
            left join css.trangthai_Tb@dataguard tt on db.trangthaitb_id = tt.trangthaitb_id
            left join v_Thongtinkm_all_ol km on a.rkm_id = km.rkm_id
        where a.phanvung_id=28  and kyhoadon=20240701 and nvl(cuoc_tk,0)!=0 ;
select* from ttkd_Bsc.nhanvien where thang = 202408 and ma_Nv = 'CTV079583';