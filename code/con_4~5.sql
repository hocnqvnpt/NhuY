update ttkd_Bsc.bangluong_kpi a
set tyle_thuchien = (select tyle from ttkd_Bsc.tl_giahan_tratruoc where thang =a.thang and ma_kpi = a.ma_kpi AND MA_PB = A.MA_PB AND LOAI_tINH ='KPI_PB')
where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_022' AND CHITIEU_GIAO IS NOT NULL;
commit;
select* from ttkd_Bsc.bangluong_kpi a where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_022' AND CHITIEU_GIAO IS NOT NULL;
select distinct 'HCM' ma_tinh,  c.ma_tb, cn.congnghe,vt.ten_Vt thietbi, tb.trangthaitb_id,  kh.loaihd_id,lhd.ten_loaihd,c.kieuld_id,kld.ten_kieuld , c.ngay_ht, 
    null ngay_Bc, dv1.ten_dv pbh, null,dv2.ten_dv ttvt
from qlvt.v_phieu_vt@dataguard a 
    join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id and a.phanvung_id = b.phanvung_id
    join css.v_hd_thuebao@dataguard c on a.hdtb_id = c.hdtb_id
    join css.v_db_adsl@dataguard db on c.thuebao_id = db.thuebao_id and db.congnghe_id =11
    join css.congnghe@dataguard cn on db.congnghe_id = cn.congnghe_id
    join qlvt.v_vattu@dataguard vt on b.vattu_id = vt.vattu_id
    left join css.v_hd_khachhang@dataguard kh on c.hdkh_id =kh.hdkh_id
    left join css.loai_Hd@dataguard lhd on kh.loaihd_id = lhd.loaihd_id
    left join css.v_Db_Thuebao@dataguard tb on c.thuebao_id = tb.thuebao_id
    left join css.kieu_ld@dataguard kld on c.kieuld_id = kld.kieuld_id
    left join admin.nhanvien@dataguard nv on kh.ctv_id = nv.nhanvien_id
    left join admin.donvi@dataguard dv on nv.donvi_id = dv.donvi_id
    left join admin.donvi@dataguard dv1 on dv.donvi_cha_id = dv1.donvi_id
    left join admin.donvi@dataguard tt on a.donvi_id = tt.donvi_id
    left join admin.donvi@dataguard dv2 on tt.donvi_cha_id = dv2.donvi_id

where a.phanvung_id =28 and b.vattu_id = 17676 and to_Number(to_char(c.ngay_ht,'yyyymmdd')) >= 20241114 and kh.loaihd_id in(1,13,8);
select* from qlvt.v_phieu_vt@dataguard where rownum =1;
select* from qlvt.v_vattu@dataguard where vattu_id = 17676;
select congnghe_id from css.v_db_adsl@dataguard; where hdtb_id in (27719346,27719141);
select* from css.congnghe@dataguard;