create table xgspon as
select distinct 'HCM' ma_tinh,  c.ma_tb, cn.congnghe,vt.ten_Vt thietbi, tb.trangthaitb_id,  kh.loaihd_id,lhd.ten_loaihd,c.kieuld_id,kld.ten_kieuld , c.ngay_ht, 
    dv1.ten_dv pbh, dv2.ten_dv ttvt ,tt.trangthai_Tb
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
    left join css.trangthai_tb@dataguard tt on tb.trangthaitb_id = tt.trangthaitb_id
where a.phanvung_id =28 and b.vattu_id = 17676 and  kh.loaihd_id in(1,13,8) ;and tb.ma_tb='tommywang394';
select * css_hcm.db_Thuebao a join css_Hcm.db_adsl b on a;
select* from qlvt_hcm.phieu_vt;
select* from qlvt.v_thietbi@dataguard a
    left join qlvt.v_Vattu@dataguard b on a.vattu_id = b.vattu_id
    where thietbi_id in (5768845,5613873,5571231,5693840,5711903,5711913,12004913);
select * from qlvt.v_sudung_Vt@dataguard where thuebao_id in (8882591);
    join ;
select* from qlvt.v_phieu_vt@dataguard a where hdtB_id in (26889038,26153433,21094244,24425548,24437250,23704807);
create table dulieu_kn_ont as
                       select db.ma_tb, a.thuebao_id, a.SDVT_ID, a.THIETBI_ID, a.NGAY_SD, a.TRANGBI_ID, a.SERIAL
                                    , b.KHO_ID, b.VATTU_ID
                                    , c.TEN_VT, c.LOAITBI_ID, c.NHOMVT_ID, rank() over (partition by db.thuebao_id order by a.SDVT_ID desc) rnk
                       from qlvt.v_sudung_vt@dataguard a
                                        join css.v_db_thuebao@dataguard db on a.thuebao_id = db.thuebao_id 
                                        left join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id
                                        left join qlvt.v_vattu@dataguard c on b.vattu_id = c.vattu_id
                       where c.loaitbi_id = 2 and c.nhomvt_id = 1 and db.loaitb_id in (58, 59) and db.trangthaitb_id not in (7,8,9)
               ;
            select * from  qlvt.vattu@dataguard_vttp;
            select * from hocnq_ttkd.dulieu_kn_ont where rnk = 1;
      
            create index nhuy.idx_ont_tbid on nhuy.dulieu_kn_ont(thuebao_id);
with tbi as (
   select db.ma_tb, a.thuebao_id, a.SDVT_ID, a.THIETBI_ID, a.NGAY_SD, a.TRANGBI_ID, a.SERIAL, db.trangthaitb_id
                                    , b.KHO_ID, b.VATTU_ID
                                    , c.TEN_VT, c.LOAITBI_ID, c.NHOMVT_ID, rank() over (partition by db.thuebao_id order by a.SDVT_ID desc) rnk
                       from qlvt.v_sudung_vt@dataguard a
                                        join css.v_db_thuebao@dataguard db on a.thuebao_id = db.thuebao_id 
                                        left join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id
                                        left join qlvt.v_vattu@dataguard c on b.vattu_id = c.vattu_id
                       where c.loaitbi_id = 2 and c.nhomvt_id = 1 and db.loaitb_id in (58, 59) --and db.trangthaitb_id not in (7,8,9)
                        and c.vattu_id = 17676 
)
, ds as (
select 
    b.ma_tb, a.ngay_Yc, b.ngay_ht , a.ma_gd, ten_kieuld, ten_loaihd , nvl(pb.ten_dv,pbrp.ten_dv) phong_Bh, tbi.ten_vt thietbi
    ,trangthai_Hd, trangthai_Tb, row_number() over (partition by b.thuebao_id order by a.hdkh_id desc) rn
from css.v_hd_khachhang@dataguard a
    left join css.v_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    join css.v_db_adsl@dataguard c on b.thuebao_id = c.thuebao_id and c.congnghe_id = 11
    left join css.loai_hd@dataguard d on a.loaihd_id = d.loaihd_id
    left join css.kieu_ld@dataguard e on b.kieuld_id = e.kieuld_id
    left join admin.nhanvien@dataguard nvtp on a.ctv_id = nvtp.nhanvien_id
    left join admin.donvi@dataguard dv on nvtp.donvi_id = dv.donvi_id
    left join admin.donvi@dataguard pb on dv.donvi_cha_id = pb.donvi_id
    left join admin.donvi@dataguard t on a.donvi_id = t.donvi_id
    left join admin.donvi@dataguard pbrp on t.donvi_cha_id = pbrp.donvi_id
    join tbi on b.thuebao_id = tbi.thuebao_id
    left join css.trangthai_hd@dataguard tthd on b.tthd_id = tthd.tthd_id
    left join css.trangthai_Tb@dataguard tt on tbi.trangthaitb_id =tt.trangthaitb_id
--    join DULIEU_KN_ONT f on b.thuebao_id = f.thuebao_id
    left join css.congnghe@dataguard cn on c.congnghe_id =cn.congnghe_id
where  d.loaihd_id in ( 8,1)
)
select* from ds where rn = 1;
;
select* from css.trangthai_Tb@dataguard;


select* from css_hcm.hd_Thuebao where ma_tb='tommywang394';
select* from ttkd_bsc.bangluong_kpi where ten_Nv like '%Kiên';
select a.ma_tb, cn.congnghe, d.ten_vt thietbi,tt.trangthai_Tb
from css.v_db_Thuebao@dataguard a
    join  qlvt.v_sudung_Vt@dataguard b on a.thuebao_id = b.thuebao_id 
    join css.v_ds_adsl@dataguard c on a.thuebao_id = c.thuebao_id and db.congnghe_id =11
    join  qlvt.v_thietbi@dataguard d on b.thietbi_id = d.thietbi_id
    join qlvt.v_Vattu@dataguard e on d.vattu_id = e.vattu_id and  b.vattu_id = 17676
    join css.congnghe@dataguard cn on c.congnghe_id = cn.congnghe_id
    join css.v_trangthai_tb@dataguard tt on a.trangthaitb_id =tt.trangthaitb_id
;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where ma_kpi ='HCM_TB_GIAHA_027' AND THANG = 202410;
select * from ttkd_bsc.blkpi_diem_202410_20241121_0002;

select* from (
select NGANHANG, MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB, MA_GD, LOAIHINH_TB, GHICHU, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, TRA_TRUOC, CHUNGTU_ID
,A.NV_GACH ma_nv, TINH_DONGIA, A.TINH_BSC, a.thang, B.MA_TO,'VNP0700900' MA_PB
from nhuy.ct_BSC_chungtu_temp a
   left join ttkd_Bsc.nhanvien b on a.nv_Gach = b.ma_Nv and b.thang = a.thang and b.donvi ='TTKD'
where a.thang = 202410
)