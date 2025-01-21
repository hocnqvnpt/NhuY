drop table xgspon;
flashback table xgspon to before drop;
select * from ttkd_bsc.nhanvien where ma_nv ='CTV028990';
select hdtb_id from css_hcm.hd_Thuebao where ma_tb='lvthang91';
select* from qlvt.v_phieu_vt@dataguard a  where hdtb_id in (select hdtb_id from css_hcm.hd_Thuebao where ma_tb='lvthang91');
select* from  qlvt.v_thietbi@dataguard b  where thietbi_id =13262682;
create table xxxxx_3012 as
select  'HCM' ma_tinh,  c.ma_tb, cn.congnghe,vt.ten_Vt thietbi, tb.trangthaitb_id,  kh.loaihd_id,lhd.ten_loaihd,c.kieuld_id,kld.ten_kieuld , c.ngay_ht, 
    dv1.ten_dv pbh, dv2.ten_dv ttvt ,tt.trangthai_Tb
from qlvt.v_phieu_vt@dataguard a 
    join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id and a.phanvung_id = b.phanvung_id
    join css.v_hd_thuebao@dataguard c on a.hdtb_id = c.hdtb_id
    join css.v_db_adsl@dataguard db on c.thuebao_id = db.thuebao_id and db.congnghe_id in (3,11)
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
where a.phanvung_id =28  and  kh.loaihd_id in(1,13,8)  and c.tthd_id = 6 and tb.loaitb_id = 58
and to_char(c.ngay_Ht,'yyyymmdd') = '20241230'; b.vattu_id = 17676
select * css_hcm.db_Thuebao a join css_Hcm.db_adsl b on a;
select *from ttkd_Bsc.ct_bsc_tratruoc_moi where thang = 202410 and tien_khop = 0 and ma_pb ='VNP0702300';
select TB_TRASAU_TTVT, TB_TRASAU , TB_TRASAU_HTKH from REPORT.BC_GHTT_BIEU1_CHOT@dataguard;
select* from css_hcm.db_thuebao where ma_Tb='livein286';
select* from ttkd_Bsc.nhanvien where ma_nv='CTV086419';
select* from xgspon;
select vattu_id from qlvt.v_Thietbi@dataguard where thietbi_id in (
select thietbi_id from qlvt.v_sudung_vt@dataguard a where  thuebao_id=12614427 );
with tbi as (
   select db.ma_tb, a.thuebao_id, a.SDVT_ID, a.THIETBI_ID, a.NGAY_SD, a.TRANGBI_ID, a.SERIAL, db.trangthaitb_id,db.donvi_id
                                    , b.KHO_ID, b.VATTU_ID
                                    , c.TEN_VT, c.LOAITBI_ID, c.NHOMVT_ID, rank() over (partition by db.thuebao_id order by a.SDVT_ID desc) rnk
                       from qlvt.v_sudung_vt@dataguard a
                                        join css.v_db_thuebao@dataguard db on a.thuebao_id = db.thuebao_id 
                                        left join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id
                                        left join qlvt.v_vattu@dataguard c on b.vattu_id = c.vattu_id
                       where db.loaitb_id in (58, 59) --and db.trangthaitb_id not in (7,8,9)
                        and c.vattu_id = 17676 and db.ma_Tb='lvthang91' -- c.loaitbi_id = 2 and c.nhomvt_id = 1 and 
)
, ds as (
select 
    b.ma_tb, a.ngay_Yc, b.ngay_ht , a.ma_gd, ten_kieuld, ten_loaihd , nvl(pb.ten_dv,pbrp.ten_dv) phong_Bh, tbi.ten_vt thietbi
    ,trangthai_Hd, trangthai_Tb, row_number() over (partition by b.thuebao_id order by a.hdkh_id desc) rn, ttvt.ten_Dv ttvt,cn.congnghe
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
    left join css.congnghe@dataguard cn on c.congnghe_id =cn.congnghe_id
    left join admin.donvi@dataguard ttvt on tbi.donvi_id = ttvt.donvi_id
    left join admin.donvi@dataguard vt on ttvt.donvi_Cha_id = vt.donvi_id

where  d.loaihd_id in ( 8,1) 
)
select* from ds where rn = 1 and ma_tb = 'lvthang91';
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_SL_ORDER_001';
select* from ttkd_Bsc.bangluong_kpi where thang in (202411, 202410) and ma_kpi in ('HCM_SL_ORDER_001','HCM_SL_COMBO_006', 'HCM_TB_GIAHA_022','HCM_TB_GIAHA_023',
'HCM_TB_GIAHA_027','') AND TEN_VTCV LIKE 'Phó%' and nvl(giao,chitieu_giao) is null;
update ttkd_Bsc.bangluong_kpi set tytrong = 15 where thang in (202411, 202410) and ma_kpi = 'HCM_SL_BHOL_002'
and ma_Vtcv = 'VNP-HNHCM_KDOL_18';in ('VNP-HNHCM_BHKV_22','VNP-HNHCM_BHKV_27','VNP-HNHCM_BHKV_28','VNP-HNHCM_KDOL_4','VNP-HNHCM_KDOL_5');
commit;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202410 and ma_kpi = 'HCM_SL_ORDER_001';