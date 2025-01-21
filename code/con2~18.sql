select * from v_thongtinkm_all where rownum =1;
select mnv_Tt from ct_homecombo_2024 where thang = 202410 and mnv_Tt is not null and ma_pb is null;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_Kpi = 'HCM_SL_COMBO_006';
select* from ttkd_Bsc.nhanvien where thang = 202410 and ma_Nv='CTV086809'; and
ma_Nv in;
(select * from ct_homecombo_2024 where thang = 202410 and mnv_Tt ='CTV086809'); is not null and ma_pb is null);
select* from css.v_loaihinh_tb where lower(loaihinh_Tb) like '%ticket%';
select* from  css.v_khuyenmai_dbtb ;
select* from 
select * from ttkd_bsc.dm_loaihinh_hsqd where dv_chitiet='Hoa don dien tu';
with hddc as (select distinct hdtb_id, g.tien_td,g.tien_tb,g.thang_Bd,g.thang_kt,g.thang_bd_mg, g.thang_kt_mg,  chitietkm_id
                from css.v_hdtb_datcoc@dataguard g 
    )
, kmtb as (
    select tien_td, tien_tb,  thang_ktdc, thang_bd,thang_kt  , hdtb_id, chitietkm_id, thang_huongdc, thang_Huongkm
    from css.v_khuyenmai_hdtb@dataguard
--            where datcoc_csd > 0 --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
)

SELECT 
    a.ma_Gd,nvl(db.ma_tb, b.ma_Tb) ma_tb
    , a.ngay_yc, nvl(hddc.tien_td,kmtb.tien_td) tien_td, nvl(hddc.tien_tb,kmtb.tien_tb) tien_tb,b.hdtb_id
    ,b.ngay_tt,nvl(hddc.thang_kt, to_number(to_char(ADD_MONTHS(nvl(b.ngay_tt,b.ngay_cn),kmtb.thang_huongdc - 1),'yyyymm'))) thang_kt
    ,nvl(hddc.thang_bd, to_number(to_char(nvl(b.ngay_tt,b.ngay_cn),'yyyymm'))) thang_bd
    ,nvl(hddc.thang_kt_Mg, to_number(to_char(ADD_MONTHS(nvl(b.ngay_tt,b.ngay_cn),kmtb.thang_huongkm - 1),'yyyymm'))) thang_kt_mg
    ,nvl(hddc.thang_bd_mg, to_number(to_char(nvl(b.ngay_tt,b.ngay_cn),'yyyymm'))) thang_bd_mg
    , ct.chitietkm_id, ct.khuyenmai_id, loaihinh_Tb,a.ten_kh, a.diachi_Kh, nv.ten_nv, pb.ten_dv, mc.muccuoc,  mc.cuoc_Tg, b.loaitb_id, hd.trangthai_hd
--    ,nvl(hddc.thang_bd, to_number(to_char(b.ngay_tt,'yyyymm'))) thang_bd,
FROM css.v_hd_khachhang@dataguard a
    join css.v_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    left join css.v_ct_tienhd@dataguard d on b.hdtb_id = d.hdtb_id and d.khoanmuctt_id = 11 
    left join css.v_ct_phieutt@dataguard ptt on d.hdtb_id = ptt.hdtb_id and ptt.khoanmuctt_id = 11 
    left join hddc on b.hdtb_id  = hddc.hdtb_id
    left join kmtb on b.hdtb_id = kmtb.hdtb_id
    left join css.v_hdtb_cntt@dataguard hd on b.hdtb_id = hd.hdtb_id
    left join css.v_ct_khuyenmai@dataguard ct on nvl(hddc.chitietkm_id, kmtb.chitietkm_id) = ct.chitietkm_id
    left join css.loaihinh_Tb@dataguard lh on b.loaitb_id = lh.loaitb_id
    left join css.v_hd_Thanhtoan@dataguard tt on b.hdtt_id = tt.hdtt_id
    left join admin.nhanvien@dataguard nv on a.nhanvien_id = nv.nhanvien_id
    left join admin.donvi@dataguard dv on nv.donvi_id = dv.donvi_id
    left join admin.donvi@dataguard pb on dv.donvi_cha_id = pb.donvi_id
    left join css.v_db_Thuebao@dataguard db on b.thuebao_id = db.thuebao_id
    left join css.v_muccuoc_Tb@dataguard mc on nvl(db.mucuoctb_id,b.mucuoctb_id) = mc.mucuoctb_id
    left join css.trangthai_Hd@dataguard hd on b.tthd_id = hd.tthd_id
where  b.tthd_id < 6 and b.loaitb_id in (2116,290,373,175, 319) and nvl(db.ma_tb, b.ma_Tb) is not null ;and nvl(cuoc_tg,0)>0 and a.ma_gd ='HCM-LD/01934683';
    and nvl(ptt.tien, d.tien) > 0;
    select* from css.v_Hd_khachhang@dataguard where ma_gd='HCM-LD/01363969';
    select* from css.v_Hd_Thuebao@dataguard where hdkh_id = 18417484;
select* from css.v_ct_tienhd@dataguard where hdtb_id =26598064;
select* from css.v_ct_phieutt@dataguard where  hdtb_id =26598064; phieutt_id = 8861160;
select loaitb_id from  css.v_hd_thuebao@dataguard where ma_Tb='hcm_bldt_00000456';
select* from css_hcm.khoanmuc_Tt where khoanmuctt_id =5;
select * from css_hcm.hd_thuebao where ma_Tb='hcm_hddt_mtt_00000611';
select * from css.v_hd_Thuebao where loaitb_id in (2116,290,373,175, 319) and ma_tb is null and tthd_id < 6;
    select* from css.v_hd_Thanhtoan@dataguard ;where ma_Tb='hcm_bldt_00000568';