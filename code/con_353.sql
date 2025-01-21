drop table cohong;
select* from tmp3_60ngay where ma_Tb ='httlieu_2019';
select a.ma_Gd, c.ma_tb, e.ten_dv
from css_hcm.hd_khachhang a
    join cohong b on a.ma_gd = b.MÃ_GT_?HSXKD
    join css_hcm.hd_thuebao c on a.hdkh_id = c.hdkh_id
    left join admin_hcm.donvi d on a.donvi_id = d.donvi_id
    left join admin_hcm.donvi e on d.donvi_cha_id = e.donvi_id;
select* from   cohong;
select* from css_hcm.db_thuebao where ma_Tb='myloc1970';
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi a 
where thang = 202412 and tien_khop is not null and not exists (
select 1 from ds_giahan_Tratruoc2_v2 where to_number(to_char(ngay_kt_Mg,'yyyymm')) > 202411 and thuebao_id = a.thuebao_id);

with tt as (
    select thuebao_id
    from ds_giahan_tratruoc2_v2
    where to_number(to_char(ngay_kt_mg,'yyyymmdd'))>=20250103
    group by thuebao_id
)
, tb as (
    select ma_Tb
    from matb_mytv
    group by ma_Tb
)
, mytv_dadv as (
    select a.thuebao_id
    from css_Hcm.bd_goi_Dadv a
      join css_Hcm.db_Thuebao b on a.thuebao_id =b.thuebao_id
      join css_hcm.bd_goi_dadv c on a.nhomtb_id = c.nhomtb_id and a.thuebao_id != c.thuebao_id
      join css_hcm.db_Thuebao d on c.thuebao_id = d.thuebao_id and d.loaitb_id in ( 58,59)
    where a.trangthai = 1 and b.loaitb_id in (61,171,271) and a.goi_id not between 1715 and 1726 and a.goi_id not in (15414, 16221) and a.goi_id < 100000
    group by a.thuebao_id
)

, fb as (
    select khachhang_id
    from css_hcm.db_thuebao
    where loaitb_id in (58,59) and trangthaitb_id not in (7,8,9)
    group by khachhang_id
)
select k.*, case when tt.thuebao_id is not null then 1 else 0 end tratruoc, case when c.httt_id in (2, 5) then 1 else  0 end ma_g, 
        case when c.httt_id not in (2, 5) then 1 else  0 end ma_h   , case when g.thuebao_id is not null then 1 else 0 end goi_fiber
        , case when g.thuebao_id is null and fb.khachhang_id is null  then 1 else 0 end mytv_donle
from tb a
    join css_hcm.db_Thuebao b on a.ma_tb = b.ma_Tb and dichvuvt_id = 4
    left join css_hcm.db_Thanhtoan c on b.thanhtoan_id = c.thanhtoan_id
    left join tt on b.thuebao_id = tt.thuebao_id
    left join mytv_dadv g on b.thuebao_id = g.thuebao_id
    left join fb on b.khachhang_id = fb.khachhang_id
    join ktkh_kh k on a.ma_Tb = k.ma_tb