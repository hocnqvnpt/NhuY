with ipcc as (
    select thuebao_id, trangthai_ob, tg_dt, row_number() over (partition by thuebao_id order by tg_Dt desc) rn
    from temp_ipcc
),
ghtt as (
    select thuebao_id , to_Number(to_char(ngay_kt_Mg,'yyyymm')) thang_kt, row_number() over (partition by thuebao_id order by ngay_kt_mg desc) rn
    from ds_giahan_tratruoc2
    where to_Number(to_char(ngay_kt_Mg,'yyyymm')) between 202401 and 202407
)
,
giao as (
    select thuebao_id , thang_kt , row_number() over (partition by thuebao_id order by ngay_kt_mg desc) rn
    from ttkdhcm_ktnv.ghtt_giao_688
    where loaibo = 0 and km = 1 and tratruoc = 1
)
, 
km as (
    select thuebao_id, thang_ktdc,  row_number() over (partition by thuebao_id order by thang_kt_mg desc) rn
    from v_Thongtinkm_all
)
select a.thuebao_id, ma_tb, ma_gd, ngay_Bd_moi, ngay_kt_moi, tien_thanhtoan, vat, ngay_tt, ngay_hd,ngay_nganhang,kenhthu,ten_ht_Tra, manv_thuyetphuc,ma_to, ma_pb, manv_cn, phong_cn,
so_thangdc_moi, avg_thang, ngay_Yc, c.thang_kt --, d.thang_kt giao --, km.thang_ktdc
                                    , case when tien_khop in (1,0) then 'Chuyen khoan'
                                            when tien_khop = 2 then 'QR Code'
                                            when tien_khop = 3 then 'Thu tai nha - Co phi'
                                            when tien_khop = 4 then 'Thu tai nha - Khong phi' end ht_thanhtoan
                                    , case when b.thuebao_id is null then 'Khong co OB'
                                            when b.tg_dt = 0 then 'Co OB chua ket noi'
                                            when b.tg_Dt > 0 then 'Co OB, da ket noi' end tinhtrang_OB

from bang_gom a
    left join ipcc b on a.thuebao_id = b.thuebao_id and rn = 1
    left join ghtt c on a.thuebao_id = c.thuebao_id and c.rn = 1
--    left join giao d on a.thuebao_id = d.thuebao_id and d.rn = 1
--    left join km on a.thuebao_id = km.thuebao_id and km.rn = 2
    ;
    
    select* from temp_ipcc where thuebao_id = 9697828;
    