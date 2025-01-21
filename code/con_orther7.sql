select THANG_KT, MA_TB, MA_KH, SO_DT, THUEBAO_ID, KHACHHANG_ID,  DS_MA_KH from (
with thuebao as(
    select c.ma_kh, c.so_dt,a.thuebao_id
    from css_Hcm.db_Thuebao a 
        join css_hcm.db_Adsl b on a.thuebao_id = b.thuebao_id
        join css_hcm.db_khachhang c on a.khachhang_id = c.khachhang_id
    where trangthaitb_id not in (7,8,9) and chuquan_id = 145 and loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
) 

select to_Number(to_char(ngay_kt_Mg,'yyyymm')) thang_kt, a.ma_tb, b.ma_kh, b.so_Dt, a.thuebao_id, b.khachhang_id,row_number() over (partition by a.ma_tb order by a.ngay_kt_mg) rn,
    listagg(distinct c.ma_kh, ',') within group (order by c.ma_kh) ds_ma_kh
--, c.ma_kh makh, tb.ma_Tb matb, 
--    row_number() over(partition by a.ma_tb, b.ma_kh,b.so_dt,a.thuebao_id, b.khachhang_id order by tb.thuebao_id) as rn
--    listagg(distinct c.ma_kh, ',') within group (order by c.khachhang_id) ds_ma_kh,
--    listagg(distinct tb.ma_tb, ',') within group (order by tb.ma_tb) ds_ma_Tb
from ds_giahan_Tratruoc2 a
     join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
     join thuebao c on b.so_dt = c.so_dt and b.ma_kh != c.ma_kh
--left join css_hcm.db_Thuebao tb on c.khachhang_id = tb.khachhang_id
--left join css_hcm.db_adsl ad on tb.thuebao_id = ad.thuebao_id
where to_Number(to_char(ngay_kt_Mg,'yyyymm')) between 202405 and 202407 
group by a.ma_tb, b.ma_kh, b.so_Dt, a.thuebao_id, b.khachhang_id, ngay_kt_mg) where rn = 1;

select THANG_KT, MA_TB, MA_KH, SO_DT, THUEBAO_ID, KHACHHANG_ID,  listagg(distinct a.makh, ',') within group (order by a.makh) ds_ma_kh
--    listagg(distinct a.matb, ',') within group (order by a.matb) ds_ma_Tb
from nhieu_makh a
--where rn < 50
group by THANG_KT, MA_TB, MA_KH, SO_DT, THUEBAO_ID, KHACHHANG_ID
;
select so_Dt from css_hcm.db_khachhang where khachhang_id = 9805816;
select* from css_hcm.db_Thuebao where khachhang_id in (select khachhang_id from css_hcm.db_khachhang where so_Dt = '0868635436');
select chuquan_id from css_hcm.db_adsl where thuebao_id in (9548811,9049012,9109968)