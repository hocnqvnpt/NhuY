select* from v_Thongtinkm_all where ma_Tb ='tr-ngocanh';
select* from admin_Hcm.donvi where ten_dv like '%T?%';
select* from ttkd_bsc.nhANVIEN WHERE THANG= 202411 AND TEN_tO LIKE '%Tr??ng';
with ton as (
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck, thang_kt

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and (ngay_thoai is null or trunc(ngay_thoai)< to_date('20250101','yyyyMMdd'))

and tiengach is null

union all

/*Danh sách rkm_id có thoái c?c t? 1/12/2024*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tien_thoai,0) ton_ck, thang_kt

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and trunc(ngay_thoai)>= to_date('20250101','yyyyMMdd')

and tiengach is null

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch trên form g?ch n? ti?n m?t)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tiengach,0) ton_ck, thang_kt

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and tiengach >=0

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch theo d/s ch? Thanh gui)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(cuoc_tk_lui,0) ton_ck, thang_kt

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and cuoc_tk_lui<>0
)
select t.*, tt.trangthai_Tb, lh.loaihinh_Tb
from ton t
    left join css.v_db_thuebao@dataguard db on t.thuebao_id = db.thuebao_id
    left join css.trangthai_tb@dataguard tt on db.trangthaitb_id = tt.trangthaitb_id
    left join css.loaihinh_Tb@dataguard lh on db.loaitb_id = lh.loaitb_id
 