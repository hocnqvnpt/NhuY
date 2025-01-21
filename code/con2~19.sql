select b.ma_Tb, a.ma_Gd, d.tien, d.vat, d.tien+d.vat  tong_Tien, seri,  soseri, e.ngay_hd
from dichvucA C 
    JOIN css_Hcm.hd_khachhang a on a.ma_gd = c.ma_Gd
    join css_hcm.hd_Thuebao b on a.hdkh_id = b.hdkh_id
    left join css_Hcm.ct_phieutt d on b.hdtb_id = d.hdtb_id
    left join css_Hcm.phieutt_Hd e on d.phieutt_id = e.phieutt_id
order by  a.ma_gd;

select ngay_Hd from css.v_phieutt_hd@dataguard where ma_gd='HCM-LD/02035319';

select* from ttkd_Bsc.nhanvien where thang = 202411 and donvi = 'TTKD' ;

select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202411 and loai_Tinh ='DONGIATRA_OB';and ma_Tb='tchinhd-h5';
SELECT* FROM TTKDHCM_KTNV.ds_Chungtu_Nganhang_oneb WHERE MA_CT='VCB_20241124_474659';
SELECT* FROM TTKDHCM_KTNV.ds_chungtu_nganhang_phieutt_hd_1 WHERE MA ='HCM-TT/03050731';
SELECT* FROM CSS.V_PHIEUTT_hD@DATAGUARD WHERE MA_gD='HCM-TT/03050731';
SELECT* FROM TTKDHCM_KTNV.ds_chungtu_nganhang_pqt_sudung; WHERE MA ='HCM-TT/03050731'; ds_chungtu_nganhang_pqt_sudung;

select sum(ton_ck) from (
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck

from dhsx.v_db_Datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101

and ngay_cn <to_date('20241202','yyyyMMdd')

and (ngay_thoai is null or trunc(ngay_thoai)< to_date('20241201','yyyyMMdd'))

and tiengach is null

union all

/*Danh sách rkm_id có thoái c?c t? 1/12/2024*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tien_thoai,0) ton_ck

from dhsx.v_db_Datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101

and ngay_cn <to_date('20241202','yyyyMMdd')

and trunc(ngay_thoai)>= to_date('20241201','yyyyMMdd')

and tiengach is null

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch trên form g?ch n? ti?n m?t)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tiengach,0) ton_ck

from dhsx.v_db_Datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101

and ngay_cn <to_date('20241202','yyyyMMdd')

and tiengach >=0

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch theo d/s ch? Thanh gui)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(cuoc_tk_lui,0) ton_ck

from dhsx.v_db_Datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101

and ngay_cn <to_date('20241202','yyyyMMdd')

and cuoc_tk_lui<>0) 