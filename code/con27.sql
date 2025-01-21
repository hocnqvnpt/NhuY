select a.* from qltn_hcm.bangphieutra partition for (20240201) a
    join css_hcm.db_thuebao b on a.thanhtoan_id = b.thanhtoan_id
    where b.loaitb_id = 58 and b.trangthaitb_id = 1;-- —> b?ng ti?n tr? chi ti?t thuê bao

select* from qltn_hcm.hinhthuc_Tt where httt_id in (113,17);

select --A.THANG, COUNT(DISTINCT A.THUEBAO_ID)
    a.thang, count(distinct case when c.khdn = 1 then a.thuebao_id else null end) KHDN,
    count(distinct case when c.khdn = 0 then a.thuebao_id else null end) KHCN
from hocnq_ttkd.ds_giahan_Tratruoc a
    join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
WHERE A.TRATRUOC = 1
GROUP BY A.THANG;