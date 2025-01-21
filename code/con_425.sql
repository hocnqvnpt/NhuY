with ds as (select distinct ma_tb from hethong)
,
ht as (select distinct ma_tb 
        from tien_pb_t8 a 
        where not exists (select * from thu_t8 where rkm_id = a.rkm_id) and nvl(ton_Ck ,0)!= 0)
select a.*, b.trangthai_Tb from tien_pb_t8 a 
    left join tmp_tttb b on a.thuebao_id = b.thuebao_id
    left join ds on a.ma_Tb = ds.ma_tb
where not exists (select * from thu_t8 where rkm_id = a.rkm_id) and nvl(ton_Ck ,0)!= 0 and ds.ma_Tb is null;
select* from ttkd_Bsc.nhanvien where thang = 202409 and ten_Nv like '%Ph??ng';
select ds.*
from ds
    left join ht on ds.ma_Tb= ht.ma_Tb
where ht.ma_Tb is null;
select* from v_Thongtinkm_all where ma_tb='huongbnh7228898';
select* from css.v_db_Datcoc@dataguard where ma_Tb='huongbnh7228898';
SELECT* FROM CSS_HCM.LOAI_HD WHERE LOAIHD_ID =7;
SELECT