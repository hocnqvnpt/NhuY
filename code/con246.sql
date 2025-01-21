--select sum (tien) from (
with tien as (
    SELECT MA_nV , SUM(TIEN_tHUYETPHUC) TIEN FROM (
    select ma_nv, tien_thuyetphuc
    from dongia_ob_final a where not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) 
    and  not exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id)
--    and ma_Tb in (select ma_Tb from ma_Tb where nguon = 'nsg')
    union all 
    select NHANVIEN_XUATHD, tien_xuathd
    from dongia_ob_final a where not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) 
    and not exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id)
--    and ma_Tb in (select ma_Tb from ma_Tb where nguon = 'nsg')

    )
    GROUP BY MA_NV
)
select 202404, 'DONGIA','DONGIATRA_OB_BS',a.ma_nv, b.ma_to, b.ma_pb,ten_pb, a.tien--,  sum(a.tien_thuyetphuc)
from tien a
    join ttkd_bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv
    where ma_pb != 'VNP0701400' and ma_pb not in ('VNP0702400','VNP0702500','VNP0702300');
    group by a.ma_nv, b.ma_to, b.ma_pb;
select* from ttkd_Bsc.tl_giahan_Tratruoc where loai_tinh =  'DONGIATRA_OB_BS' and   ma_pb not in ('VNP0702400','VNP0702500','VNP0702300','VNP0701400');

delete from dongia_bosung_t4_1 where thuebao_id  in (select thuebao_id from dongia_bosung_t4);
commit;
select* from dongia_bosung_t4_1 where thuebao_id in (9289617,3024117,5939542,8240832,2800052,8575979,8575981)