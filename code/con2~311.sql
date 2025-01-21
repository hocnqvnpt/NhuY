select* from v_Thongtinkm_all where ma_tb = 'hnp55';
rollback;
commit;
delete from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'DONGIATRA_OB_BS';
select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'DONGIATRA_OB_BS' and ma_pb = 'VNP0701400';
insert into ttkd_bsc.tl_giahan_tratruoc (thang,ma_kpi, loai_tinh, ma_nv, ma_to, ma_pb, tien)
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
select 202404, 'DONGIA','DONGIATRA_OB_BS',a.ma_nv, b.ma_to, b.ma_pb,  a.tien
from tien a
    join ttkd_bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv
    where ma_pb!= 'VNP0701400';
    commit;
select* from bang_gom a where thuebao_id not in (select thuebao_id from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt in (202403,202404,202405)) and ngay_bd_moi  >= 20240301
and not exists (select 1 from v_thongtinkm_all where thuebao_id = a.thuebao_id and nvl(thang_Huy, thang_kt_dc) = round(ngay_bd_moi/100));
;
select* from bang_gom a where   exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id)