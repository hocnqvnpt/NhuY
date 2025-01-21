select * from (select THANG, MA_KPI, TEN_KPI, MA_PB, TEN_PB, TYTRONG, DONVI_TINH, GIAO, THUCHIEN, TYLE_THUCHIEN, TYLE_TANGTRUONG, MUCDO_HOANTHANH, DIEM_CONG, DIEM_TRU, DIEM_QUYDOI, GHICHU, to_char(NGAY_INSERT, 'dd/mm/yyyy') NGAY_INSERT
from ttkd_bsc.khkt_bsc_phong
where ma_kpi not in ('HCM_DT_01', 'HCM_Test') and thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)'
order by ma_kpi
)
;
select * from ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202409 ;and tien_khop is null and ht_Tra_id is not null;
;
select rkm_id from tmp3_60ngay ;where thuebao_id = 8979379;
update ttkd_bsc.khkt_bsc_phong a
set thuchien = (select tyle from rmp_bsc_phong where a.ten_kpi = ten_kpi and a.ma_pb = ma_pb)
where thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)';
commit;

update ttkd_Bsc.khkt_bsc_phong a
set tyle_thuchien = round(thuchien*100/giao,4)
where thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)';
update ttkd_Bsc.khkt_bsc_phong a
set mucdo_Hoanthanh = case when thuchien >= 90 then 120
                            when thuchien >= 80 and thuchien < 90 then 100 + (thuchien-80)
                            when thuchien >= 65 and thuchien < 80 then 100 - 2*(80-thuchien)
                            when thuchien < 65 then 0 end
where thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)';
update ttkd_Bsc.khkt_bsc_phong a
set diem_quydoi = round(mucdo_Hoanthanh*tytrong/100,4)
where thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)';
