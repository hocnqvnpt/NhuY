select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202408
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
union all
--2
select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202410
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
union all --3
select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202411 
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
union all --4
select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202409
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
 union all
 select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                        where thang = 202408
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
union all
select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                        where thang = 202410
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
union all
select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                        where thang = 202411 
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)
union all
select thang, -- case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null
from 
(
        select thang
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                        where thang = 202409
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
)