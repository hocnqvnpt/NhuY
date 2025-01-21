-- ky luong thang 2, thanh toan thang 2 -> danh sach het han thang 3 -> thang bsc thang 4
select thang, ma_pb, phong_giao
     , count(thuebao_id) tong
     , sum(case when dthu > 0 and tien_khop > 0  and  to_number(to_char(ngay_Tt,'yyyymm')) = 202402 then 1 else 0 end) da_giahan
     , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
     , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0  and to_number(to_char(ngay_Tt,'yyyymm')) = 202402
     then 1 else 0 end)*100/count(thuebao_id), 2) tyle
from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                         , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                     from ct_bsc_tratruoc_moi_30day a
                      where thang = 202403
                    group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
           )
group by thang, ma_pb, phong_giao
order by 2;

select distinct ht_Tra_id, ten_ht_Tra from ct_bsc_tratruoc_moi_30day where thang = 202403 and rkm_id is not null;
update ct_bsc_tratruoc_moi_30day set tien_khop = 1 where ht_Tra_id = 204 and thang = 202403
commit;