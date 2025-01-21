select* from ct_Bsc_Tratruoc_moi_30day where thang = 202409;
insert into tl_giahan_tratruoc (thang, ma_pb, tong,da_giahan_dung_hen,dthu_duytri, dthu_thanhcong_dung_hen, tyle)
select thang, ma_pb, tong,da_giahan,dthu_duytri, dthu_thanhcong_dung_hen, tyle
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ct_bsc_tratruoc_moi_30day a
                                        where thang = 202409
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
                ) a  
;
commit;
select a.*, 
    case when a.giao != b.giao then 1 else 0 end giao_Td,
    case when  a.thuchien != b.thuchien  then 1 else 0 end thuchien_Td,
    case when  a.mucdo_hoanthanh != b.mucdo_hoanthanh   then 1 else 0 end mdht_Td,
    case when  a.diem_cong != b.diem_cong   then 1 else 0 end dc_Td,
    case when  a.diem_tru != b.diem_tru  then 1 else 0 end dt_Td
from ttkd_Bsc.bangluong_kpi_202408_dot3 a
    JOIN TTKD_bSC.BANGLUONG_KPI b on a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi and b.thang = 202408
where a.giao != b.giao or a.thuchien != b.thuchien or a.mucdo_hoanthanh != b.mucdo_hoanthanh or a.diem_cong != b.diem_cong or a.diem_tru != b.diem_tru