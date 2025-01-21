update ttkd_Bsc.ct_Bsc_tratruoc_moi_30day set tien_khop = 1 where thang = 202407 and rkm_id is not null and tien_khop is null and ht_Tra_id = 216;
commit;
alter 
delete from Ct_Bsc_slpt_chua_xuly where thang = 202407 and ma_Tb in ('tamson_42c','thienphu1715605','hk_2019');
select* from  ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202407 and rkm_id is not null and tien_khop is null ;
select* from css_hcm.hinhthuc_Tra where ht_Tra_id = 216;
select* from ttkd_Bsc.nhanvien_202406 where ten_nv like '%Ý';
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202407
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
                ) a