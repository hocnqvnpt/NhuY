create table bsc_quy as;
select * from (
select  ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_giahan_cntt a
                                          where thang_ktdc_cu = thang and thang between 202401 and 202409 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 )
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                            )
                group by  ma_pb, phong_giao
                order by 2
);
select c.ten_kh, tien_khop from ttkd_bsc.ct_bsc_giahan_cntt a
    join css_hcm.db_Thuebao b on a.thuebao_id =b.thuebao_id
    join css_hcm.db_khachhang c on b.khachhang_id = c.khachhang_id
            where thang_ktdc_cu = thang and thang =20240 and manv_Giao='VNP017793';
            select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202405 and ma_Nv='VNP017793';
;
select* from bsc_quy;
update bsc_quy set da_giahan = da_Giahan+4 where ma_pb = 'VNP0702300';
update bsc_quy set tong = tong+32 where ma_pb = 'VNP0702300';
update bsc_quy set tyle = round(da_Giahan*100/tong,4) where ma_pb = 'VNP0702300';

commit;
