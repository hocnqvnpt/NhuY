select* from v_thongtinkm_all_ol where thuebao_id = 8157103
6
1
7
2
4
select distinct ht_Tra_id from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202312
select* from css_hcm.hinhthuc_tra
select ma_kh, ten_kh from css_hcm.db_khachhang where khachhang_id = 9091836;
with ct as  (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                                    group by bb.phieu_id)
select distinct ht_Tra_id from css_hcm.phieutt_hd where  to_number(to_char(ngay_tt, 'yyyymmdd')) between 20231101 and 20240202 and phieutt_id in
(select phieu_id from ct where  to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20231101 and 20240202) ; -- ht_Tra_id in (6,1,7,2,4,89,208)
select* from css_hcm.hinhthuc_Tra where ht_tra_id in (6,1,7,2,4)
