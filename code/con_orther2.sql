select* from (
select thuebao_id from css_hcm.bd_goi_dadv a where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                               
           group by thuebao_id having count(thuebao_id) > 1                                                                                                      --   and nhomtb_id not in (2691065)
                ) where  thuebao_id in  (select thuebao_id from  ttkdhcm_ktnv.ghtt_giao_688
                    where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202402)
                    
                    select* from admin_hcm.nhanvien_onebss where ma_nv in ('CTV030901','CTV051867','CTV062741');
                    select* 
                    select* from admin_hcm.donvi where donvi_id in (283881,283872,11299);

select* from ttkd_Bsc.ct_bsc_tratruoc_moi where thang = 202403  and  phieutt_id = 8038783
 select bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                    where  phieu_id = 8212830
                    ;
                    select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                    select ht_tra_id from css.v_phieutt_hd@dataguard where phieutt_id in (8155232,8222739)
where thang = 202403 and a.rkm_id is not null and tien_khop = 0
and ht_tra_id in (2, 4,5)
and  exists 
(
    select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
    where  phieu_id = a.phieutt_id  
    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
    );
    select tien_nhapthem from  ttkdhcm_ktnv.phieutt_hd_dongbo  where  phieu_id = 8038783;
select ngay_bddc, ngay_thoai, ngay_huy, ngay_ktdc from v_thongtinkm_all where rkm_id in (
select rkm_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a 
-- delete from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
where rkm_id is not null and thang = 202403  );