(
;select *-- aa.NGAY_NGANHANG NGAY_NGANHANG, bb.phieu_id
                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT where bb.phieu_id = 8278166;
                                                    group by bb.phieu_id)