update ct_bsc_tratruoc_moi
set tien_khop = null
where thuebao_id in (select thuebao_id from ct_bsc_tratruoc_moi where tien_thanhtoan < 20000 and thang = 202311)
commit;
select thuebao_id from ct_bsc_tratruoc_moi where tien_thanhtoan < 20000 and thang = 202311
insert into
ttkd_Bsc.ct_bsc_trasau_tp_tratruoc
select* from ct_bsc_trasau_tp_tratruoc
where thang = 202311
commit;
select* from ttkd_bsc.ct_bsc_Tratruoc_moi where thang = 202311
insert into ttkd_bsc.ct_bsc_Tratruoc_moi_30day
select* from ct_bsc_Tratruoc_moi_30day where thang = 202311 and thuebao_id in (8253337,
9110816,
8253336)      
select* from  ttkd_bsc.ct_bsc_Tratruoc_moi_30day where thang = 202311 and thuebao_id in (8253337,
9110816,
8253336)      
select* from tmp3_30ngay where thuebao_id in (8253337,
9110816,
8253336)
-- BSC 60 NGAY
update ttkd_bsc.ct_bsc_tratruoc_moi a set TIEN_KHOP = 1
--update ct_bsc_tratruoc_moi a set TIEN_KHOP = 1

--select * from ttkd_bsc.ct_bsc_tratruoc_moi a
--select * from ct_bsc_tratruoc_moi a
where thang = 202401 and a.rkm_id is not null and tien_khop = 0
    and ht_tra_id in (2, 4 )  -- and a.phieutt_id = 7675246    
   -- and ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')
and  exists 
    (
        select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
        where  phieu_id = a.phieutt_id
        group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10
        )
                                                commit;   
                                                   rollback;
                ;
   select bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
        where   phieu_id = 7962864
        group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10
-- BSC 30 NGAY
--update ttkd_bsc.ct_bsc_tratruoc_moi_30day a set TIEN_KHOP = 1
--update ct_bsc_tratruoc_moi_30day a set TIEN_KHOP = 1
--select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
-- select * from ct_bsc_tratruoc_moi_30day a

where thang = 202401 and a.rkm_id is not null and tien_khop = 0
    and ht_tra_id in (2, 4)
    and  exists 
        (
            select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
            from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                            join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
            where  phieu_id = a.phieutt_id
            group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
            having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
            );
            commit;
 -- FIBER TRA SAU CHUYEN TRA TRUOC                                                                       
update ttkd_bsc.ct_bsc_trasau_tp_tratruoc a set TIEN_KHOP = 1
--update ct_bsc_trasau_tp_tratruoc a set TIEN_KHOP = 1
---select * from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
---select * from ct_bsc_trasau_tp_tratruoc a
where thang = 202401 and a.rkm_id is not null and TIEN_KHOP = 0
    and ht_tra_id in (2, 4)
    and  exists 
            (
                select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                where  phieu_id = a.phieutt_id
                group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
                )        
                commit;
   delete from           ttkd_bsc.ct_bsc_trasau_tp_tratruoc  where thang = 202311 and thuebao_id in (
select a.thuebao_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a 
where thang = 202311 and rkm_id is not null and 
exists (select 1 from v_Thongtinkm_all b where a.thuebao = b.thuebao_id and 
);


select* from v_Thongtinkm_all where rownum = 1