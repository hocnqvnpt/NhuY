select* from dongia_ob_final where thuebao_id in (
select * from bang_gom where ht_Tra_id in (214,216)) and thuebao_id not in (select thuebao_id from ttkd_bsc.ct_dongia_tratruoc where thang = 202404);

create table temp_Chot as select* from bang_gom where ht_Tra_id in (214,216);

select ht_tra_id, ten_ht_tra, tien_khop from temp_chot --where thuebao_id not in (select thuebao_id from ttkd_bsc.ct_dongia_tratruoc where thang = 202404);  
update temp_chot set tien_khop = 2 where ht_tra_id = 216 and tien_khop != 2;
rollback;
commit;
select* from v_Thongtinkm_all where ma_tb = 'workshopoto';
select* from v_Thongtinkm_all where ma_tb = 'trieuhung103';

update temp_chot a set TIEN_KHOP = 1
                -- select * from temp_chot a
                where thang = 202404 and a.rkm_id is not null and tien_khop = 0 --and so_Thangdc_cu = -1-- and thuebao_id in (select thuebao_id from xsmax)
                                and ht_tra_id in (2,4,5,214)-- and a.phieutt_id = 7675246    
                               -- and ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')
                                and  exists 
                                (
                                    select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                    where  phieu_id = a.phieutt_id
                                    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10
                                    );