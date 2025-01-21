select * from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where ma_tb in ('hcm_ca_00073395',
'hcm_ca_00095463',
'hcm_ca_00095460',
'hcm_ca_00072884',
'hcm_ca_00039067',
'hcm_ca_00039209'
);

select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202403 and phieutt_id is not null and ht_tra_id = 2 and rownum = 1
union all
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202403  and ma_tb in ('hcm_ca_00073395',
'hcm_ca_00095463',
'hcm_ca_00095460',
'hcm_ca_00072884',
'hcm_ca_00039067',
'hcm_ca_00039209'
);
COMMIT;
update ttkd_bsc.ct_bsc_giahan_cntt a
set ht_tra_id = (select ht_tra_id_hp from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
ma_Gd =  (select ma_gd from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
phieutt_id =  (select phieutt_id from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
ma_chungtu =  (select ma_capnhat from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
ngay_nganhang = (select ngay_nganhang from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
ngay_tt = (select ngay_tt_hp from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
tien_khop = 0,
thuebao_KhAC_ID = (select thuebao_id_khAC from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id),
MA_TB_KHAC = (select MATB_KHAC from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thuebao_id = a.thuebao_id)
where thang = 202403  and ma_tb in ('hcm_ca_00073395',
'hcm_ca_00095463',
'hcm_ca_00095460',
'hcm_ca_00072884',
'hcm_ca_00039067',
'hcm_ca_00039209'
);
select thuebao_id, ngay_kt from css_hcm.db_cntt where thuebao_id in (10724301,11942666)
;
select ma_Gd, ma_chungtu, phieutt_id from ttkd_bsc.ct_bsc_giahan_cntt  where thang = 202403 and ht_tra_id = 2 and tien_khop = 0;
;
select distinct loai_tinh from ttkd_Bsc.ct_dongia_tratruoc where thang = 202403
select bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM, tien_nhapthem
    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
         where  phieu_id =8206534