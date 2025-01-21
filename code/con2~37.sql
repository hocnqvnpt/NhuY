select* from ttkdhcm_ktnv.ghtt_chotngay_271 where to_char(ngay_chot,'yyyymmdd') = '20240602' and ma_Tb in ('hcm_tmvn_00004525','hcm_tmvn_00000852','hcm_tmvn_00004524','hcm_tmvn_00004462');
update ttkd_Bsc.ct_dongia_Tratruoc set tien_khop = 1 , dongia = 0 where thang = 202405  and ma_Tb = 'hcm_ca_00078209';
commit;
select* from ttkd_Bsc.ct_dongia_tratruoc where ma_Tb in('mesh0065132','mesh0066902','hcmttkhue-htv3','mesh0065131') and thang >= 202404;
select* from  where ma_Tb in('mesh0065132','mesh0066902','hcmttkhue-htv3','mesh0065131') ;
8317898
8325739
8400360
8317922;
select* from ttkd_Bsc.nhanvien where ma_nv = 'CTV073592';
select* from ttkd_Bsc.nhanvien_
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405 and ma_tb in ('ngoctrang05',
'tuandv17',
'nghien91',
'mesh0065132',
'mesh0065131',
'quangthien1972',
'ndung0920',
'hcmndung0920',
'duydung72',
'hoacc3351606',
'tuantrung29',
'hcm_hoacc',
'hcmtrung29',
'ttkhue-htv3',
'hcmttkhue-htv3',
'mesh0066902',
'traneyelash',
'thoa-92021',
'vandung_ng',
'hcmvandung_ng',
'ynphuong93771348',
'thanhtung16823',
'hcmtung16823',
'nvm1494',
'pttth_33',
'hcmptth_33',
'tomanhcuong90',
'minhtuan0507',
'vdt959',
'kieuthao2021',
'ncv1_813',
'vanvu-1985') and ma_tb not in (
select ma_Tb from ttkd_Bsc.ct_dongia_tratruoc where loai_tinh !='DONGIATRA30D' and thang >= 202404 and ma_tb in ('ngoctrang05',
'tuandv17',
'nghien91',
'mesh0065132',
'mesh0065131',
'quangthien1972',
'ndung0920',
'hcmndung0920',
'duydung72',
'hoacc3351606',
'tuantrung29',
'hcm_hoacc',
'hcmtrung29',
'ttkhue-htv3',
'hcmttkhue-htv3',
'mesh0066902',
'traneyelash',
'thoa-92021',
'vandung_ng',
'hcmvandung_ng',
'ynphuong93771348',
'thanhtung16823',
'hcmtung16823',
'nvm1494',
'pttth_33',
'hcmptth_33',
'tomanhcuong90',
'minhtuan0507',
'vdt959',
'kieuthao2021',
'ncv1_813',
'vanvu-1985'));
update ttkd_Bsc.ct_bsc_trasau_tp_tratruoc a set TIEN_KHOP = 1
                -- select * from ttkd_Bsc.ct_bsc_trasau_tp_tratruoc a
                where thang = 202405 and a.rkm_id is not null and tien_khop = 0 -- and thuebao_id in (select thuebao_id from xsmax)
                                and ht_tra_id in (2,4,5)-- and a.phieutt_id = 7675246    
                               -- and ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')
                                and  exists 
                                (
                                    select bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                    where  phieu_id in (8317898,8325739,8400360,8317922) 
                                    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10
                                    );
                                    rollback;
                                 commit;
                                update ttkd_Bsc.ct_bsc_trasau_Tp_tratruoc set tien_khop = 1 where thang = 202404 and rkm_id is not null and tien_khop is null
                ;