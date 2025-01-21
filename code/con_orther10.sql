select* from css_hcm.hinhthuc_Tra where ht_tra_id in (2,7,5,207,214,204,4);

select* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_gd in ('HCM-TT/02811587','HCM-TT/02811514','HCM-TT/02811594','HCM-TT/02812248');
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb; where tien_tt < tien_ct;

select* from css_hcm.phieutt_hd where ma_Gd = 'HCM-TT/02766531';

select* from ttkd_Bsc.nhanvien_202406;
select b.hoantat,a.hoantat , a.chungtu_id, a.ma_Gd, b.ma_Ct, b.* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb a join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.chungtu_id = b.chungtu_id 
where  a.ma_gd in ('HCM-TT/02811587','HCM-TT/02811514','HCM-TT/02811594','HCM-TT/02812248');

select* from ttkd_Bsc.nhanvien_202405 where ten_nv like '%?n';

update ttkd_bsc.ct_bsc_tratruoc_moi a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_tratruoc_moi a
where  ht_tra_id in (2,4,5,214) and tien_khop = 0  and thang = 202405 
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.tien_ct between bb.tien_tt and bb.tien_tt - 10 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
        );
commit;
update ttkd_bsc.ct_bsc_tratruoc_moi a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_tratruoc_moi a
where thang = 202405 and ht_tra_id in (2,4,5,214) and tien_khop = 0 
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat a, aa.tien_sub_ct, cc.tien_hd
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join (select sum(tien_thanhtoan)+sum(vat_thanhtoan) tien_hd, ma_gd 
                               from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt 
                               where thang = 202405
                               group by ma_gd) cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >= cc.tien_hd - 10  and bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
        );
commit;
select distinct ht_Tra_id, ten_Ht_Tra from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202405;
update ttkd_bsc.ct_bsc_tratruoc_moi a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_tratruoc_moi a
where thang = 202405 and ht_tra_id in (2,4,5,214,207) and tien_khop = 0 
and exists 
(
    select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat a, aa.tien_sub_ct, (cc.tien+cc.vat) tien_hd
    from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                    join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                    join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
    where   aa.tien_sub_ct >= (cc.tien+cc.vat) - 10  and bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--                                    group by aa.chungtu_id 
    );
-------------------
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
where thang = 202405 and ht_tra_id in (2,4,5) and tien_khop = 0 
and exists 
(
    select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat a, aa.tien_sub_ct, (cc.tien+cc.vat) tien_hd
    from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                    join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                    join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
    where   aa.tien_sub_ct >= (cc.tien+cc.vat) - 10  and bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--                                    group by aa.chungtu_id 
    )
minus 
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
where thang = 202405 and ht_tra_id in (2,4,5) and tien_khop = 0 
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat a, aa.tien_sub_ct, cc.tien_hd
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join (select sum(tien_thanhtoan)+sum(vat_thanhtoan) tien_hd, ma_gd 
                               from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt 
                               where thang = 202405
                               group by ma_gd) cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >= cc.tien_hd - 10  and bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--                                    group by aa.chungtu_id 
        );
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405 and tien_khop = 0;
rollback;
select* from css_hcm.phieutt_hd;
HCM-TT/02818646
HCM-TT/02818379
HCM-TT/02818198
HCM-TT/02818199
HCM-TT/02781853;
select aa.* from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU8525983NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id;
select* from v_thongtinkm_all where thuebao_id = 11846631 and hieuluc = 1 and ttdc_id = 0 order by ngay_Bddc;