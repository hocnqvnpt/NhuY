select a.*, case when a.manv_ob is not null then 1 else 0 end co_ipcc from ttkd_Bsc.nhuY_ct_Bsc_ipcc_obghtt a where thang = 202405;
commit;
update ttkd_Bsc.ct_bsc_giahan_Cntt set tien_khop = 1 where thang =202405 and ma_gd in ('HCM-LD/01663140', )
select distinct a.* from ttkdhcm_ktghtt_chotngay_271 a where ngay_chot=to_date('20240602','yyyymmdd') and thang_kt in (202405,202406);

update ttkd_bsc.ct_bsc_giahan_Cntt a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_giahan_Cntt a
where  ht_tra_id in (2,4,5,214,207) and tien_khop = 0  and thang = 202405 
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.tien_ct between  bb.tien_tt - 10 and bb.tien_tt  and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
        );

select distinct a.* from ttkdhcm_ktnv.ghtt_chotngay_271 a where ngay_chot=to_date('20240602','yyyymmdd') and thang_kt in (202405,202406)  and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 )  
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) ) and tien_khop = 1
                   and ma_Tb in (select ma_tb from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202405 and tien_khop = 0)   ;
                   
update ttkd_bsc.ct_bsc_Trasau_tp_tratruoc a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_Trasau_tp_tratruoc a
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