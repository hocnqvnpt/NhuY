drop table ct_ghtt_t11;
drop table ct_ghtt_t12;
SELECT* FROM ct_thang12;
create table ct_ghtt_t12 as
select a.ma_Gd, b.tien, b.vat, d.ma_Ct, cast(null as number) tien_khop 
from ct_thang12_v2 a
    join css_hcm.phieutt_Hd b on a.ma_gd = b.ma_gd
    join ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb c on a.ma_gd = c.ma_Gd
    join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb d on c.chungtu_id = d.chungtu_id;
update ct_ghtt_t12 a set tien_khop = 1 where ma_Gd in (Select ma from ttkdhcm_ktnv.ds_chungtu_Nganhang_phieutt_Hd_1);
commit;
select* from ct_ghtt_t12;
               --1. phieu cha hoan tat
update ct_ghtt_t11 a set tien_khop = 1
-- select* from ct_ghtt_t11 a
where  --ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202409 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
 exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct, bb.ngay_Ht
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
    select* from thang10_ct;
-- phieu con co tien bang
update ct_ghtt_t11 a set tien_khop = 1
-- select* from ct_ghtt_t11 a
where --  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202409 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
 exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- bang cha du tien
update ct_ghtt_t11 a set tien_khop = 1
-- select* from ct_ghtt_t11 a
where  -- ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202409 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
 exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                         join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--        and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
--                                    group by aa.chungtu_id 
    );
commit;
select* from ct_ghtt_t12 ;