drop table hocnq_ttkd.a_ct_temp purge;
create table a_ct_temp as
        select *
     from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a
            where hoantat = 0
            and tratruoc  in (1,2,3)
            and tien_tt is null
            and not exists (select 1  from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb b,ttkdhcm_ktnv.ghtt_ds_phieutt d
                where b.chungtu_id=a.chungtu_id and b.ma_gd = d.ma_gd
                and d.loaihd_id in (1,3,6,8,17,31,41));
select* from a_ct_temp 