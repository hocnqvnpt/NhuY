-----giao ghtt thang_kt = 202206
/*select * from a_sdd;
create table a_sdd as
        select 202205 thang from dual;
DECLARE
vthang_tmp number := 202206;-- to_char(ADD_MONTHS(to_date(202206,'yyyyMM'),-3),'yyyyMM');
begin
        insert into a_sdd
        select vthang_tmp from dual;
        commit;
end
;
*/
update ds_giahan_tratruoc2 dc set sl_tratruoc = (select count(thang_tru_cuoi) sl_datcoc from v_thongtinkm_all 
                                                    where thang_tru_cuoi >= thang_bddc and cuoc_dc > 0 and thangdc >=3 and dc.thuebao_id = thuebao_id)
                                                    ;
select * from ds_giahan_tratruoc2 ;
rename ds_giahan_tratruoc2 to ds_giahan_tratruoc2_202401;
--drop table ds_giahan_tratruoc2 purge;                 thang n
-- drop table ds_giahan_tratruoc2_tmp purge;                 thang n+1
create table ds_giahan_tratruoc2 as
--    insert into ds_giahan_tratruoc2
select dc.*, ts.sl_datcoc, pkg.nhomtb_id, pkg.goi_id, db.trangthaitb_id, db.khachhang_id--, pttt.ht_tra_id, pttt.kenhthu_id
            from (
                    ----------------TT Thang tang tren 1 dong-------------
                    select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc, least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_ktdc
                                    , least(km.ngay_kt, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_kt_mg, km.ngay_huy, km.NGAY_THOAI
                                    , km.tien_td, km.cuoc_dc
                                    , km.thangdc + km.thangkm so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km 
                    where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))  >= ngay_bddc
                                    and km.loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 )
                                    and km.thang_ktdc > 202310
                                --    and km.thuebao_id = 9085442
                   union all
    --                                                                 -------KM 100, nhung khong co datcoc              
    --                                                                               select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, to_date(km.thang_bd_mg, 'yyyymm') ngay_bd
    --                                                                                                , LAST_DAY(to_date(km.thang_kt_mg, 'yyyymm')) ngay_ktdc
    --                                                                                                , LAST_DAY(to_date(km.thang_kt_mg, 'yyyymm')) ngay_kt_mg, km.ngay_huy, km.NGAY_THOAI                                                                                               
    --                                                                                                , km.tien_td, km.cuoc_dc
    --                                                                                                , km.thangdc + km.thangkm so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
    --                                                                                from v_thongtinkm_all_ol km 
    --                                                                                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc =0
    --                                                                                                and hieuluc = 1 and ttdc_id = 0
    --                                                                                                and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
    --                                                                                                and km.thang_kt_mg > 202307
    --                                                                                                and km.thuebao_id = 8131788
    --                                                                               union all
    ----------------TT giam cuoc or thang tang tren 2 dong-------------
                    select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc,  least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_ktdc
                                    , case when km1.ngay_kt_mg is not null then km1.ngay_kt_mg else least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) end ngay_kt_mg
                                    , km.ngay_huy, km.NGAY_THOAI, km.tien_td, km.cuoc_dc
                                    , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km left join (select thuebao_id, thang_bd_mg, LAST_DAY(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg, rkm_id, thangkm
                                                                                                        from v_thongtinkm_all_ol
                                                                                                        where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                                                                                        and (tyle_sd = 100 or tyle_tb = 100) and thang_bd_mg > 202310
                                                                                                    ) km1 on km1.thuebao_id = km.thuebao_id and km.ngay_ktdc + 1 =  to_date(km1.thang_bd_mg, 'yyyymm')
                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc
                                    and km.loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 )
                                    and km.thang_ktdc > 202310
                                 --   and km.thuebao_id = 8131788
                    
                     union all
    ----------------TT giam cuoc or thang tang tren 2 dong-------------
                    select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, trunc(to_date(km.thang_bddc, 'yyyymm')) ngay_bddc
                                    , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) thang_ktdc
                                    , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) thang_kt_mg
                                    , last_day(to_date(km.thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(km.thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai, km.tien_td, km.cuoc_dc
                                    , km.thangdc so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km 
                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0
                                    and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                    and km.loaitb_id not  in (55 ,80 ,116 ,117,132,140,154,181,288,318 )
                                    and km.thang_ktdc > 202310
                              --  and km.thuebao_id = 9085442
                ) dc
                left join (select nhomtb_id, goi_id, thuebao_id from css.v_bd_goi_dadv@dataguard where trangthai = 1 and dichvuvt_id = 4
                                            and goi_id not between 1715 and 1726 and goi_id not in (15414) and goi_id < 100000
                                    ) pkg on dc.thuebao_id = pkg.thuebao_id
                left join (select thuebao_id, count(thang_tru_cuoi) sl_datcoc from v_thongtinkm_all_ol 
                                            where thang_tru_cuoi >= thang_bddc+2 and cuoc_dc > 0 and thangdc >=3 group by thuebao_id) ts on dc.thuebao_id = ts.thuebao_id
                left join css.v_db_thuebao@dataguard db on dc.thuebao_id = db.thuebao_id and db.trangthaitb_id not in (7,8,9)
       --         left join css.v_hd_thuebao@dataguard hdtb on dc.hdtb_id = hdtb.hdtb_id
       --         left join css.v_phieutt_hd@dataguard pttt on hdtb.hdkh_id = pttt.hdkh_id and pttt.ht_tra_id <> 6 and pttt.trangthai = 1 and kenhthu_id <> 6 and pttt.tien > 0

        where  to_number(to_char(ngay_kt_mg, 'yyyymm')) >= 202312 ----thang n+1, n thang chay du lieu
           --             and db.ma_tb in ('dtcattuong', 'minhtri-f26')
                 
;

select RKM_ID||',', thuebao_id, ma_tb from hocnq_ttkd.ds_giahan_tratruoc2 group by  RKM_ID, thuebao_id, ma_tb having count(*)>1;


drop index IDX;
drop index IDX2;
drop index IDX3;
CREATE INDEX IDX ON ds_giahan_tratruoc2 (thuebao_id);
CREATE INDEX IDX2 ON ds_giahan_tratruoc2 (khachhang_id, ngay_kt_mg);
CREATE INDEX IDX3 ON ds_giahan_tratruoc2 (thuebao_id, ngay_kt_mg);
--    CREATE INDEX IDX3 ON ds_giahan_tratruoc2 (thuebao_id, thang_bddc ASC);


delete from hocnq_ttkd.ds_giahan_tratruoc2 where rowid = 'AADJNJAAQAACurFAAo';ma_tb in ('dtcattuong', 'minhtri-f26');
;
select * from tinhcuoc_hcm.dbkh_20220401; where ma_tb = 'nquanghoc_2015';thuebao_id = '8139916';
select * from css_hcm.db_khachhang;
select thuebao_id, madoicap from css_hcm.db_adsl where matb_tn = 'nquanghoc_2015';
select rowid, a.* from ds_giahan_tratruoc2 a where ma_tb in ('hcm_kimkhai');

rollback;
commit;

rename ds_giahan_tratruoc1 to ds_giahan_tratruoc1_08;

drop table ds_giahan_tratruoc purge;
select * from ds_giahan_tratruoc;

drop table ds_giahan_tratruoc1 purge; 

create table ds_giahan_tratruoc1 as
select --count(*) a
        --dc.chitietkm_id, tt.thanhtoan_id, dc.thuebao_id
          to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) thang, dc.RKM_ID, dc.THUEBAO_ID, dc.MA_TB, dc.LOAITB_ID, dc.ngay_BDDC, dc.ngay_KTDC, dc.ngay_KT_MG, dc.ngay_HUY, dc.ngay_thoai, dc.TIEN_TD
          , dc.CUOC_DC, dc.SO_THANGDC, dc.SO_THANGkm, dc.congvan_id, dc.KHUYENMAI_ID, dc.CHITIETKM_ID, dc.nhom_datcoc_id, dc.SL_DATCOC, dc.NHOMTB_ID, dc.GOI_ID
          , tt.tuyenthu_id, nv_tuyenthu.nvtc_id, nv_tuyenthu.nvqltc_id nvnvqltc_id, dmdc.quan_id, dmdc.phuong_id, ks.nhanvien_id nvttvt_id, ks.tovt tovt_id, ks.ttvt ttvt_id
         , khc.diemtinnhiem
  --   , ts.sl_datcoc    , pkg.nhomtb_id, pkg.goi_id
                 ----check ds tbao chung goi da dich vu
         , (select listagg((select ma_tb from css_hcm.db_thuebao where thuebao_id = pkg1.thuebao_id), '; ') within group (order by pkg1.thuebao_id)
                from css_hcm.bd_goi_dadv pkg1 where pkg1.trangthai = 1 and pkg1.nhomtb_id = dc.nhomtb_id-- group by pkg1.nhomtb_id
                ) matb_phu
                ----check Fiber va KH CN
       , case when lkh.khdn = 0 and a.loaitb_id in (58, 59) then 1 else 0 end fiber_donle
                ---Check du an
            -- , nvl((select distinct 1 from dattdt.duan_thuebao_mapping where thuebao_id = a.thuebao_id), 0) duan   ---bo ngay 25/7/22 dk duan_id=345 and 
            , (select duan_id from css_hcm.toanha xz where xz.toanha_id = a1.toanha_id) duan_id
                ---Check KH dat biet
            , nvl((select 1 from ttkdhcm_ktnv.Khdb_Ds_Kh_Dacbiet 
                                                            where thuebao_id = a.thuebao_id and ((f_trangthai=2 and f_duyet in (2, 3)) or (f_trangthai=3 and f_duyet=3))), 0)
                                     kh_db ---check kh dac biet anh Cong Son
            , a1.tocdo_id, a1.madoicap, a.khachhang_id, lkh.khdn, a.thanhtoan_id, a.trangthaitb_id
            , dbkh.so_dt sdt_lh, dbkh.email, dbkh.so_dt sdt_bh, a2.ma_dt_kh, a2.ma_nv, a2.tbh_ql_id, a2.pbh_ql_id
            , a.ghichu, sysdate ngay_tao
            , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                    when a.loaitb_id in (58, 59) then 1  
                                    ------Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                    when a.loaitb_id in (210, 222, 224) then 0.5 - nvl(0.3* (select distinct 1 from ds_giahan_tratruoc2 ax
                                                                                                                                            where ax.trangthaitb_id is not null
                                                                                                                                                            and ax.loaitb_id in (58, 59)
                                                                                                                                                            and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id and ngay_kt_mg > ax.ngay_kt_mg)
                                                                                                                                            and ax.khachhang_id = dc.khachhang_id and dc.ngay_kt_mg = ax.ngay_kt_mg), 0)
                                   ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                    when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ds_giahan_tratruoc2 ax
                                                                                                                                            where ax.trangthaitb_id is not null
                                                                                                                                                            and ax.loaitb_id in (58, 59)
                                                                                                                                                            and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id and ngay_kt_mg > ax.ngay_kt_mg)
                                                                                                                                            and ax.khachhang_id = dc.khachhang_id and dc.ngay_kt_mg = ax.ngay_kt_mg), 0)
                                else 0 
                    end Heso
                    , decode(dc.congvan_id, 190, 0, (select count(thuebao_id) from ds_giahan_tratruoc2 ax
                                                                                            where ax.trangthaitb_id is not null and congvan_id  <> 190 
                                                                                                            and ax.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224)
                                                                                                            and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id and ngay_kt_mg > ax.ngay_kt_mg)
                                                                                                            and ax.khachhang_id = dc.khachhang_id and ax.ngay_kt_mg = dc.ngay_kt_mg)) uutien_giao
                    , cast(1 as number(2)) tratruoc 
                    -- dc.*, pkg.*--chitietkm_id, dc.thuebao_id
-- select * 
from css_hcm.db_thuebao a
            join css_hcm.db_adsl a1 on a.thuebao_id = a1.thuebao_id and a1.chuquan_id = 145
            join ttkd_bct.db_thuebao_ttkd a2 on a.thuebao_id = a2.thuebao_id --and a.thanhtoan_id = a2.thanhtoan_id
            left join css_hcm.db_khachhang dbkh on a.khachhang_id = dbkh.khachhang_id
           join ds_giahan_tratruoc2 dc on a.thuebao_id = dc.thuebao_id                   --thang n
          --  join ds_giahan_tratruoc2_tmp dc on a.thuebao_id = dc.thuebao_id  ---thang n+1
                    
                        join css_hcm.db_thanhtoan tt on a.thanhtoan_id = tt.thanhtoan_id
                        left join css_hcm.tuyenthu tut on tt.tuyenthu_id = tut.tuyenthu_id
                        left join (select  nddl.daily_id nvtc_id, nd.nhanvien_id nvqltc_id, nd.nhom_nd_id, nd.ma_nd user_nvqltc, nv.donvi_id, dv.ten_dv
                                                    , (select ma_nd  from admin_hcm.nguoidung a,admin_hcm.nhanvien b 
                                                            where a.nhom_nd_id=225 and SUBSTR(ma_nd,4,5)='sbhtt' and a.nhanvien_id=b.nhanvien_id and b.donvi_id = nv.donvi_id
                                                        ) user_nvqltt
                                       from admin_hcm.nguoidung_dl nddl, admin_hcm.nguoidung nd, admin_hcm.nhanvien nv, admin_hcm.donvi dv
                                       where nddl.nguoidung_id = nd.nguoidung_id and nd.nhanvien_id = nv.nhanvien_id and nv.donvi_id = dv.donvi_id
                                                        and nd.nhom_nd_id in (53,59) and nd.trangthai=1  and (substr(nd.ma_nd,3,2)='s_' or substr(nd.ma_nd,3,4)='_sbh')
                                                         and nddl.daily_id not in (91228, 91260)   ---loai tru dai ly chua dc SBH phan tuyen
                                    ) nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                        left join css_hcm.diachi_tb dcld on a.thuebao_id = dcld.thuebao_id
                        left join css_hcm.diachi dmdc on dcld.diachild_id = dmdc.diachi_id
                        left join kiemsoat.dbtb_20230901 ks on a.thuebao_id = ks.thuebao_id                                             -----change
                        left join tinhcuoc_hcm.dbkh partition for (20230901) khc on a.khachhang_id = khc.khachhang_id                   ----change
                        left join css_hcm.loai_kh lkh on khc.loaikh_id = lkh.loaikh_id
   
    where a.trangthaitb_id not in (7, 8, 9) and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) = 202311 --and a.thuebao_id  in( 9049183)
                        and a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) --and pkg.nhomtb_id = '334800'
                        and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = dc.thuebao_id and ngay_kt_mg > dc.ngay_kt_mg)
              ;         
            
  ;
  rollback;
  commit;
  select * from ds_giahan_tratruoc1;
  rollback;
  
  ---update fiber_donle theo MATB_TN, MADOICAP cua MYTV
   update ds_giahan_tratruoc1 a set fiber_donle = 0
            --- select * from ds_giahan_tratruoc1 a
            where fiber_donle = 1-- and thang = 202307
                            and exists (select 1 from css.v_db_thuebao@dataguard xj, css.v_db_adsl@dataguard xk 
                                                                                    where xj.thuebao_id = xk.thuebao_id and xj.loaitb_id in (61, 171, 210, 222, 224)
                                                                                                        and xk.matb_tn = a.ma_tb )
                           ;
                   update ds_giahan_tratruoc1 a set fiber_donle = 0
                            --- select * from ds_giahan_tratruoc1 a
                            where fiber_donle = 1 
                                            and exists (select 1 from css.v_db_thuebao@dataguard xj
                                                                                                                            where xj.loaitb_id in (61, 171, 210, 222, 224)
                                                                                                                                                and xj.khachhang_id = a.khachhang_id)
                                                    
                        ;
--update so dien thoai lien he bsung
update ds_giahan_tratruoc1 hs
       set hs.sdt_lh = (select lh.dienthoai
                                            from css.v_db_lienhe@dataguard lh, css.v_db_lienhe_md@dataguard md
                                                        , (select * from css.v_db_ttlh@dataguard a 
                                                            where ttlh_id = (select max(ttlh_id) from css.v_db_ttlh@dataguard where db_id = a.db_id and ttkh_id = a.ttkh_id group by ttkh_id, db_id)) b
                                            where lh.ttlh_id = md.ttlh_id and md.mucdich_id in (1, 5, 29, 30) and lh.dienthoai is not null and rownum = 1
                                                            and lh.ttlh_id = b.ttlh_id and b.ttkh_id = 1 and b.db_id = hs.khachhang_id)
where hs.sdt_lh is null
;
--update so dien thoai bao hu
update ds_giahan_tratruoc1 a
set ghichu = null
            , sdt_bh = (select dienthoai_lh from (
                                    select thuebao_id, dienthoai_lh, rank() over(partition by thuebao_id order by baohong_id desc) rnk from baohong_hcm.baohong
                                      ) xj where rnk = 1 and xj.thuebao_id = a.thuebao_id
                    )
;
----update KHDN is null
update ds_giahan_tratruoc1 a
                set khdn = (select khdn from css_hcm.db_khachhang kh 
                                                                                    join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id and a.khachhang_id = kh.khachhang_id 
                                                                    )
        where khdn is null
;
-----Update ds tra truoc
 update ds_giahan_tratruoc1 a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc1 a
where not exists (select congvan_id from ds_giahan_tratruoc1 where congvan_id not in (190, 343, 483, 491, 545, 8922) and a.thuebao_id = thuebao_id)
;
----Update ds tra truoc
update ds_giahan_tratruoc1 a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc1 a
where so_thangdc < 3 and tratruoc = 1
;
delete from ds_giahan_tratruoc1 where thuebao_id in (select thuebao_id from ds_giahan_tratruoc);
delete from ds_giahan_tratruoc1 where thuebao_id in (select thuebao_id from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202310 and tratruoc = 1);
commit;
rollback;
select * from ttkd_bct.moi_giahan_tratruoc_moi where thang_kt = 202308 and tratruoc = 1 ;
select * from ds_giahan_tratruoc1;

select  thuebao_id, chitietkm_id from ds_giahan_tratruoc1 group by thuebao_id, chitietkm_id having count(*)>1
;
----------File moi T+1
/*kenh thu cu
 with 
                  pthd_old (select a.thuebao_id, a.ma_tb, a.rkm_id, km.hdtb_id km_hdtb_id, hddc.hdtb_id hddc_hdtb_id, hddc.thuebao_dc_id
                                                        , pttt.ht_tra_id, pttt.kenhthu_id, hdtb.hdkh_id
                                        from ds_giahan_tratruoc2 a
                                                    left join css_hcm.khuyenmai_dbtb km on a.rkm_id = km.rkm_id
                                                    left join css_hcm.db_datcoc db on a.rkm_id = db.rkm_id
                                                    left join css_hcm.hdtb_datcoc hddc on db.thuebao_dc_id = hddc.thuebao_dc_id
                                                    left join css_hcm.hd_thuebao hdtb on nvl(km.hdtb_id, hddc.hdtb_id) = hdtb.hdtb_id
                                                    left join css_hcm.phieutt_hd pttt on hdtb.hdkh_id = pttt.hdkh_id and pttt.ht_tra_id <> 6
                                    )
*/
select * from table(dbms_xplan.display);
drop table ds_giahan_tratruoc purge;
rename ds_giahan_tratruoc to ds_giahan_tratruoc_10;
--  explain plan for
create table ds_giahan_tratruoc as
with kv as (
                        select THUEBAO_ID, knv.NHANVIEN_ID, kvtb.khuvuc_id, ma_kv, nv.donvi_id tovt_id, dv.donvi_cha_id ttvt_id, dv.ten_dv TEN_TO, dv.ten_dvql ten_TTVT
               from css_hcm.dbtb_kv kvtb
                                    join css_hcm.khuvuc kv on kvtb.khuvuc_id = kv.khuvuc_id and kvtb.loaikv_id = 4
                                    left join css_hcm.khuvuc_nv knv on kv.khuvuc_id = knv.khuvuc_id and knv.loaikv_id = kvtb.loaikv_id and knv.loainv_id=51 and knv.nhiemvu=1
                                    left join admin_hcm.nhanvien nv on knv.nhanvien_id = nv.nhanvien_id
                                    left join admin_hcm.donvi dv on nv.donvi_id = dv.donvi_id     
                    )
        , nv_tuyenthu as (select  nddl.daily_id nvtc_id, nd.nhanvien_id nvqltc_id, nd.nhom_nd_id, nd.ma_nd user_nvqltc, nv.donvi_id, dv.ten_dv
                                                    , (select ma_nd  from admin_hcm.nguoidung a,admin_hcm.nhanvien b 
                                                            where a.nhom_nd_id=225 and SUBSTR(ma_nd,4,5)='sbhtt' and a.nhanvien_id=b.nhanvien_id and b.donvi_id = nv.donvi_id
                                                        ) user_nvqltt
                                       from admin_hcm.nguoidung_dl nddl, admin_hcm.nguoidung nd, admin_hcm.nhanvien nv, admin_hcm.donvi dv
                                       where nddl.nguoidung_id = nd.nguoidung_id and nd.nhanvien_id = nv.nhanvien_id and nv.donvi_id = dv.donvi_id
                                                        and nd.nhom_nd_id in (53,59) and nd.trangthai=1  and (substr(nd.ma_nd,3,2)='s_' or substr(nd.ma_nd,3,4)='_sbh')
                                                         and nddl.daily_id not in (91228, 91260)   ---loai tru dai ly chua dc SBH phan tuyen
                                    ) 

select --count(*) a
          to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) thang, dc.RKM_ID, dc.THUEBAO_ID, dc.MA_TB, dc.LOAITB_ID, dc.ngay_BDDC, dc.ngay_KTDC, dc.ngay_KT_MG, dc.ngay_HUY, dc.ngay_thoai, dc.TIEN_TD
          , dc.CUOC_DC, dc.SO_THANGDC, dc.so_thangkm, dc.congvan_id, dc.KHUYENMAI_ID, dc.CHITIETKM_ID, dc.SL_DATCOC, dc.NHOMTB_ID, dc.GOI_ID, dc.ht_tra_id, dc.kenhthu_id
             ----check ds tbao chung goi da dich vu
         , (select listagg((select ma_tb from css_hcm.db_thuebao where thuebao_id = pkg1.thuebao_id), '; ') within group (order by pkg1.thuebao_id)
                from css_hcm.bd_goi_dadv pkg1 where pkg1.trangthai = 1 and pkg1.nhomtb_id = dc.nhomtb_id-- group by pkg1.nhomtb_id
                ) matb_phu
---part 1----
          , tt.tuyenthu_id, nv_tuyenthu.nvtc_id, nv_tuyenthu.nvqltc_id nvnvqltc_id
          , dmdc.quan_id, dmdc.phuong_id
          , kv.khuvuc_id, kv.nhanvien_id nvttvt_id, kv.tovt_id, kv.ttvt_id
              
                ----check Fiber va KH CN
       , case when lkh.khdn = 0 and a.loaitb_id in (58, 59) then 1 else 0 end fiber_donle
       , khc.diemtinnhiem, lkh.khdn
          ---Check KH dat biet
            , nvl((select 1 from ttkdhcm_ktnv.Khdb_Ds_Kh_Dacbiet 
                                                            where thuebao_id = a.thuebao_id and ((f_trangthai=2 and f_duyet in (2, 3)) or (f_trangthai=3 and f_duyet=3))), 0)
                                     kh_db ---check kh dac biet anh Cong Son
             , dbkh.so_dt sdt_lh, dbkh.email, dbkh.so_dt sdt_bh
 
---End part 1---      
---Check du an
            , (select duan_id from css_hcm.toanha xz where xz.toanha_id = a1.toanha_id) duan_id                         
            , a1.tocdo_id, a1.madoicap, a.khachhang_id, a.thanhtoan_id, a.trangthaitb_id
          , a2.ma_dt_kh, a2.ma_nv, a2.tbh_ql_id, a2.pbh_ql_id
            , a.ghichu, sysdate ngay_tao
            , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                    when a.loaitb_id in (58, 59) then 1  
                                    ------Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                    when a.loaitb_id in (210, 222, 224) then 0.5 - nvl(0.3* (select distinct 1 from hocnq_ttkd.ds_giahan_tratruoc2 ax
                                                                                                                                            where ax.loaitb_id in (58, 59)
                                                                                                                                                            and not exists (select 1 from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id 
                                                                                                                                                                                                and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                                                                                            and ax.khachhang_id = dc.khachhang_id 
                                                                                                                                            and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm'))), 0)
                                   ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                    when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from hocnq_ttkd.ds_giahan_tratruoc2 ax
                                                                                                                                            where ax.loaitb_id in (58, 59)
                                                                                                                                                            and not exists (select 1 from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id 
                                                                                                                                                                                                and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                                                                                            and ax.khachhang_id = dc.khachhang_id
                                                                                                                                            and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm'))), 0)
                                else 0 
                    end Heso
                    , decode(dc.congvan_id, 190, 0, (select count(thuebao_id) from hocnq_ttkd.ds_giahan_tratruoc2 ax
                                                                                            where congvan_id  <> 190 
                                                                                                            and ax.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                                                                                                            and not exists (select 1 from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id 
                                                                                                                                                            and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                                                            and ax.khachhang_id = dc.khachhang_id 
                                                                                                            and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                                                            ) uutien_giao
                    , cast (1 as number (2)) tratruoc
from css_hcm.db_thuebao a --on a.thuebao_id = dc.thuebao_id                   --thang n
            join css_hcm.db_adsl a1 on a.thuebao_id = a1.thuebao_id and a1.chuquan_id = 145
            join ttkd_bct.db_thuebao_ttkd a2 on a.thuebao_id = a2.thuebao_id 
    --part 1----        
    join css_hcm.db_khachhang dbkh on a.khachhang_id = dbkh.khachhang_id
    join hocnq_ttkd.ds_giahan_tratruoc2 dc on a.thuebao_id = dc.thuebao_id                   --thang n
             
                                join css_hcm.db_thanhtoan tt on a.thanhtoan_id = tt.thanhtoan_id
                               left join css_hcm.tuyenthu tut on tt.tuyenthu_id = tut.tuyenthu_id
                    --            left join hocnq_ttkd.nv_tuyenthu nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                                left join nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                    
                    left join css_hcm.diachi_tb dcld on a.thuebao_id = dcld.thuebao_id
                    left join css_hcm.diachi dmdc on dcld.diachild_id = dmdc.diachi_id
                    left join kv on a.thuebao_id = kv.thuebao_id                                             -----change
                    left join tinhcuoc_hcm.dbkh partition for (20231001) khc on a.khachhang_id = khc.khachhang_id                   ----change
                    left join css_hcm.loai_kh lkh on lkh.loaikh_id = khc.loaikh_id
    ---end part 1----                
                  
    where  a.trangthaitb_id not in (7, 8, 9) and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) = 202401
                        and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) 
                       and not exists (select 1 from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id = dc.thuebao_id and ngay_kt_mg > dc.ngay_kt_mg)
                     
  ;
  
  ---update fiber_donle theo MATB_TN, MADOICAP cua MYTV
   update ds_giahan_tratruoc a set fiber_donle = 0
            --- select * from ds_giahan_tratruoc a
            where fiber_donle = 1
                            and exists (select 1 from css.v_db_thuebao@dataguard xj, css.v_db_adsl@dataguard xk 
                                                                                    where xj.thuebao_id = xk.thuebao_id and xj.loaitb_id in (61, 171, 210, 222, 224)
                                                                                                        and xk.matb_tn = a.ma_tb)
                           ;
                   update ds_giahan_tratruoc a set fiber_donle = 0
                            --- select * from ds_giahan_tratruoc a
                            where fiber_donle = 1
                                            and exists (select 1 from css.v_db_thuebao@dataguard xj
                                                                                                                            where xj.loaitb_id in (61, 171, 210, 222, 224)
                                                                                                                                                and xj.khachhang_id = a.khachhang_id)
                                                    
                        ;
--update so dien thoai lien he bsung
update ds_giahan_tratruoc hs
set hs.sdt_lh = (select lh.dienthoai
                                            from css.v_db_lienhe@dataguard lh, css.v_db_lienhe_md@dataguard md
                                                        , (select * from css.v_db_ttlh@dataguard a 
                                                            where ttlh_id = (select max(ttlh_id) from css.v_db_ttlh@dataguard where db_id = a.db_id and ttkh_id = a.ttkh_id group by ttkh_id, db_id)) b
                                            where lh.ttlh_id = md.ttlh_id and md.mucdich_id in (1, 5, 29, 30) and lh.dienthoai is not null and rownum = 1
                                                            and lh.ttlh_id = b.ttlh_id and b.ttkh_id = 1 and b.db_id = hs.khachhang_id)
where hs.sdt_lh is null
;
--update so dien thoai bao hu
update ds_giahan_tratruoc a
set ghichu = null
            , sdt_bh = (select dienthoai_lh from (
                                    select thuebao_id, dienthoai_lh, rank() over(partition by thuebao_id order by baohong_id desc) rnk from baohong_hcm.baohong
                                      ) xj where rnk = 1 and xj.thuebao_id = a.thuebao_id
                    )
;
----update KHDN is null
update ds_giahan_tratruoc a
                set khdn = (select khdn from css_hcm.db_khachhang kh 
                                                                                    join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id and a.khachhang_id = kh.khachhang_id 
                                                                    )
        where khdn is null
;
-----Update ds tra truoc
update ds_giahan_tratruoc a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc a
where not exists (select congvan_id from ds_giahan_tratruoc where congvan_id not in (190, 343, 483, 491, 545, 8922) and a.thuebao_id = thuebao_id)
;
----Update ds tra truoc
update ds_giahan_tratruoc a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc a
where so_thangdc < 3 and tratruoc = 1
;
commit;
rollback;
select * from ds_giahan_tratruoc where ma_tb in ('dtcattuong', 'minhtri-f26');

select  thuebao_id, chitietkm_id from ds_giahan_tratruoc group by thuebao_id, chitietkm_id having count(*)>1
;

select * delete from ds_giahan_tratruoc where thuebao_id in (select thuebao_id from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202312 and tratruoc = 1) and tratruoc = 1

;
          




