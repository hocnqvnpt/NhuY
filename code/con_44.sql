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
create table kehoach_2024 as 
alter table kehoach_2024 add tratruoc number--,  number
update kehoach_2024 set fiber_donle = 1 where loaitb_id in (58,59)
update kehoach_2024 set tratruoc = 1 --where loaitb_id in (58,59)

select* from hocnq_ttkd.kehoach_2024 where to_char(ngay_kt_mg,'yyyy') = '2024'
select * from css_hcm.db_cntt where ngAY_DUYTRI IS NOT NULL
update kehoach_2024 dc set sl_tratruoc = (select count(thang_tru_cuoi) sl_datcoc from v_thongtinkm_all 
                                                    where thang_tru_cuoi >= thang_bddc and cuoc_dc > 0 and thangdc >=3 and dc.thuebao_id = thuebao_id)
                                                    ;
drop table kehoach_2024 ;
rename kehoach_2024 to kehoach_2024_202401;
--drop table kehoach_2024 purge;                 thang n
-- drop table kehoach_2024_tmp purge;                 thang n+1
select* from ttkdhcml;
create table kehoach_2024 as
--    insert into kehoach_2024
    with dc as (
    ----------------TT Thang tang tren 1 dong-------------
                     select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc, least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_ktdc
                                    , least(km.ngay_kt, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_kt_mg, km.ngay_huy, km.NGAY_THOAI
                                    , km.tien_td, km.cuoc_dc
                                    , km.thangdc + km.thangkm so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km 
                    where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))  >= ngay_bddc
                                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
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
                                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)    
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
                                    and km.loaitb_id not  in (18, 58, 59, 61, 171, 210, 222, 224)
                                    and km.thang_ktdc > 202310
                                  --  and km.thuebao_id = 9085442
            ) select* from dc where rkm_id in (5747847,6490666,5942183,5836941,6301975,5803981,5201083  ) , 
            pkg as (select nhomtb_id, goi_id, thuebao_id from css.v_bd_goi_dadv@dataguard where trangthai = 1 and dichvuvt_id = 4
                                                                            and goi_id not between 1715 and 1726 and goi_id not in (15414) and goi_id < 100000
            ),
            ts as (select thuebao_id, count(thang_tru_cuoi) sl_datcoc from v_thongtinkm_all_ol 
                                        where thang_tru_cuoi >= thang_bddc+2 and cuoc_dc > 0 and thangdc >=3 group by thuebao_id)
            select dc.*, ts.sl_datcoc, pkg.nhomtb_id, pkg.goi_id, db.trangthaitb_id, db.khachhang_id--, pttt.ht_tra_id, pttt.kenhthu_id
            from  dc
            left join  pkg on dc.thuebao_id = pkg.thuebao_id
            left join  ts on dc.thuebao_id = ts.thuebao_id
            left join css.v_db_thuebao@dataguard db on dc.thuebao_id = db.thuebao_id and db.trangthaitb_id not in (7,8,9)
            join css_Hcm.hd_thuebao a on dc.thuebao_id 
   --         left join css.v_hd_thuebao@dataguard hdtb on dc.hdtb_id = hdtb.hdtb_id
   --         left join css.v_phieutt_hd@dataguard pttt on hdtb.hdkh_id = pttt.hdkh_id and pttt.ht_tra_id <> 6 and pttt.trangthai = 1 and kenhthu_id <> 6 and pttt.tien > 0

where  to_number(to_char(ngay_kt_mg, 'yyyymm')) >= 202312 ----thang n+1, n thang chay du lieu
   --             and db.ma_tb in ('dtcattuong', 'minhtri-f26')
         
;5747847	8979370
5942183	4406993
5836941	9741424
select* from v_Thongtinkm_all_ol where rkm_id = 5747847
select rowid , rkm_id from ds_giahan_Tratruoc where rkm_id in (5747847,5942183,5836941);
delete from kehoach_2024 where rowid in ('AAEb4TABgAAAV3YAAJ','AAEb4TABdAAALI5AAH','AAEb4TABaAAAR6jAAO');
commit;
select RKM_ID||',', thuebao_id, ma_tb from hocnq_ttkd.kehoach_2024 group by  RKM_ID, thuebao_id, ma_tb having count(*)>1;


drop index IDX;
drop index IDX2;
drop index IDX3;
CREATE INDEX IDX ON kehoach_2024 (thuebao_id);
CREATE INDEX IDX2 ON kehoach_2024 (khachhang_id, ngay_kt_mg);
CREATE INDEX IDX3 ON kehoach_2024 (thuebao_id, ngay_kt_mg);
--    CREATE INDEX IDX3 ON kehoach_2024 (thuebao_id, thang_bddc ASC);
commit;

select* from kehoach_2024-- where rowid = 'AADJNJAAQAACurFAAo';ma_tb in ('dtcattuong', 'minhtri-f26');
;
select * from tinhcuoc_hcm.dbkh_20220401; where ma_tb = 'nquanghoc_2015';thuebao_id = '8139916';
select * from css_hcm.db_khachhang;
select thuebao_id, madoicap from css_hcm.db_adsl where matb_tn = 'nquanghoc_2015';
select rowid, a.* from kehoach_2024 a where ma_tb in ('hcm_kimkhai');

rollback;
commit;

rename ds_giahan_tratruoc1 to ds_giahan_tratruoc1_08;

drop table ds_giahan_tratruoc purge;

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
                                    when a.loaitb_id in (210, 222, 224) then 0.5 - nvl(0.3* (select distinct 1 from kehoach_2024 ax
                                                                                                                                            where ax.trangthaitb_id is not null
                                                                                                                                                            and ax.loaitb_id in (58, 59)
                                                                                                                                                            and not exists (select 1 from kehoach_2024 where thuebao_id = ax.thuebao_id and ngay_kt_mg > ax.ngay_kt_mg)
                                                                                                                                            and ax.khachhang_id = dc.khachhang_id and dc.ngay_kt_mg = ax.ngay_kt_mg), 0)
                                   ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                    when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from kehoach_2024 ax
                                                                                                                                            where ax.trangthaitb_id is not null
                                                                                                                                                            and ax.loaitb_id in (58, 59)
                                                                                                                                                            and not exists (select 1 from kehoach_2024 where thuebao_id = ax.thuebao_id and ngay_kt_mg > ax.ngay_kt_mg)
                                                                                                                                            and ax.khachhang_id = dc.khachhang_id and dc.ngay_kt_mg = ax.ngay_kt_mg), 0)
                                else 0 
                    end Heso
                    , decode(dc.congvan_id, 190, 0, (select count(thuebao_id) from kehoach_2024 ax
                                                                                            where ax.trangthaitb_id is not null and congvan_id  <> 190 
                                                                                                            and ax.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224)
                                                                                                            and not exists (select 1 from kehoach_2024 where thuebao_id = ax.thuebao_id and ngay_kt_mg > ax.ngay_kt_mg)
                                                                                                            and ax.khachhang_id = dc.khachhang_id and ax.ngay_kt_mg = dc.ngay_kt_mg)) uutien_giao
                    , cast(1 as number(2)) tratruoc 
                    -- dc.*, pkg.*--chitietkm_id, dc.thuebao_id
-- select * 
from css_hcm.db_thuebao a
            join css_hcm.db_adsl a1 on a.thuebao_id = a1.thuebao_id and a1.chuquan_id = 145
            join ttkd_bct.db_thuebao_ttkd a2 on a.thuebao_id = a2.thuebao_id --and a.thanhtoan_id = a2.thanhtoan_id
            left join css_hcm.db_khachhang dbkh on a.khachhang_id = dbkh.khachhang_id
           join kehoach_2024 dc on a.thuebao_id = dc.thuebao_id                   --thang n
          --  join kehoach_2024_tmp dc on a.thuebao_id = dc.thuebao_id  ---thang n+1
                    
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
                        left join kiemsoat.dbtb_20240101 ks on a.thuebao_id = ks.thuebao_id                                             -----change
                        left join tinhcuoc_hcm.dbkh partition for (20240101) khc on a.khachhang_id = khc.khachhang_id                   ----change
                        left join css_hcm.loai_kh lkh on khc.loaikh_id = lkh.loaikh_id
   
    where a.trangthaitb_id not in (7, 8, 9) -- and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) = 202311 --and a.thuebao_id  in( 9049183)
                        and a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) --and pkg.nhomtb_id = '334800'
                        and not exists (select 1 from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id = dc.thuebao_id and ngay_kt_mg > dc.ngay_kt_mg)
              ;         
            
  ;
  rollback;
  commit;
  select * from ds_giahan_tratruoc1;
  rollback;
  
  ---update fiber_donle theo MATB_TN, MADOICAP cua MYTV
   update kehoach_2024 a set fiber_donle = 0
            --- select * from ds_giahan_tratruoc1 a
            where fiber_donle = 1-- and thang = 202307
                            and exists (select 1 from css.v_db_thuebao@dataguard xj, css.v_db_adsl@dataguard xk 
                                                        where xj.thuebao_id = xk.thuebao_id and xj.loaitb_id in (61, 171, 210, 222, 224)
                                                          and xk.matb_tn = a.ma_tb )
                           ;
                   update kehoach_2024 a set fiber_donle = 0
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
                                        from kehoach_2024 a
                                                    left join css_hcm.khuyenmai_dbtb km on a.rkm_id = km.rkm_id
                                                    left join css_hcm.db_datcoc db on a.rkm_id = db.rkm_id
                                                    left join css_hcm.hdtb_datcoc hddc on db.thuebao_dc_id = hddc.thuebao_dc_id
                                                    left join css_hcm.hd_thuebao hdtb on nvl(km.hdtb_id, hddc.hdtb_id) = hdtb.hdtb_id
                                                    left join css_hcm.phieutt_hd pttt on hdtb.hdkh_id = pttt.hdkh_id and pttt.ht_tra_id <> 6
                                    )
*/
select * from table(dbms_xplan.display);
drop table ds_giahan_tratruoc --purge;
commit;
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
          , dc.CUOC_DC, dc.SO_THANGDC, dc.so_thangkm, dc.congvan_id, dc.KHUYENMAI_ID, dc.CHITIETKM_ID, dc.SL_DATCOC, dc.NHOMTB_ID, dc.GOI_ID, ttin.ht_tra_id, ttin.kenhthu_id
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
                                    when a.loaitb_id in (210, 222, 224) then 0.5 - nvl(0.3* (select distinct 1 from kehoach_2024 ax
                                                                        where ax.loaitb_id in (58, 59)
                                                                                        and not exists (select 1 from kehoach_2024 where thuebao_id = ax.thuebao_id 
                                                                                                                            and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                        and ax.khachhang_id = dc.khachhang_id 
                                                                        and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm'))), 0)
                                   ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                    when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from kehoach_2024 ax
                                            where ax.loaitb_id in (58, 59)
                                                            and not exists (select 1 from kehoach_2024 where thuebao_id = ax.thuebao_id 
                                                                                                and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                            and ax.khachhang_id = dc.khachhang_id
                                            and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm'))), 0)
                                else 0 
                    end Heso
                    , decode(dc.congvan_id, 190, 0, (select count(thuebao_id) from kehoach_2024 ax
                                                    where congvan_id  <> 190 
                                                    and ax.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                                                    and not exists (select 1 from kehoach_2024 where thuebao_id = ax.thuebao_id 
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
    join kehoach_2024 dc on a.thuebao_id = dc.thuebao_id                   --thang n
                                join css_hcm.db_thanhtoan tt on a.thanhtoan_id = tt.thanhtoan_id
                               left join css_hcm.tuyenthu tut on tt.tuyenthu_id = tut.tuyenthu_id
                    --            left join hocnq_ttkd.nv_tuyenthu nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                                left join nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                    
                    left join css_hcm.diachi_tb dcld on a.thuebao_id = dcld.thuebao_id
                    left join css_hcm.diachi dmdc on dcld.diachild_id = dmdc.diachi_id
                    left join kv on a.thuebao_id = kv.thuebao_id                                             -----change
                    left join tinhcuoc_hcm.dbkh partition for (20231201) khc on a.khachhang_id = khc.khachhang_id                   ----change
                    left join css_hcm.loai_kh lkh on lkh.loaikh_id = khc.loaikh_id
                    left join ttin on dc.rkm_id = ttin.rkm_id 
    ---end part 1----                
                  
    where  a.trangthaitb_id not in (7, 8, 9) and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) = 202403 
                        and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) 
                       and not exists (select 1 from kehoach_2024 where thuebao_id = dc.thuebao_id and ngay_kt_mg > dc.ngay_kt_mg and nhom_datcoc_id = dc.nhom_datcoc_id) --and dc.rkm_id in 
--                       (5747847,6490666,5942183,5836941,6301975,5803981,5201083)
  ;
  commit;
select* from DS_GIAHAN_TRATRUOC --where thuebao_id= 8609353-- in 
(select thuebao_id from DS_GIAHAN_TRATRUOC group by thuebao_id having count(thuebao_id) > 1 )
select a.* , b.nhom_datcoc_id from DS_GIAHAN_TRATRUOC a left join css_hcm.db_datcoc b on a.rkm_id = b.rkm_id 
    left join css_hcm.khuyenmai_Dbtb c on a.rkm_id = c.rkm_id
    left join css_hcm.ct_khuyenmai d on c.chitietkm_id = d.chitietkm_id
    where a.thuebao_id = 8609353 --nvl(b.nhom_datcoc_id, d.nhom_Datcoc_id) =1
  ---update fiber_donle theo MATB_TN, MADOICAP cua MYTV;
select* from v_Thongtinkm_all_ol where thuebao_id = 8609353

select* from ds_giahan_tratruoc where rkm_id not in (select rkm_id from hocnq_ttkd.ds_giahan_tratruoc);
select* from hocnq_ttkd.ds_giahan_tratruoc where rkm_id not in (select rkm_id from ds_giahan_tratruoc);

update ds_giahan_tratruoc a set fiber_donle = 0
--- select * from ds_giahan_tratruoc a
where fiber_donle = 1
and exists (select 1 from css.v_db_thuebao@dataguard xj, css.v_db_adsl@dataguard xk 
where xj.thuebao_id = xk.thuebao_id and xj.loaitb_id in (61, 171, 210, 222, 224)
                    and xk.matb_tn = a.ma_tb)
       ;
       commit
update final_kh a set fiber_donle = 0
--- select * from ds_giahan_tratruoc a
where fiber_donle = 1
    and exists (select 1 from css.v_db_thuebao@dataguard xj
    where xj.loaitb_id in (61, 171, 210, 222, 224)
                        and xj.khachhang_id = a.khachhang_id)

;
select* from kehoach_2024
--update so dien thoai lien he bsung
update ds_giahan_tratruoc hs
set hs.sdt_lh = (select lh.dienthoai
    from css.v_db_lienhe@dataguard lh, css.v_db_lienhe_md@dataguard md
                , (select * from css.v_db_ttlh@dataguard a 
                    where ttlh_id = (select max(ttlh_id) from css.v_db_ttlh@dataguard where db_id = a.db_id and ttkh_id = a.ttkh_id group by ttkh_id, db_id)) b
    where lh.ttlh_id = md.ttlh_id and md.mucdich_id in (1, 5, 29, 30) and lh.dienthoai is not null and rownum = 1
                    and lh.ttlh_id = b.ttlh_id and b.ttkh_id = 1 and b.db_id = hs.khachhang_id)
where hs.sdt_lh is null
commit;
;
--update so dien thoai bao hu
update ds_giahan_tratruoc a
set ghichu = null
    , sdt_bh = (select dienthoai_lh from (
                            select thuebao_id, dienthoai_lh, rank() over(partition by thuebao_id order by baohong_id desc) rnk from baohong_hcm.baohong
                              ) xj where rnk = 1 and xj.thuebao_id = a.thuebao_id
            )
;
alter table final_kh add khdn number
----update KHDN is null
update final_kh a
                set khdn = (select khdn from css_hcm.db_khachhang kh 
                                    join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id and a.khachhang_id = kh.khachhang_id 
                                                                    )
        where khdn is null
;
-----Update ds tra truoc
update kehoach_2024 a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc a
where not exists (select congvan_id from kehoach_2024 where congvan_id not in (190, 343, 483, 491, 545, 8922) and a.thuebao_id = thuebao_id)
;
----Update ds tra truoc
update kehoach_2024 a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc a
where so_thangdc < 3 and tratruoc = 1
;
commit;
rollback;
select* from css_hcm.diachi_tb where thuebao_id in (9407268,7443621,9332862)
select* from css_hcm.db_thuebao where thuebao_id in (9407268,7443621,9332862)
select* from final_kh
create table ds_diachi as;
create table final_kh as 
select to_char(a.ngay_kt_mg,'yyyymm') thang_kt, a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC,
a.SO_THANGDC, a.SO_THANGKM, 
a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,
c.thanhtoan_id, d.ma_Tt,e.ma_nv, f.tento,g.ten_pb, b.tinh_id, b.quan_id, b.phuong_id from kehoach_2024 a 
join dchi_toanbo_thuebao b on a.thuebao_id = b.thuebao_id
    left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
    left join css_hcm.db_Thanhtoan d on c.thanhtoan_id = d.thanhtoan_id
    left join ttkd_bct.db_thuebao_ttkd e on a.thuebao_id = e.thuebao_id
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) f on f.tbh_id = e.tbh_ql_id and e.pbh_ql_id = f.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) g on g.pbh_id = e.pbh_ql_id
where a.tratruoc = 1;
select rkm_id from v_Thongtinkm_all where ma_tb = 'phatdat39pnt';
select hdtb_id from css_hcm.khuyenmai_dbtb where rkm_id = 6453132;
select* from css_hcm.phieutt_hd where phieutt_id = 8001297;
create table khdn as 
select to_char(a.ngay_kt_mg,'yyyymm') thang_kt,a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID,lh.loaihinh_Tb, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC
,a.SO_THANGDC, a.SO_THANGKM, a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn
,count (distinct case when nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) and b.nhomtb_id is null 
    then b.thuebao_id else null end ) SLTB_ChuaGhep_GoiDadv -- + count(case when a.nhomtb_id is null then 1 else null end) 
--,
,LISTAGG( distinct case when  nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) 
and b.nhomtb_id is null then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) 
 DS_ChuaGhep_Goi
,count(distinct case when a.ma_Tt != b.ma_Tt and nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0)
then b.thuebao_id else null end)  as SLTB_Khac_MaTT
--,
,LISTAGG( distinct case when  a.tinh_id = b.tinh_id and a.quan_id = b.quan_id and a.phuong_id = b.phuong_id  and b.ma_tt != a.ma_tt then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id)
 DS_ChuaGhepMaTT
,count(distinct case when nvl(a.tinh_id ,0) != nvl(b.tinh_id,0) or nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) then b.thuebao_id else null end )  as SLTB_Khac_DiaChi
--,
,LISTAGG( distinct case when nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) and nvl(a.tinh_id,0) != nvl(b.tinh_id,0) then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id)
 DS_KhacDiaChi
, count (distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.thuebao_id else null end) SLTB_LechKy
--,
,LISTAGG( distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) 
 DS_LechKY
, count(distinct b.thuebao_id) + 1 as SLTB

from final_kh a
    left join final_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id 
    left join css_hcm.loaihinh_Tb lh on a.loaitb_id =lh.loaitb_id
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
        left join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id 
where lkh.khdn= 1 and a.khachhang_id not in (9798610,9769564, 9759328,9726101,9795170) --and a.khachhang_id = kh.khachhang_id 
group by a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC,
a.SO_THANGDC, a.SO_THANGKM, 
a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, lh.loaihinh_Tb,a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn;
select * from final_kh  where khachhang_id =9715656--ma_tb in ('dtcattuong', 'minhtri-f26');

select * from final_kh-- thuebao_id, chitietkm_id from ds_giahan_tratruoc group by thuebao_id, chitietkm_id having count(*)>1
;
select* from khdn where thuebao_id in (1307267,1306728,1307096)
update final_kh a set khachhang_id = (select khachhang_id from css_Hcm.db_thuebao where thuebao_id = a.thuebao_id) where a.khachhang_id is null
commit;
select *  from final_kh-- where thuebao_id in (select thuebao_id from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202312 and tratruoc = 1) and tratruoc = 1

;
alter table khdn add ds_lk clob--, ds_matt clob, ds_dchi clob, ds_lk clob;

update khdn  set ds_dchi = (select ab.thuebao_id, RTRIM(XMLAGG(XMLELEMENT(E,B.MA_TB,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATB
    from khdn ab join final_kh b on ab.thuebao_id != b.thuebao_id and ab.khachhang_id = b.khachhang_id
    where  to_number(to_char(ab.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm'))
--    and khdn.thuebao_id = ab.thuebao_id
    group by ab.khachhang_id, ab.thuebao_id
    order by ab.thuebao_id
--    having count (a.thuebao_id) > 1
    )
    
          
commit;



