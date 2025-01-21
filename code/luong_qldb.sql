select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null and ma_kpi in (
select ma_kpi from ttkd_bsc.blkpi_danhmuc_kpi where loai='dongia' and thang_kt is null); and thuchien != 'Hoc');
select * from ttkd_bsc.blkpi_danhmuc_kpi where loai='dongia' and thang_kt is null; and substr(ma_kpi, 16) between 71 and 71; and thuchien = 'Hoc';
SELECT * FROM bldg_danhmuc_vtcv_p1 where thang_kt is null;
select *  from ttkd_bsc.bangluong_dongia_qldb_202307;
desc  TTKD_BSC.blkpi_danhmuc_kpi_vtcv ;
insert into ttkd_bsc.blkpi_danhmuc_kpi (MA_KPI, TEN_KPI, LOAI, THANG_BD)
               values ('HCM_LUONG_QLDB_071', 'TC_071 T?ng ti?n l??ng ??n giá theo 6 nhi?m v?', 'dongia', 202308);

alter table TTKD_BSC.blkpi_danhmuc_kpi add HCM_LUONG_QLDB_071 varchar2(500) ;

update TTKD_BSC.blkpi_danhmuc_kpi set ghichu = ten_kpi
                where loai='dongia' and thang_kt is null; and substr(ma_kpi, 16) between 71 and 71
                ;
commit;
rollback;

select * from ttkd_bsc.nhanvien_202308;
insert into TTKD_BSC.blkpi_danhmuc_kpi_vtcv (MA_KPI, MA_VTCV, LOAI, TYTRONG, TO_TRUONG_PHO, GIAMDOC_PHOGIAMDOC, THANG_BD)
            select b.MA_KPI, a.MA_VTCV, a.LOAI, 0 TYTRONG, a.TO_TRUONG_PHO, a.GIAMDOC_PHOGIAMDOC, a.THANG_BD
            from TTKD_BSC.blkpi_danhmuc_kpi_vtcv a, blkpi_danhmuc_kpi b 
                where a.loai='dongia' and a.thang_kt is null and a.ma_kpi = 'HCM_LUONG_QLDB_054' and b.loai='dongia' and b.thang_kt is null
                                                     and (b.ma_kpi, a.ma_vtcv) not in (
select ma_kpi, ma_vtcv from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null); and thuchien != 'Hoc');

;
        ---Khai bao gia tri ma_kpi
            select vtcvp1.MA_VTCV, P1, DONGIA, vtcvp1.GHICHU, kpivtcv.MA_KPI, TYTRONG, TO_TRUONG_PHO, GIAMDOC_PHOGIAMDOC, DONVI
            from ttkd_bsc.bldg_danhmuc_vtcv_p1 vtcvp1 
                            left join TTKD_BSC.blkpi_danhmuc_kpi_vtcv kpivtcv on kpivtcv.ma_vtcv = vtcvp1.ma_vtcv and kpivtcv.loai='dongia' and kpivtcv.thang_kt is null
                             join ttkd_bsc.blkpi_danhmuc_kpi kpi on kpi.loai='dongia' and kpi.thang_kt is null and thuchien = 'Hoc' and kpi.ma_kpi = kpivtcv.ma_kpi
            where vtcvp1.thang_kt is null 
                             --   and kpivtcv.ma_kpi in (
                            --    select ma_kpi from ttkd_bsc.blkpi_danhmuc_kpi where loai='dongia' and thang_kt is null and thuchien = 'H?c');
;


---test xoa sau khi xong
create table hocnq_ttkd.blkpi_danhmuc_kpi_vtcv as select a.*, a.thang_bd tytrong_tl, a.thang_bd tienluong_kh, a.thang_bd dinhmuc from TTKD_BSC.blkpi_danhmuc_kpi_vtcv a;
select a.ma_nv, a.ten_nv, a.ma_vtcv, a.ten_vtcv, b.ma_kpi, ten_kpi--, null TO_TRUONG_PHO, null GIAMDOC_PHOGIAMDOC, a.ten_vtcv, ngay_ins, thang_bd, thang_kt
from ttkd_bsc.nhanvien_202307 a
                , (select * from ttkd_bsc.blkpi_danhmuc_kpi where loai='dongia' and thang_kt is null and thuchien = 'H?c') b
              --  join TTKD_BSC.blkpi_danhmuc_kpi_vtcv b on a.
where ma_vtcv in ('VNP-HNHCM_BHKV_12', 'VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_2', 'VNP-HNHCM_BHKV_18');
;
-- Table ttkd_bsc.tonghop_dongia
drop table ttkd_bsc.tonghop_dongia purge;

CREATE TABLE "ttkd_bsc.tonghop_dongia"(
  "THANG" Number,
  "MA_NV" Varchar2(20 ) NOT NULL,
  "TEN_NV" Varchar2(100 ),
  "MA_VTCV" Varchar2(30 ) NOT NULL,
  "TEN_VTCV" Varchar2(100 ),
  "MA_TO" Varchar2(20 ),
  "TEN_TO" Varchar2(100 ),
  "MA_PB" Varchar2(20 ),
  "TEN_PB" Varchar2(100 ),
  "MA_KPI" Varchar2(30 ) NOT NULL,
  "TEN_KPI" Varchar2(400 ),
  "NCTT" Number,
  "DIEM_P1" Number,
  "DONGIA" Number,
  "GIATRI" Number(10,2)
)
;
select * from hocnq_ttkd.blkpi_danhmuc_kpi_vtcv;

----update diem P1, dongia/vtcv
update TTKD_BSC.bldg_danhmuc_vtcv_p1 set p1 =, dongia = 25000, ghichu = 'P1: ?i?m; DONGIA: 25000 vnd'; 
;
-----update tytrong tien luong/vtcv/kpi
update TTKD_BSC.blkpi_danhmuc_kpi_vtcv set tytrong = 15, donvi = 'tyle' where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_053'
;
-----update dinh muc thue bao/vtcv/kpi
update TTKD_BSC.blkpi_danhmuc_kpi_vtcv set tytrong =  4783, donvi = 'thuebao' where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_055' and ma_vtcv =  'VNP-HNHCM_BHKV_2.2'
;
-----update don gia kenh khac gh thanh cong dongia/thue bao/vtcv/kpi
update TTKD_BSC.blkpi_danhmuc_kpi_vtcv set tytrong = 5700, donvi = 'dong' where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_061' and ma_vtcv =  'VNP-HNHCM_BHKV_12'
;
-----update ty le giao de danh gia muc do hoan thanh vtcv/kpi
update TTKD_BSC.blkpi_danhmuc_kpi_vtcv set tytrong = 65, donvi = 'tyle' where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_059'; and ma_vtcv =  'VNP-HNHCM_BHKV_12'
;
commit;
rollback;
    ----tao du lieu view tong quan cac gia KPI
         
            delete from ttkd_bsc.tonghop_dongia where thang = 202401;
            commit;
          select* from ttkd_bsc.tonghop_dongia  where thang = 202401 and ma_nv = 'VNP016534'
          insert into ttkd_bsc.bangluong_dongia_qldb (thang) values (-10)
          delete from ttkd_bsc.tonghop_dongia where thang = 202311
          select* from ttkd_bsc.tonghop_dongia where thang = 202311
          create table backup_tonghop_dongia as           select* from ttkd_bsc.tonghop_dongia where thang = 202311
select* from ttkd_bsc.tonghop_dongia  where thang = 202401-- and manv = 'VNP016534'
insert into ttkd_bsc.tonghop_dongia 
with dm_kpi as (select * from ttkd_bsc.blkpi_danhmuc_kpi where loai='dongia' and thang_kt is null)-- and thuchien = 'Hoc')
    , dm_diemp1 as (SELECT * FROM ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang_kt is null)
    , m_kpi_vtcv as (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null)
    , nv as (select a.ma_nv, a.ten_nv, a.ma_vtcv ma_vtcv_old, a.ten_vtcv, a.ma_to, a.ten_to, a.ma_pb, a.ten_pb 
                                    , case
                                                when a.ma_vtcv = 'VNP-HNHCM_BHKV_18' and a.ma_to = 'VNP0701402' then 'VNP-HNHCM_BHKV_18.1'
                                                when a.ma_vtcv = 'VNP-HNHCM_BHKV_18' and a.ma_to = 'VNP0702215' then 'VNP-HNHCM_BHKV_18.1'
                                                when a.ma_vtcv = 'VNP-HNHCM_BHKV_18' and a.ma_to = 'VNP0702216' then 'VNP-HNHCM_BHKV_18.1'
                                                when a.ma_nv = 'VNP016534' then 'VNP-HNHCM_BHKV_2.2' --ma_nv = 'VNP016534'
                                        else a.ma_vtcv end ma_vtcv
                        from ttkd_bsc.nhanvien_202401 a) 
select 202401 thang
                , a.ma_nv, a.ten_nv, a.ma_vtcv, a.ten_vtcv, a.ma_to, a.ten_to, a.ma_pb, a.ten_pb, c.ma_kpi, c.ten_kpi, 22 NCTT, b.p1 diem_p1, b.dongia
               ---cach 1 khong khai bao ma_kpi cha, tinh toan gia tri trong bang tong hop
               , b1.tytrong as giatri
               ---cach 2 khai bao ma_kpi_cha ma kpi co lien quan tinh *
            --    , CASE WHEN b1.MA_KPI_cha is not null THEN b.dongia * b.p1 * b2.tytrong ELSE b1.tytrong END  giatri
             
from nv a
            left join dm_diemp1 b on a.ma_vtcv = b.ma_vtcv
            join m_kpi_vtcv b1 on a.ma_vtcv = b1.ma_vtcv 
            join dm_kpi c on b1.ma_kpi = c.ma_kpi
      --      left join m_kpi_vtcv b2 on b1.ma_vtcv = b2.ma_vtcv and b1.MA_KPI_cha = b2.MA_KPI
            --where ma_nv = 'CTV043447'; and b1.MA_KPI = 'HCM_LUONG_QLDB_054'
;

            commit;
select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null and ma_kpi = 'HCM_LUONG_QLDB_053'
select ma_nv, ma_kpi, count(*) from ttkd_bsc.tonghop_dongia WHERE thang = 202401 group by ma_nv, ma_kpi; and ma_nv = 'CTV043447';
select * from ttkd_bsc.nhanvien_202308 where ma_vtcv in (select ma_vtcv from m_kpi_vtcv);
select* from ttkd_Bsc.tonghop_dongia where thang = 202401 and ma_kpi = 'HCM_LUONG_QLDB_053'
commit;
select * from ttkd_bsc.bangluong_dongia_qldb WHERE thang = 202401 --and ma_nv in ('CTV074982') and ten_kpi like 'TC5%';
commit;
rollback;
select * from ttkd_bsc.
        -- 
        select giatri from ttkd_Bsc.tonghop_dongia where thang  = 202311 and ma_kpi = 'HCM_LUONG_QLDB_056'
---Update gia HCM_LUONG_QLDB_054 = p1 * dongia * tytrong tien luong
update ttkd_bsc.tonghop_dongia a 
            set giatri = (select DIEM_P1 * DONGIA * nvl(giatri, 0)/100 from ttkd_bsc.tonghop_dongia where thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_053' and a.ma_nv = ma_nv)
            WHERE a.ma_kpi = 'HCM_LUONG_QLDB_054' and thang = 202401
;
                    rollback;
                    commit;
;  
---Update gia HCM_LUONG_QLDB_056 = tong heso thuebao da giao ghtt da quy doi
update ttkd_bsc.tonghop_dongia a 
set giatri = (select round(sum(heso_giao), 0) from ttkd_bsc.tl_giahan_tratruoc
                            where MA_KPI in ('HCM_TB_GIAHA_022') and loai_tinh = 'KPI_NV' and thang = a.thang and a.ma_nv = ma_nv)
WHERE a.ma_kpi = 'HCM_LUONG_QLDB_056' and thang = 202401
;
                     ----TO TRUONG
update ttkd_bsc.tonghop_dongia a 
set giatri = (select round(sum(heso_giao), 0) from ttkd_bsc.tl_giahan_tratruoc
                                    where MA_KPI in ('HCM_TB_GIAHA_022') and loai_tinh = 'KPI_TO' and thang = 202401 and a.ma_to = ma_to)
--select * from ttkd_bsc.tonghop_dongia a 
WHERE a.ma_kpi = 'HCM_LUONG_QLDB_056' and thang = 202401
          and exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 
                      and thang_kt is null and MA_VTCV = a.MA_VTCV and ma_kpi = a.ma_kpi)
;
                     ----PGD
update ttkd_bsc.tonghop_dongia a1
set giatri = ( select round(sum(a.heso_giao), 0) from ttkd_bsc.tl_giahan_tratruoc a
                                                            join ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach b on b.thang = a.thang and a.ma_to = b.ma_to
                            where a.MA_KPI in ('HCM_TB_GIAHA_022') and a.loai_tinh = 'KPI_NV'
                                                and a.thang = 202401 and b.manv_pgd = a1.ma_nv
                     )
-- select * from ttkd_bsc.TONGHOP_DONGIA a1
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_056' and a1.thang = 202401
            and exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = a1.ma_kpi)
            and exists (select 1 from ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach x2 where x2.thang = a1.thang and x2.manv_pgd = a1.MA_NV)
;                                         
commit;
            ---Update gia HCM_LUONG_QLDB_057 (He so K5)= HCM_LUONG_QLDB_056/ HCM_LUONG_QLDB_055
update ttkd_bsc.TONGHOP_DONGIA a 
set giatri = round((select giatri from TTKD_BSC.TONGHOP_DONGIA where thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_056' and a.ma_nv = ma_nv)
                            / (select giatri from TTKD_BSC.TONGHOP_DONGIA where thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_055' and a.ma_nv = ma_nv), 4)
WHERE a.ma_kpi = 'HCM_LUONG_QLDB_057' and thang = 202401
;
                    select* from 
---Update gia HCM_LUONG_QLDB_058 = Ty le ghtt TC 30 ngay thang T
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri =  ( select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc a 
                    where a.MA_KPI in ('HCM_TB_GIAHA_022') and a.loai_tinh = 'KPI_NV'
                                    and a.thang = a1.thang and a.ma_nv = a1.ma_nv
             )
--               select * from TONGHOP_DONGIA a1
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_058' and a1.thang = 202401
; 
                    ----TO TRUONG
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri = ( select tyle from ttkd_bsc.tl_giahan_tratruoc a where a.MA_KPI in ('HCM_TB_GIAHA_022') and a.loai_tinh = 'KPI_TO'
                        and a.thang = a1.thang and a.ma_to = a1.ma_to
                     )
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_058' and a1.thang = 202401
and exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 
                                            and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = a1.ma_kpi)
;
                    ----PGD
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri = ( select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                from ttkd_bsc.tl_giahan_tratruoc a 
                                                join ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach b on b.thang = a.thang and a.ma_to = b.ma_to
                where a.MA_KPI in ('HCM_TB_GIAHA_022') and a.loai_tinh = 'KPI_NV'
                                    and a.thang = a1.thang and b.manv_pgd = a1.ma_nv
         )
--select * from TONGHOP_DONGIA a1
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_058' and a1.thang = 202401
                and exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                                and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = a1.ma_kpi)
                and exists (select * from ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach x2 where x2.thang = a1.thang and x2.manv_pgd = a1.MA_NV)
;
commit;
                    ---Update gia HCM_LUONG_QLDB_059 = Ty le TH ghtt TC 30 ngay (so luong ghtt/ soluong giao ghtt) /tyle giao 65% theo van ban                                                           
update ttkd_bsc.TONGHOP_DONGIA a 
set a.giatri = (select giatri
            from TTKD_BSC.TONGHOP_DONGIA where thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_058' and a.ma_nv = ma_nv)
        *100/  (select tytrong from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai = 'dongia' and thang_kt is null and ma_kpi = a.ma_kpi and a.ma_vtcv = ma_vtcv) 
WHERE a.ma_kpi = 'HCM_LUONG_QLDB_059' and thang = 202401
;
commit;
---Update gia HCM_LUONG_QLDB_060 = so luong thue bao gh thanh cong kenh khac thuc hien (ma_nv giao <> manv tu van)
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri = (select round(sum(heso), 0)
                from ttkd_bsc.ct_dongia_tratruoc a
                where a.ma_kpi = 'DONGIA' and a.loai_tinh = 'DONGIATRA30D' and a.dthu > 0 and a.tien_khop > 0 and a.manv_giao <> a.ma_nv 
                            and a.thang = a1.thang  and a.manv_giao = a1.ma_nv
                 )
 --    select * from ttkd_bsc.ct_dongia_tratruoc@ttkddb a where thang  = 202308 and ma_kpi = 'DONGIA'  and loai_tinh = 'DONGIATRA30D' and manv_giao <> ma_nv
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_060' and a1.thang = 202401
;
                    
---To truong                          
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri = (SELECT sum(GIATRI)
                      FROM ttkd_bsc.TONGHOP_DONGIA a
                      WHERE exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho is null and giamdoc_phogiamdoc is null
                                                                                and thang_kt is null and MA_VTCV = a.MA_VTCV and ma_kpi = a.ma_kpi)
                                        and thang = a1.thang and a1.ma_kpi = ma_kpi and ma_to = a1.ma_to
                 )
      
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_060' and a1.thang = 202401
and exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 
                                and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = a1.ma_kpi)
;
---Pho giam doc                          
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri = (SELECT sum(GIATRI)
                          FROM ttkd_bsc.TONGHOP_DONGIA a
                          WHERE exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho is null and giamdoc_phogiamdoc is null
                                                                                    and thang_kt is null and MA_VTCV = a.MA_VTCV and ma_kpi = a.ma_kpi)
                                            and a.ma_pb = a1.ma_pb and a.thang = a1.thang and a1.ma_kpi = a.ma_kpi
                     )
       --              select * from TONGHOP_DONGIA a1
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_060' and a1.thang = 202401
and exists (select 1 from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                    and thang_kt is null and MA_VTCV = a1.MA_VTCV and ma_kpi = a1.ma_kpi)
and exists (select 1 from ttkd_bsc.bldg_danhmuc_qldb_pgd_phutrach x2 where x2.thang = a1.thang and x2.manv_pgd = a1.MA_NV)
;
commit;
                    ---Update gia HCM_LUONG_QLDB_062 = (HCM_LUONG_QLDB_054 * HCM_LUONG_QLDB_057 * HCM_LUONG_QLDB_059/100) - (HCM_LUONG_QLDB_060 * HCM_LUONG_QLDB_061)
                                                    
update ttkd_bsc.TONGHOP_DONGIA a1
set giatri = round(((select giatri
                FROM ttkd_bsc.TONGHOP_DONGIA
                WHERE thang = a1.thang and ma_nv = a1.ma_nv and ma_kpi = 'HCM_LUONG_QLDB_054'
             )           
            * (select giatri
                        FROM ttkd_Bsc.TONGHOP_DONGIA
                        WHERE thang = a1.thang and ma_nv = a1.ma_nv and ma_kpi = 'HCM_LUONG_QLDB_057'
                     )
            * (select giatri/100
                    FROM ttkd_bsc.TONGHOP_DONGIA
                    WHERE thang = a1.thang and ma_nv = a1.ma_nv and ma_kpi = 'HCM_LUONG_QLDB_059'
        )), 0)
        - ((select nvl(giatri, 0)
                    FROM ttkd_bsc.TONGHOP_DONGIA
                    WHERE thang = a1.thang and ma_nv = a1.ma_nv and ma_kpi = 'HCM_LUONG_QLDB_060'
                 )        
                * (select giatri
                        FROM ttkd_bsc.TONGHOP_DONGIA
                        WHERE thang = a1.thang and ma_nv = a1.ma_nv and ma_kpi = 'HCM_LUONG_QLDB_061'
            ))
WHERE a1.ma_kpi = 'HCM_LUONG_QLDB_062' and a1.thang = 202401
;
                commit;
select *
                                            from ttkd_bsc.ct_dongia_tratruoc  a
                                            where ma_kpi = 'DONGIA' 
                                                            and loai_tinh = 'DONGIATRA30D' and thang = 202307
                                                            ;
                    SELECT *--THANG, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_KPI, NCTT, DIEM_P1, DONGIA, GIATRI
                      FROM ttkd_bsc.TONGHOP_DONGIA
                      WHERE thang = 202311 and ma_nv = 'VNP017522' and ten_kpi like 'TC5%';
                      update TONGHOP_DONGIA a1 set giatri = 0 where a1.ma_kpi = 'HCM_LUONG_QLDB_062' and a1.thang = 202307;
            commit;
            rollback;
            
SELECT * FROM
(
  SELECT THANG, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_KPI, NCTT, DIEM_P1, DONGIA, GIATRI
  FROM TONGHOP_DONGIA
  WHERE thang = 202308 --and ma_nv = 'CTV043447'
 )--as OriginalOrder
PIVOT
(
  sum(giatri)
  FOR ma_kpi
  IN  ('HCM_LUONG_QLDB_001' "TC1_001 T? tr?ng ti?n l??ng", 'HCM_LUONG_QLDB_002' "TC1_002 Ti?n l??ng k? ho?ch tiêu chí (1) ", 'HCM_LUONG_QLDB_003' "TC1_003 ??nh m?c Thuê bao qui ??i c?a t?p khách hàng giao ch?m sóc", 
'HCM_LUONG_QLDB_004' "TC1_004 ??nh m?c Doanh thu qui ??i c?a t?p khách hàng giao ch?m sóc", 'HCM_LUONG_QLDB_005' "TC1_005 Thuê bao qui ??i c?a t?p khách hàng giao ch?m sóc", 'HCM_LUONG_QLDB_006' "TC1_006 Doanh thu qui ??i c?a t?p khách hàng giao ch?m sóc", 
'HCM_LUONG_QLDB_007' "TC1_007 H? s? K1", 'HCM_LUONG_QLDB_008' "TC1_008 T? l? ??t công tác CSKH", 'HCM_LUONG_QLDB_009' "TC1_009 Ti?n l??ng ch?m sóc KH", 
'HCM_LUONG_QLDB_010' "TC2_010 T? tr?ng ti?n l??ng", 'HCM_LUONG_QLDB_011' "TC2_011 Ti?n l??ng k? ho?ch tiêu chí (2)", 'HCM_LUONG_QLDB_012' "TC2_012 ??nh m?c Doanh thu qui ??i c?a t?p khách hàng BRC? giao ch?m sóc", 
'HCM_LUONG_QLDB_013' "TC2_013 Doanh thu qui ??i c?a t?p khách hàng BRC? giao ch?m sóc", 'HCM_LUONG_QLDB_014' "TC2_014 H? s? K2", 'HCM_LUONG_QLDB_015' "TC2_015  T? l? thu n? c??c tr? sau l?y k? d?ch v? BRCD", 
'HCM_LUONG_QLDB_016' "TC2_016  M?HT quy ??i c?a t? l? thu n? c??c tr? sau l?y k? d?ch v? BRC?", 'HCM_LUONG_QLDB_017' "TC2_017 Doanh thu d?ch v? BRC? s?t gi?m trong tháng", 'HCM_LUONG_QLDB_018' "TC2_018 Doanh thu d?ch v? BRC? s?t gi?m theo m?c cho phép (1)", 
'HCM_LUONG_QLDB_019' "TC2_019 M?HT quy ??i c?a t? l? doanh thu BRC? gi?m", 'HCM_LUONG_QLDB_020' "TC2_020 Doanh thu d?ch v? BRC? s?t gi?m v??t m?c cho phép (1) trong tháng", 'HCM_LUONG_QLDB_021' "TC2_021 ??n giá gi?m tr? doanh thu d?ch v? BRC? gi?m v??t m?c cho phép", 
'HCM_LUONG_QLDB_022' "TC2_022 Doanh thu d?ch v? BRC? s?t gi?m d??i m?c cho phép (2) trong tháng", 'HCM_LUONG_QLDB_023' "TC2_023 ??n giá khuy?n khích gi? doanh thu d?ch v? BRC? gi?m d??i m?c cho phép ", 'HCM_LUONG_QLDB_024' "TC2_024 Ti?n l??ng gi? doanh thu BRCD", 
'HCM_LUONG_QLDB_025' "TC3_025 T? tr?ng ti?n l??ng", 'HCM_LUONG_QLDB_026' "TC3_026 Ti?n l??ng k? ho?ch tiêu chí (3)", 'HCM_LUONG_QLDB_027' "TC3_027 ??nh m?c Doanh thu qui ??i c?a t?p khách hàng các d?ch v? còn l?i giao ch?m sóc", 
'HCM_LUONG_QLDB_028' "TC3_028 Doanh thu qui ??i c?a t?p khách hàng các d?ch v? còn l?i giao ch?m sóc", 'HCM_LUONG_QLDB_029' "TC3_029 H? s? K3", 'HCM_LUONG_QLDB_030' "TC3_030 T? l? thu n? c??c tr? sau l?y k? các d?ch v? còn l?i", 
'HCM_LUONG_QLDB_031' "TC3_031 M?HT quy ??i c?a t? l? thu n? c??c tr? sau l?y k? các d?ch v? còn l?i", 'HCM_LUONG_QLDB_032' "TC3_032 Doanh thu các d?ch v? còn l?i s?t gi?m trong tháng", 
'HCM_LUONG_QLDB_033' "TC3_033 Doanh thu các d?ch v? còn l?i s?t gi?m theo m?c cho phép (1)", 'HCM_LUONG_QLDB_034' "TC3_034 M?HT quy ??i c?a t? l? doanh thu các d?ch v? còn l?i gi?m", 'HCM_LUONG_QLDB_035' "TC3_035 Doanh thu các d?ch v? còn l?i s?t gi?m v??t m?c cho phép (1) trong tháng", 'HCM_LUONG_QLDB_036' "TC3_036 ??n giá gi?m tr? doanh thu các d?ch v? còn l?i gi?m v??t m?c cho phép (1)", 
'HCM_LUONG_QLDB_037' "TC3_037 Doanh thu các d?ch v? còn l?i s?t gi?m d??i m?c cho phép (2) trong tháng", 'HCM_LUONG_QLDB_038' "TC3_038 ??n giá khuy?n khích gi? doanh thu d?ch v? còn l?i gi?m d??i m?c cho phép", 'HCM_LUONG_QLDB_039' "TC3_039 Ti?n l??ng gi? doanh thu khác d?ch v? BRCD", 
'HCM_LUONG_QLDB_040' "TC4_040 T? tr?ng ti?n l??ng", 'HCM_LUONG_QLDB_041' "TC4_041 Ti?n l??ng k? ho?ch tiêu chí (4)", 'HCM_LUONG_QLDB_042' "TC4_042 ??nh m?c Thuê bao c?a t?p khách hàng Fiber giao ch?m sóc", 
'HCM_LUONG_QLDB_043' "TC4_043 Thuê bao c?a t?p khách hàng Fiber giao ch?m sóc", 'HCM_LUONG_QLDB_044' "TC4_044 H? s? K4", 'HCM_LUONG_QLDB_045' "TC4_045 Thuê bao Fiber không PSC trong tháng", 'HCM_LUONG_QLDB_046' "TC4_046 Thuê bao Fiber không PSC theo m?c quy ??nh (1)", 
'HCM_LUONG_QLDB_047' "TC4_047 M?HT quy ??i c?a T? l? thuê bao Fiber không PSC", 'HCM_LUONG_QLDB_048' "TC4_048 Thuê bao Fiber không PSC v??t m?c quy ??nh", 'HCM_LUONG_QLDB_049' "TC4_049 ??n giá gi?m tr? thuê bao Fiber không PSC v??t m?c quy ??nh", 
'HCM_LUONG_QLDB_050' "TC4_050 Thuê bao Fiber không PSC d??i m?c quy ??nh", 'HCM_LUONG_QLDB_051' "TC4_051 ??n giá khuy?n khích gi? thuê bao Fiber không PSC d??i m?c quy ??nh", 'HCM_LUONG_QLDB_052' "TC4_052 Ti?n l??ng gi? thuê bao Fiber PSC", 
'HCM_LUONG_QLDB_053' "TC5_053 T? tr?ng ti?n l??ng", 'HCM_LUONG_QLDB_054' "TC5_054 Ti?n l??ng k? ho?ch", 'HCM_LUONG_QLDB_055' "TC5_055 ??nh m?c thuê bao quy ??i giao ghtt", 
'HCM_LUONG_QLDB_056' "TC5_056 Thuê bao quy ??i giao ghtt", 'HCM_LUONG_QLDB_057' "TC5_057 H? s? K5", 'HCM_LUONG_QLDB_058' "TC5_058 T? l? ghtt thành công 30 ngày", 
'HCM_LUONG_QLDB_059' "TC5_059 M?HT so v?i 80% giao", 'HCM_LUONG_QLDB_060' "TC5_060 Thuê bao quy ??i kênh khác th?c hi?n ghtt TC", 'HCM_LUONG_QLDB_061' "TC5_061 ??n giá quy ??i do kênh khác th?c hi?n", 
'HCM_LUONG_QLDB_062' "TC5_062 T?ng ti?n l??ng", 'HCM_LUONG_QLDB_063' "TC6_063 T? tr?ng ti?n l??ng", 'HCM_LUONG_QLDB_064' "TC6_064 Ti?n l??ng k? ho?ch tiêu chí (5)", 
'HCM_LUONG_QLDB_065' "TC6_065 DT thu n? tác ??ng ??nh m?c hàng tháng", 'HCM_LUONG_QLDB_066' "TC6_066 DT thu n? giao", 'HCM_LUONG_QLDB_067' "TC6_067 H? s? K6", 'HCM_LUONG_QLDB_068' "TC6_068 DT thu n? th?c hi?n", 
'HCM_LUONG_QLDB_069' "TC6_069 T? l? thu n? l?y k? các d?ch v?", 'HCM_LUONG_QLDB_070' "TC6_070 Ti?n l??ng t? l? thu n? l?y k?", 
'HCM_LUONG_QLDB_071' "TC_071 T?ng ti?n l??ng ??n giá theo 6 nhi?m v?"
        )
) 
;
ORDER BY expression [ ASC | DESC ];
select sum(HCM_LUONG_QLDB_062) from ttkd_bsc.bangluong_dongia_qldb where thang = 202311 --192884350.5
----1
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_053 = (SELECT GIATRI
FROM ttkd_bsc.TONGHOP_DONGIA
WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_053' and ma_nv = a.manv)
where a.thang = 202401
;
commit;
----2
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_054 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_054' and ma_nv = a.manv)
where a.thang = 202401
;
----3        
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_055 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_055' and ma_nv = a.manv)
where a.thang = 202401
;
commit;
----4        
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_056 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_056' and ma_nv = a.manv)
where a.thang = 202401
;
----5
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_057 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_057' and ma_nv = a.manv)
where a.thang = 202401
;
-----6        
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_058 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_058' and ma_nv = a.manv)
where a.thang = 202401
;
-----7        
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_059 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_059' and ma_nv = a.manv)
where a.thang = 202401
;
-----8        
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_060 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_060' and ma_nv = a.manv)
where a.thang = 202401
;
commit;
------9        
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_061 = (SELECT GIATRI
                              FROM ttkd_bsc.TONGHOP_DONGIA
                              WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_061' and ma_nv = a.manv)
where a.thang = 202401
;
commit;
------10        
---Update gia HCM_LUONG_QLDB_062 =
update ttkd_bsc.bangluong_dongia_qldb a
set HCM_LUONG_QLDB_062 = case when exists (select * from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  >= 0.9 and manv = a.manv)
            then greatest(0.5 * (select nvl(HCM_LUONG_QLDB_054, 0) from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and manv = a.manv)
                                            , (SELECT GIATRI
                                                  FROM ttkd_bsc.TONGHOP_DONGIA
                                                  WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_062' and ma_nv = a.manv)
                                        )
        when exists (select 1 from bangluong_dongia_qldb where thang = a.thang and HCM_LUONG_QLDB_008  < 0.9 and manv = a.manv)
                then greatest(0.2 * (select nvl(HCM_LUONG_QLDB_054, 0) from ttkd_bsc.bangluong_dongia_qldb where thang = a.thang and manv = a.manv)
                                            , (SELECT GIATRI
                                                  FROM ttkd_bsc.TONGHOP_DONGIA
                                                  WHERE thang = a.thang and ma_kpi = 'HCM_LUONG_QLDB_062' and ma_nv = a.manv)
                                        )
        end

--       exists (select 1 from TONGHOP_DONGIA where ma_kpi = 'HCM_LUONG_QLDB_059' and nvl(giatri, 0) >= 90) and thang = a1.thang and ma_nv = a1.ma_nv) 
--       exists (select 1 from TONGHOP_DONGIA where ma_kpi = 'HCM_LUONG_QLDB_059' and nvl(giatri, 0) < 90) and thang = a1.thang and ma_nv = a1.ma_nv) 
where a.thang = 202401
;
commit;
rollback

select distinct manv , mato, mapb , ma_to, ma_pb
from ttkd_bsc.bangluong_dongia_qldb a 
join ttkd_bsc.tonghop_dongia b on a.thang = b.thang and a.manv = b.ma_nv
where a.ma_vtcv != b.ma_vtcv
select a.manv, a.hcm_luong_qldb_061, b.hcm_luong_qldb_061
from dongia a join ttkd_bsc.bangluong_dongia_qldb b on a.manv = b.manv 
where a.thang = b.thang and a.thang = 202311 and a.hcm_luong_qldb_055 <> b.hcm_luong_qldb_055
select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where loai='dongia' and thang_kt is null  and ma_kpi = 'HCM_LUONG_QLDB_055'
select hcm_luong_qldb_061, ma_vtcv
from TTKD_BSC.BANGLUONG_DONGIA_QLDB where thang = 202311 and manv =  'VNP016534'
select ma_Nv, ma_vtcv from ttkd_Bsc.tonghop_dongia where  ma_nv =  'VNP016534' and thang = 202311
select ma_Nv, ma_vtcv from ttkd_Bsc.nhanvien_202311 where  ma_nv =  'VNP016534' and thang = 202311

commit;
select round(sum(heso),0) from ttkd_bsc.ct_dongia_tratruoc a where loai_tinh = 'DONGIATRA30D' and thang = 202311 and manv_giao = 'VNP017364' and a.dthu > 0 and a.tien_khop > 0 and a.manv_giao <> a.ma_nv
S                      select * from ttkd_bsc.bangluong_dongia_qldb_202308 where manv = 'VNP017522';
                      select HCM_LUONG_QLDB_008 from ttkd_bsc.bangluong_dongia_qldb where thang = 202311  ;
-- SAU KHI BAO ANH NGUYEN
select sum(hcm_luong_qldb_071) from bangluong_dongia_qldb where thang = 202401
update bangluong_dongia_qldb
   set hcm_luong_qldb_071=round((nvl(hcm_luong_qldb_009,0)+nvl(hcm_luong_qldb_024,0)+nvl(hcm_luong_qldb_039,0)
                                +nvl(hcm_luong_qldb_052,0)+nvl(hcm_luong_qldb_062,0)+nvl(hcm_luong_qldb_070,0))
                               *(nctt/(select nctt from bldg_danhmuc_qldb_nctt where thang=202311 and ma_nv is null)))
 where thang=202401;
 select* from ttkd_bsc.bangluong_dongia_qldb  where thang=202401;
commit;
update bangluong_dongia_qldb
   set hcm_luong_qldb_071=null
 where thang=202401 and hcm_luong_qldb_071=0;
 commit;
 SElect ma_nv from ttkd_bsc.bangluong_dongia_qldb a
 join ba
 where 
 
 rollback
 thang = 202311

create table temp_qldb as select
THANG, MANV, HO, TEN, MA_VTCV, TEN_VTCV, MATO, TEN_TO, MAPB, TEN_PB,HCM_LUONG_QLDB_053, HCM_LUONG_QLDB_054, HCM_LUONG_QLDB_055, 
HCM_LUONG_QLDB_056, HCM_LUONG_QLDB_057, HCM_LUONG_QLDB_058, HCM_LUONG_QLDB_059, HCM_LUONG_QLDB_060, HCM_LUONG_QLDB_061, HCM_LUONG_QLDB_062
 from ttkd_bsc.bangluong_dongia_qldb where thang = 202401 