select* from nhanvien_202406_lan1;-- where thang = 202404;
select* from TTKD_BSC.NHANVIEN where ma_Nv = 'CTV082807';

rollback;
    /* -------------- UP LAI TEN PHONG BAN TEN_PB ------------------ */
update nhanvien_202406 a set ten_pb=(select distinct ten_pb from ttkd_bsc.nhanvien_202405 where ma_pb=a.ma_pb) where ma_nv = 'CTV076023';
commit ;
update nhanvien_202406 a set loai_ld = 'CTV GD' where ma_nv = 'CTV076023';

/* ------------ UP NHOMLD_ID ------------- */
update nhanvien_202406 a set nhomld_id=
    (case  when substr(upper(loai_ld),1,3) like 'CH_' then 1
                when substr(upper(loai_ld),1,3)='CTV' then 3                
                when substr(upper(loai_ld),1,4) like '_LCN' then 4
                end) where ma_nv = 'CTV076023';
rollback;
commit;
select* from nhanvien_202406_lan1-- where thang = 202404;
        
update nhanvien_vttp set thang = '202404';
update nhanvien_vttp a set nhomld_id=
    (case  when substr(upper(loai_ld),1,3) like 'CH_' then 2
                when substr(upper(loai_ld),1,3)='CTV' then 3                
                end)
where thang='202405';
;

commit;

/* --------------- UP CASE CAC FIELDS --------------- */
update nhanvien_202406
SET MANV_NNL=UPPER(trim(MANV_NNL)),MANV_HRM=UPPER(trim(MANV_HRM)),MA_NV=UPPER(trim(MA_NV)),TEN_NV=initcap(trim(TEN_NV)),MA_VTCV=UPPER(trim(MA_VTCV)), TEN_VTCV=initcap(trim(TEN_VTCV)),
    MA_TO=UPPER(trim(MA_TO)), TEN_TO=initcap(trim(TEN_TO)), MA_PB=UPPER(trim(MA_PB)), TEN_PB=initcap(trim(TEN_PB)),
    LOAI_LD=TRIM(LOAI_LD),mail_vnpt=trim(mail_vnpt),gioitinh=initcap(trim(gioitinh)),cmnd=trim(cmnd), phan_loai=trim(phan_loai), loai_laodong=trim(loai_laodong)
    where ma_nv = 'CTV076023';
COMMIT;

/* -------------- UP USER_IPCC ----------------- */
update nhanvien_202406 set user_ipcc='' where ma_nv = 'CTV076023';
update nhanvien_202406 set user_ipcc=REPLACE(MAIL_VNPT,'@vnpt.vn','') where user_ipcc is null and ma_nv = 'CTV076023';;
commit;

/* ------------------ UP NHANVIEN_ID TU FILE NHAN VIEN THANG TRUOC ----------------- */
update nhanvien_202406 a set (a.nhanvien_id,a.user_dhsxkd)=(select nhanvien_id, user_dhsxkd from ttkd_bsc.nhanvien_202405 where ma_nv=a.ma_nv) where ma_nv = 'CTV076023'; ;
commit ;
select* from ttkd_bsc.nhanvien_202406_lan1;
delete from nhanvien_202406_lan1;
/* ------------------ UP BO SUNG NHANVIEN_ID TU FILE NHAN VIEN TU ONE ----------------- */
update nhanvien_202406 a set a.nhanvien_id=(select nhanvien_id from admin_hcm.nhanvien where ma_nv=a.manv_hrm) 
where a.nhanvien_id is null and exists(select 1 from admin_hcm.nhanvien where ma_nv=a.manv_hrm) and ma_nv = 'CTV076023';;
commit;
-----
insert into userld_202405_new 
select* from userld_202405_goc where user_ld not in (select user_ld from userld_202404_goc);
/* ------------------ UP BO SUNG USER_DHSXKD TU FILE NHAN VIEN TU ONE ----------------- */
update nhanvien_202406 a set a.user_dhsxkd=(select distinct cast(ma_nd as varchar(100)) from admin_hcm.nguoidung
                                                 where nhanvien_id=a.nhanvien_id) 
where a.nhanvien_id is not null and exists(select 1 from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) and a.user_dhsxkd is null  and ma_nv = 'CTV076023';;
select* from ttkd_bsc.userld_202403_goc;
select* from ttkd_bsc.nhanvien_202406_lan1 where user_dhsxkd  is null ;
commit;
select* from ttkd_Bsc.dm_ccos;
-- update user ccos
update dm_ccos set thang = 202405 where thang is null;
/* ------------------ UP USER CCOS TU ANH KHANG CUNG CAP ----------------- */
update nhanvien_202406 A SET (a.user_ccos,trangthai_ccos)=(select user_ccos, trangthai from dm_ccos where manv_hrm=a.ma_nv and thang='202405' ) where ma_nv = 'CTV076023';
--where a.user_ccos is null 
;
commit ;

update nhanvien_202406 A SET a.user_ccos=lower(a.user_ccos) ;
commit ;
/* ------------ UP USER_CCBS ------------- */
update nhanvien_202406 a SET (a.USER_CCBS,a.tr_thai)=(SELECT USER_CCBS,tr_thai FROM ttkd_bsc.nhanvien_202405 WHERE MANV_HRM=A.MANV_HRM) where a.user_ccbs is null 
and ma_nv = 'CTV076023';;
COMMIT ;

/* ------------------ UP BO SUNG USER_CCBS TU FILE NHAN VIEN TU ONE ----------------- */
update nhanvien_202406 a set user_ccbs=(select nd.user_neo from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null)
where exists(select 1 from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null) 
and a.user_ccbs is null and ma_nv = 'CTV076023';
;
commit ;

update nhanvien_202406 A SET (A.USER_CCBS,a.tr_thai,a.user_ccos,a.trangthai_ccos)=(SELECT USER_CCBS,tr_thai,user_ccos,trangthai_ccos 
                                                                                        FROM ttkd_bsc.nhanvien_202405 WHERE MANV_HRM=A.MANV_HRM) where ma_nv = 'CTV076023';;
COMMIT ;

update nhanvien_202406 a set a.tr_thai=(select tr_thai from userld_202405_goc b where a.user_ccbs=b.user_ld) where ma_nv = 'CTV076023';;
commit;
select* from userld_202403_goc;
select manv_hrm, mail_vnpt from nhanvien_202406_lan1 where mail_vnpt not like '%@vnpt.vn' ;

/* KIEM TRA TRUNG MANV */
select manv_hrm,count(*)sl from nhanvien_202406 group by manv_hrm having count(*) > 1 ;
select ma_pb,ten_pb from nhanvien_202406_lan1 group by ma_pb,ten_pb order by ma_pb,ten_pb  ; -- ;
select ma_to,ten_to,ten_pb from nhanvien_202404 group by ma_to,ten_to,ten_pb ; -- ;

select ma_vtcv,ten_vtcv from nhanvien_202406_lan1 a group by ma_vtcv,ten_vtcv order by ma_vtcv,ten_vtcv ; -- ;
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID 
from nhanvien_202406_lan1
minus
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from ttkd_bsc.nhanvien_202406_lan1 ;

select * from nhanvien_202406_lan1 a;
create table nhanvien_vttp as select* from ttkd_bsc.nhanvien_vttp where 1=2;
left join ttkd_Bsc.nhanvien_202406_lan1 b on a.ma_nv = b.ma_nv
where MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID;
-- drop table ttkd_bsc.nhanvien_202404 purge ;
rename nhanvien_202404 to nhanvien_202404_old ;
create table nhanvien_202406 as select * from nhanvien_202406_lan1;
CREATE INDEX nhanvien_202406_manv ON nhanvien_202406 (MA_NV ASC);
CREATE INDEX nhanvien_202406_manvhrm ON nhanvien_202406 (MANV_HRM ASC);
CREATE INDEX nhanvien_202406_userccbs ON nhanvien_202406(user_ccbs ASC);
CREATE INDEX nhanvien_donvi ON nhanvien(MANV_HRM ASC);

create table dm_to as 
SELECT * FROM ttkd_bsc.dm_to a ;

/* NHO CAP NHAT DM_TO */
-- INSERT INTO DM_TO(ma_to,ten_to,ma_pb,ten_pb,hieuluc,pbh_id) 
SELECT A.MA_TO, initcap(A.TEN_TO) ten_to, A.MA_PB, initcap(A.TEN_PB) ten_pb, 1, 29 --, '' TBH_ID, '' KENHBH_ID
FROM nhanvien_202406_lan1 a
WHERE NOT EXISTS(SELECT 1 FROM DM_TO B WHERE A.MA_TO=B.MA_TO and hieuluc=1) AND A.MA_TO IS NOT NULL GROUP BY A.MA_TO, A.TEN_TO, A.MA_PB, A.TEN_PB  
; 
select* from DM_TO;
COMMIT ;
update dm_to a set ma_to_hrm=ma_to where hieuluc=1 ;

update dm_to a set (pbh_id, tbh_id)=(select distinct pbh_id, tbh_id from ttkd_bct.tobanhang where hieuluc=1 and ma_to_hrm=a.ma_to) where hieuluc=1 ;
commit ;

-- INSERT INTO DM_VTCV(ma_vtcv,ten_vtcv) 
SELECT A.MA_VTCV, initcap(A.TEN_VTCV) ten_VTCV
FROM nhanvien_202404 a
WHERE NOT EXISTS    (SELECT 1 FROM DM_VTCV B WHERE A.MA_VTCV=B.MA_VTCV) AND A.MA_VTCV IS NOT NULL GROUP BY A.MA_VTCV, A.TEN_VTCV
; 
commit;
SELECT * FROM DM_VTCV ;
select* from nhanvien_vttp where thang = 202404;
select distinct ma_to from ttkd_bsc.nhanvien_202406_lan1

SELECT distinct A.MA_VTCV, initcap(A.TEN_VTCV) ten_VTCV
FROM nhanvien_202404 a
WHERE NOT EXISTS(SELECT 1 FROM nhanvien_202209 B WHERE A.MA_VTCV=B.MA_VTCV) 
; 

-- INSERT INTO DM_PHONGBAN ;
SELECT A.MA_PB, initcap(A.TEN_PB) ten_PB, '' ACTIVE,'' MA_PB_PTTB,'' PBH_ID FROM ttkd_bsc.nhanvien_202404 a
WHERE NOT EXISTS(SELECT 1 FROM DM_PHONGBAN B WHERE A.MA_PB=B.MA_PB) AND A.MA_PB IS NOT NULL GROUP BY A.MA_PB, A.TEN_PB
; 
commit;


/* ------------------ INSERT FILE NHANVIEN THANG VAO FILE NHANVIEN_TONG ------------------------ */
select distinct thang from ttkd_bsc.nhanvien_tong ;
insert into nhanvien_tong(THANG, MANV_NNL,MA_NV,TEN_NV,MA_VTCV,TEN_VTCV,MA_TO,TEN_TO,MA_PB,TEN_PB,LOAI_LD,USER_CCBS,TR_THAI,USER_CCBS2, MAIL_VNPT,
                          SDT,NGAYSINH,NHANVIEN_ID,GIOITINH,CMND,USER_DHSXKD,MANV_HRM,USER_CCOS,TRANGTHAI_CCOS,USER_IPCC,PHAN_LOAI,LOAI_LAODONG, nhomld_id)

select '202404',MANV_NNL,MA_NV,TEN_NV,MA_VTCV,TEN_VTCV,MA_TO,TEN_TO,MA_PB,TEN_PB,LOAI_LD,USER_CCBS,TR_THAI,USER_CCBS2, MAIL_VNPT,
                SDT,NGAYSINH,NHANVIEN_ID,GIOITINH,CMND,USER_DHSXKD,MANV_HRM,USER_CCOS,TRANGTHAI_CCOS,USER_IPCC,PHAN_LOAI,LOAI_LAODONG, nhomld_id
from nhanvien_202403

;
commit;
select* from ttkd_bsc.nhanvien_202406
