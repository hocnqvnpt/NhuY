update ttkd_Bsc.nhanvien set thang = 202412 , donvi = 'VTTP' where thang is null;
commit;

delete from nhanvien;
commit;
    /* -------------- UP LAI TEN PHONG BAN TEN_PB ------------------ */

/* ------------ UP NHOMLD_ID ------------- */
update nhanvien a set loai_ld=loai_laodong;
update ttkd_Bsc.nhanvien a set nhomld_id = 
    (case  when substr(upper(MA_NV),1,3) like 'HCM' then 2
                when substr(upper(MA_NV),1,3)='CTV' then 11                
--                when substr(upper(loai_ld),1,4) like '_LCN' then 4
                end)
where thang = 202412 and donvi = 'VTTP';
commit;
update ttkd_Bsc.nhanvien a set nhomld_id = 
    (case  when substr(upper(ma_nv),1,3) like 'VNP' then 1
                when substr(upper(ma_nv),1,3)='CTV' then 3                
                when substr(upper(loai_ld),1,4) like '_LCN' then 4
                end) where nhomld_id is null;
select* from ttkd_Bsc.nhanvien where nhomld_id is null and  thang = 202412 and donvi = 'VTTP';
;
commit;
                select* from ttkd_Bsc.dm_nhomld;
                select owner , table_name from all_tab_columns where column_name = upper('nhomld_id');

select distinct loai_ld from ttkd_Bsc.nhanvien_202412 where nhomld_id is null;
rollback;
commit;
select* from ttkd_Bsc.nhanvien_202406_lan1-- where thang = 202404;;

;
select* from ttkd_Bsc.dm_nhomld;
update ttkd_Bsc.nhanvien set LOAI_LD = case when ma_nv like 'CTV%' then 'CTV' 
                                        when ma_nv like 'VNP%' then 'Chính Th?c' 
                                        when ma_nv like 'HCM%' then 'Chính Th?c' else null end
where thang = 202412 and donvi = 'VTTP';
commit;
SELECT* FROM ttkd_Bsc.nhanvien;
/* --------------- UP CASE CAC FIELDS --------------- */
update ttkd_Bsc.nhanvien
SET MANV_NNL=UPPER(trim(MANV_NNL)),MANV_HRM=UPPER(trim(MANV_HRM)),MA_NV=UPPER(trim(MA_NV)),TEN_NV=initcap(trim(TEN_NV)),MA_VTCV=UPPER(trim(MA_VTCV)), TEN_VTCV=initcap(trim(TEN_VTCV)),
    MA_TO=UPPER(trim(MA_TO)), TEN_TO=initcap(trim(TEN_TO)), MA_PB=UPPER(trim(MA_PB)), TEN_PB=initcap(trim(TEN_PB)),
    LOAI_LD=TRIM(LOAI_LD),mail_vnpt=trim(mail_vnpt),gioitinh=initcap(trim(gioitinh)),cmnd=trim(cmnd), phan_loai=trim(phan_loai), loai_laodong=trim(loai_laodong),user_ipcc=trim(user_ipcc)
   where thang = 202412 and donvi = 'VTTP';

   COMMIT;
UPDATE ttkd_Bsc.nhanvien set  LOAI_LD = 'DLCN',nhomld_id = 4 WHERE THANG = 202407 AND MA_nV IN ('CTV021993','CTV028920','CTV041290','CTV022001');
/* -------------- UP USER_IPCC ----------------- */
update ttkd_Bsc.nhanvien set user_ipcc='' where thang = 202412 and donvi = 'VTTP';

update  ttkd_Bsc.nhanvien set user_ipcc=REPLACE(MAIL_VNPT,'@vnpt.vn','') where user_ipcc is null and  thang = 202412 and donvi = 'VTTP';
  ;
commit;
ROLLBACK;
/* ------------------ UP ttkd_Bsc.nhanvien_ID TU FILE NHAN VIEN THANG TRUOC ----------------- */
update  ttkd_Bsc.nhanvien a set (a.nhanvien_id,a.user_dhsxkd)=(select nhanvien_id, user_dhsxkd 
            from ttkd_Bsc.nhanvien where thang = '202411' and donvi = 'VTTP' AND ma_nv=a.ma_nv)
            where thang = 202412 and donvi = 'VTTP';

;
commit ;
rollback;
UPDATE  ttkd_Bsc.nhanvien SET MAnV_HRM = MA_NV where thang = 202412 and donvi = 'VTTP';
select* from ttkd_Bsc.nhanvien where thang = 202412 and donvi = 'VTTP';
;
/* ------------------ UP BO SUNG ttkd_Bsc.nhanvien_ID TU FILE NHAN VIEN TU ONE ----------------- */
update ttkd_Bsc.nhanvien a set a.nhanvien_id=(select nhanvien_id from admin_hcm.nhanvien where ma_nv=a.manv_hrm) 
where a.nhanvien_id is null  and thang = 202412 and donvi = 'VTTP';
 ;
commit;
select* from userld_202405_goc;
-----
insert into userld_202405_new 
select* from userld_202405_goc where user_ld not in (select user_ld from userld_202404_goc);
/* ------------------ UP BO SUNG USER_DHSXKD TU FILE NHAN VIEN TU ONE ----------------- */
update ttkd_Bsc.nhanvien a set a.user_dhsxkd=(select distinct cast(ma_nd as varchar(100)) from admin_hcm.nguoidung
                                                 where nhanvien_id=a.nhanvien_id and rownum = 1) 
where a.nhanvien_id is not null and exists(select 1 from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) and a.user_dhsxkd is null 
and thang = 202412 and donvi = 'VTTP'; thang = 202412 and donvi = 'VTTP';
select* from ttkd_Bsc.nhanvien where nhanvien_id  is null and thang = 202412 and donvi = 'VTTP';
select* from admin.nhanvien@dataguard where ma_Nv='CTV088333';
commit;
select* from ttkd_Bsc.dm_ccos;
-- update user ccos
update ttkd_Bsc.dm_ccos set thang = 202412 where thang is null;
/* ------------------ UP USER CCOS TU ANH KHANG CUNG CAP ----------------- */
update ttkd_Bsc.nhanvien A SET (a.user_ccos,trangthai_ccos)=(select user_ccos, trangthai from ttkd_Bsc.dm_ccos where manv_hrm=a.ma_nv and thang='202412' )
where thang = 202412 and donvi = 'VTTP';
;--where a.user_ccos is null 
;
select* from dm_ccos;
select* from userld_202412_goc;
commit ;
--- 
update ttkd_Bsc.nhanvien A SET a.user_ccos=lower(a.user_ccos) where thang = 202412 and donvi = 'VTTP';;
commit;
/* ------------ UP USER_CCBS ------------- */
update ttkd_Bsc.nhanvien  a SET (a.USER_CCBS,a.tr_thai)=(SELECT USER_CCBS,tr_thai FROM TTKD_bSC.nhanvien WHERE THANG =202411 AND DONVI = a.donvi
    AND MANV_HRM=A.MANV_HRM) 
where a.user_ccbs is null and thang = 202412 and donvi = 'VTTP';;
COMMIT ;
select* from ttkd_Bsc.nhanvien WHERE USER_CCBS IS NULL and thang = 202412 and donvi = 'VTTP';;
update ttkd_Bsc.nhanvien set manv_Hrm = ma_nv;
/* ------------------ UP BO SUNG USER_CCBS TU FILE NHAN VIEN TU ONE ----------------- */
update ttkd_Bsc.nhanvien a set user_ccbs=(select nd.user_neo from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null)
where exists(select 1 from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null) 
and a.user_ccbs is null and thang = 202412 and donvi = 'VTTP';
;
commit ;
update ttkd_Bsc.nhanvien a set USER_CCBS = (Select user_ld from userld_202412_goc where mail_vnpt = email )
where thang = 202412 and donvi ='VTTP' and USER_CCBS is null;
update ttkd_Bsc.nhanvien A SET (A.USER_CCBS,a.tr_thai,a.trangthai_ccos)
                            =(SELECT USER_CCBS,tr_thai,user_ccos 
                            FROM ttkd_Bsc.nhanvien where thang = 202411 and MANV_HRM=A.MANV_HRM and donvi = a.donvi) 
                            where thang = 202412 and donvi = 'VTTP';;
COMMIT ;

update ttkd_Bsc.nhanvien a set a.tr_thai=(select tr_thai from userld_202412_goc b where a.user_ccbs=b.user_ld)     
 where thang = 202412 and donvi = 'VTTP';
select* from ttkd_Bsc
commit;
rollback;
select* from TTKD_bSC.NHANVIEN WHERE THANG = 202412 AND DONVI = 'VTTP';
select manv_hrm, mail_vnpt from ttkd_Bsc.nhanvien where mail_vnpt not like '%@vnpt.vn' ;
delete from ttkd_Bsc.nhanvien where ma_Nv is null;
/* KIEM TRA TRUNG MANV */
select manv_hrm,count(*)sl from ttkd_Bsc.nhanvien  WHERE THANG = 202412 AND DONVI = 'VTTP' group by manv_hrm having count(*) > 1 ;
select ma_pb,ten_pb from ttkd_Bsc.nhanvien group by ma_pb,ten_pb order by ma_pb,ten_pb  ; -- ;
select ma_to,ten_to,ten_pb from ttkd_Bsc.nhanvien group by ma_to,ten_to,ten_pb ; -- ;
select ma_pb,ten_pb from ttkd_Bsc.ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' group by ma_pb,ten_pb order by ma_pb,ten_pb  ; -- ;

select ma_vtcv,ten_vtcv from ttkd_Bsc.ttkd_Bsc.nhanvien_202406 a group by ma_vtcv,ten_vtcv order by ma_vtcv,ten_vtcv ; -- ;
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, ttkd_Bsc.nhanvien_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID 
from ttkd_Bsc.nhanvien_202406_lan1
minus
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, ttkd_Bsc.nhanvien_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from ttkd_Bsc.nhanvien_202406_lan1 ;
select* from ttkd_Bsc.nhanvien where thang = 202412 and donvi = 'VTTP';
select * from ttkd_Bsc.nhanvien_202406_lan2 a;
select* from ttkd_Bsc.nhanvien;
create table nhanvien_vttp as select* from nhanvien_vttp where 1=2;
left join nhanvien_202406_lan1 b on a.ma_nv = b.ma_nv
where MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID;
-- drop table nhanvien_202404 purge ;
rename nhanvien_lan1 to nhanvien ;
drop index nhanvien_donvi;
create table nhanvien_202406 as select * from nhanvien_202406_lan2;
CREATE INDEX nhanvien_202406_manv ON nhanvien_202406 (MA_NV ASC);
CREATE INDEX nhanvien_202406_manvhrm ON nhanvien_202406 (MANV_HRM ASC);
CREATE INDEX nhanvien_202406_userccbs ON nhanvien_202406(user_ccbs ASC);
CREATE INDEX nhanvien_donvi ON nhanvien(MANV_HRM ASC);
commit;
create table dm_to as ;
SELECT * FROM ttkd_bsc.dm_nhomld a ;
SELECT* FROM ttkd_bsc.nhanvien where thang = 202412 and donvi = 'VTTP';
/* NHO CAP NHAT DM_TO */
-- INSERT INTO TTKD_BSC.DM_TO(ma_to,ten_to,ma_pb,ten_pb,hieuluc,pbh_id) 
SELECT A.MA_TO, initcap(A.TEN_TO) ten_to, A.MA_PB, initcap(A.TEN_PB) ten_pb, 1, 29 --, '' TBH_ID, '' KENHBH_ID
FROM ttkd_Bsc.nhanvien a
WHERE thang = 202412 and donvi = 'TTKD' AND NOT EXISTS(SELECT 1 FROM TTKD_bSC.DM_TO B WHERE A.MA_TO=B.MA_TO and hieuluc=1) AND A.MA_TO IS NOT NULL GROUP BY A.MA_TO, A.TEN_TO, A.MA_PB, A.TEN_PB  
; 
select* from DM_TO;
COMMIT ;
update dm_to a set ma_to_hrm=ma_to where hieuluc=1 ;
UPDATE nhanvien SET NHOMLD_ID = 1 ;
update dm_to a set (pbh_id, tbh_id)=(select distinct pbh_id, tbh_id from ttkd_bct.tobanhang where hieuluc=1 and ma_to_hrm=a.ma_to) where hieuluc=1 ;
commit ;

-- INSERT INTO TTKD_BSC.DM_VTCV(ma_vtcv,ten_vtcv) 
SELECT A.MA_VTCV, initcap(A.TEN_VTCV) ten_VTCV
FROM ttkd_Bsc.nhanvien a
WHERE thang = 202412 and donvi = 'TTKD' AND NOT EXISTS (SELECT 1 FROM TTKD_bSC.DM_VTCV B WHERE A.MA_VTCV=B.MA_VTCV) AND A.MA_VTCV IS NOT NULL GROUP BY A.MA_VTCV, A.TEN_VTCV
; 
commit;
SELECT * FROM DM_VTCV ;
select* from nhanvien_vttp where thang = 202404;
select distinct ma_to from nhanvien_202406_lan1

SELECT distinct A.MA_VTCV, initcap(A.TEN_VTCV) ten_VTCV
FROM nhanvien_202404 a
WHERE NOT EXISTS(SELECT 1 FROM nhanvien_202209 B WHERE A.MA_VTCV=B.MA_VTCV) 
; 

-- INSERT INTO TTKD_bSC.DM_PHONGBAN 
SELECT A.MA_PB, initcap(A.TEN_PB) ten_PB, '' ACTIVE,'' MA_PB_PTTB,'' PBH_ID 
FROM ttkd_Bsc.nhanvien a
WHERE thang = 202412 and donvi = 'TTKD' AND NOT EXISTS(SELECT 1 FROM TTKD_bSC.DM_PHONGBAN B WHERE A.MA_PB=B.MA_PB) AND A.MA_PB IS NOT NULL GROUP BY A.MA_PB, A.TEN_PB
; 
commit;


/* ------------------ INSERT FILE NHANVIEN THANG VAO FILE NHANVIEN_TONG ------------------------ */
select distinct thang from nhanvien_tong ;
delete from ttkd_bsc.nhanvien where thang = 202407;
commit;
insert into nhanvien_tong(THANG, MANV_NNL,MA_NV,TEN_NV,MA_VTCV,TEN_VTCV,MA_TO,TEN_TO,MA_PB,TEN_PB,LOAI_LD,USER_CCBS,TR_THAI,USER_CCBS2, MAIL_VNPT,
                          SDT,NGAYSINH,NHANVIEN_ID,GIOITINH,CMND,USER_DHSXKD,MANV_HRM,USER_CCOS,TRANGTHAI_CCOS,USER_IPCC,PHAN_LOAI,LOAI_LAODONG, nhomld_id)

select '202404',MANV_NNL,MA_NV,TEN_NV,MA_VTCV,TEN_VTCV,MA_TO,TEN_TO,MA_PB,TEN_PB,LOAI_LD,USER_CCBS,TR_THAI,USER_CCBS2, MAIL_VNPT,
                SDT,NGAYSINH,NHANVIEN_ID,GIOITINH,CMND,USER_DHSXKD,MANV_HRM,USER_CCOS,TRANGTHAI_CCOS,USER_IPCC,PHAN_LOAI,LOAI_LAODONG, nhomld_id
from nhanvien_202403

;
insert into ttkd_bsc.nhanvien(THANG, DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID)
select 202412 THANG,'TTKD' DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH,
NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from nhanvien;
delete from ttkd_bsc.nhanvien where thang = 202412 and donvi = 'TTKD';
commit;
select* from ttkd_bsc.nhanvien where thang = 202409 and donvi = 'VTTP'; AND TEN_nV LIKE '%Nga';
update ttkd_bsc.nhanvien set ma_vtcv ='VNP-HNHCM_BHKV_1'
where ma_nv = 'HCM004899' and thang = 202407;
SELECT* FROM TTKD_BSC.DM_NHOMLD;
SELECT* FROM ttkd_bsc.NHANVIEN_202403;
UPDATE NHANVIEN_202412 SET NHOMLD_ID = 2 WHERE NHOMLD_ID IS NULL AND MA_nV LIKE 'HCM%';