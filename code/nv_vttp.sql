update ttkd_bsc.nhanvien set thang = 202407 , donvi = 'VTTP' where thang is null;
SELECT* FROM TTKD_bSC.NHANVIEN WHERE thang = 202407 AND donvi = 'VTTP';
update ttkd_bsc.nhanvien set NHOMLD_ID = 11 WHERE donvi = 'VTTP' AND MA_NV LIKE 'CTV%' AND NHOMLD_ID != 2;
COMMIT;
select * from ttkd_bsc.nhanvien where thang = 202407 and ma_Nv = 'HCM013438';
SELECT* FROM TTKD_BSC.DM_NHOMLD;
update  ttkd_bsc.nhanvien set ma_vtcv = 'VNP-HNHCM_KHDN_3', ten_vtcv = 'Chuyên viên kinh doanh (AM)' where thang = 202407 and ma_Vtcv = 'VNP-HNHCM_KHDN_18';
rollback;
    /* -------------- UP LAI TEN PHONG BAN TEN_PB ------------------ */
update nhanvien_202407_l2 a set ten_pb=(select distinct ten_pb from ttkd_bsc.nhanvien_202406 where ma_pb=a.ma_pb);
commit ;
/* ------------ UP NHOMLD_ID ------------- */
update nhanvien_202407_l2 a set loai_ld=loai_laodong;
update ttkd_bsc.nhanvien a set nhomld_id = 
    (case  when substr(upper(loai_ld),1,3) like 'CH_' then 1
                when substr(upper(loai_ld),1,3)='CTV' then 3                
                when substr(upper(loai_ld),1,4) like '_LCN' then 4
                end) where thang = 202407 and donvi = 'VTTP';
                select* from ttkd_Bsc.dm_nhomld;
                select owner , table_name from all_tab_columns where column_name = upper('nhomld_id');

select distinct loai_ld from nhanvien_202407_l2 where nhomld_id is null;
rollback;
commit;
select* from nhanvien_202406_lan1-- where thang = 202404;
        
update nhanvien_vttp set thang = '202404';
update nhanvien_vttp a set nhomld_id=
    (case  when substr(upper(loai_ld),1,3) like 'CH_' then 2
                when substr(upper(loai_ld),1,3)='CTV' then 3                
                end)
where thang='20240';
;
select* from ttkd_Bsc.dm_nhomld;
commit;
rollback;
SELECT* FROM nhanvien_202407_l2;
/* --------------- UP CASE CAC FIELDS --------------- */
update ttkd_bsc.nhanvien
SET MANV_NNL=UPPER(trim(MANV_NNL)),MANV_HRM=UPPER(trim(MANV_HRM)),MA_NV=UPPER(trim(MA_NV)),TEN_NV=initcap(trim(TEN_NV)),MA_VTCV=UPPER(trim(MA_VTCV)), TEN_VTCV=initcap(trim(TEN_VTCV)),
    MA_TO=UPPER(trim(MA_TO)), TEN_TO=initcap(trim(TEN_TO)), MA_PB=UPPER(trim(MA_PB)), TEN_PB=initcap(trim(TEN_PB)),
    LOAI_LD=TRIM(LOAI_LD),mail_vnpt=trim(mail_vnpt),gioitinh=initcap(trim(gioitinh)),cmnd=trim(cmnd), phan_loai=trim(phan_loai), loai_laodong=trim(loai_laodong),user_ipcc=trim(user_ipcc)
   where thang = 202407 and donvi = 'VTTP';
   COMMIT;
UPDATE ttkd_bsc.nhanvien set  LOAI_LD = 'DLCN',nhomld_id = 4 WHERE THANG = 202407 AND MA_nV IN ('CTV021993','CTV028920','CTV041290','CTV022001');
/* -------------- UP USER_IPCC ----------------- */
update ttkd_bsc.nhanvien set user_ipcc='' where thang = 202407 and donvi = 'VTTP';;
update  ttkd_bsc.nhanvien set user_ipcc=REPLACE(MAIL_VNPT,'@vnpt.vn','') where user_ipcc is null  and thang = 202407 and donvi = 'VTTP';;
commit;
ROLLBACK;
/* ------------------ UP NHANVIEN_ID TU FILE NHAN VIEN THANG TRUOC ----------------- */
update  ttkd_bsc.nhanvien a set (a.nhanvien_id,a.user_dhsxkd)=(select nhanvien_id, user_dhsxkd from ttkd_bsc.nhanvien where thang = 202406 and donvi='VTTP' and ma_nv=a.ma_nv) 
where thang = 202407 and donvi = 'VTTP';
;
commit ;
rollback;

/* ------------------ UP BO SUNG NHANVIEN_ID TU FILE NHAN VIEN TU ONE ----------------- */
update ttkd_bsc.nhanvien a set a.nhanvien_id=(select nhanvien_id from admin_hcm.nhanvien where ma_nv=a.manv_hrm) 
where a.nhanvien_id is null and exists(select 1 from admin_hcm.nhanvien where ma_nv=a.manv_hrm) AND thang = 202407 and donvi = 'VTTP';;
commit;
select* from userld_202405_goc;
-----
insert into userld_202405_new 
select* from userld_202405_goc where user_ld not in (select user_ld from userld_202404_goc);
/* ------------------ UP BO SUNG USER_DHSXKD TU FILE NHAN VIEN TU ONE ----------------- */
update ttkd_bsc.nhanvien a set a.user_dhsxkd=(select distinct cast(ma_nd as varchar(100)) from admin_hcm.nguoidung
                                                 where nhanvien_id=a.nhanvien_id) 
where a.nhanvien_id is not null and exists(select 1 from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) and a.user_dhsxkd is null 
AND thang = 202407 and donvi = 'VTTP';
;
update ttkd_Bsc.nhanvien A set user_ccbs = (Select user_ccbs from  ttkd_Bsc.nhanvien where a.ma_Nv = ma_nv and thang = 202406 and donvi = 'VTTP') WHERE THANG = 202407 AND DONVI = 'VTTP';
select* from ttkd_bsc.userld_202403_goc;
select* from ttkd_bsc.nhanvien_202406_lan1 where user_dhsxkd  is null ;
commit;
select* from ttkd_Bsc.dm_ccos;
-- update user ccos
update dm_ccos set thang = 202407 where thang is null;
/* ------------------ UP USER CCOS TU ANH KHANG CUNG CAP ----------------- */
update ttkd_bsc.nhanvien A SET (a.user_ccos,trangthai_ccos)=(select user_ccos, trangthai from dm_ccos where manv_hrm=a.ma_nv and thang='202407' )where thang = 202407 and donvi = 'VTTP';
;--where a.user_ccos is null 
;
select* from dm_ccos;
commit ;
rollback;
update nhanvien_202406_lan2 A SET a.user_ccos=lower(a.user_ccos) ;
commit ;
/* ------------ UP USER_CCBS ------------- */
update ttkd_bsc.nhanvien  a SET (a.USER_CCBS,a.tr_thai)=(SELECT USER_CCBS,tr_thai FROM ttkd_bsc.nhanvien_202406 WHERE MANV_HRM=A.MANV_HRM) where a.user_ccbs is null
and thang = 202407 and donvi = 'VTTP';;
COMMIT ;
select* from ttkd_bsc.nhanvien;
update ttkd_bsc.nhanvien set manv_Hrm = ma_nv;
/* ------------------ UP BO SUNG USER_CCBS TU FILE NHAN VIEN TU ONE ----------------- */
update ttkd_bsc.nhanvien a set user_ccbs=(select nd.user_neo from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null)
where exists(select 1 from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null) 
and a.user_ccbs is null  and thang = 202407 and donvi = 'VTTP';
;
commit ;

update ttkd_bsc.nhanvien A SET (A.USER_CCBS,a.tr_thai,a.user_ccos,a.trangthai_ccos)=(SELECT USER_CCBS,tr_thai,user_ccos,trangthai_ccos 
                                                                                FROM ttkd_bsc.nhanvien_202406 WHERE MANV_HRM=A.MANV_HRM) 
where thang = 202407 and donvi = 'VTTP';                                                                                ;
COMMIT ;

update ttkd_bsc.nhanvien a set a.tr_thai=(select distinct tr_thai from userld_202407_goc b where a.user_ccbs=b.user_ld) where thang = 202407 and donvi = 'VTTP';                                                                                ;
select* from ttkd_bsc.nhanvien where  thang = 202407 and donvi = 'VTTP';   
update ttkd_bsc.nhanvien set manv_hrm = ma_Nv where manv_hrm is null;
commit;
select* from userld_202403_goc;
select manv_hrm, mail_vnpt from ttkd_bsc.nhanvien where mail_vnpt not like '%@vnpt.vn' ;

/* KIEM TRA TRUNG MANV */
select manv_hrm,count(*)sl from ttkd_bsc.nhanvien group by manv_hrm having count(*) > 1 ;
select ma_pb,ten_pb from ttkd_bsc.nhanvien group by ma_pb,ten_pb order by ma_pb,ten_pb  ; -- ;
select ma_to,ten_to,ten_pb from ttkd_bsc.nhanvien group by ma_to,ten_to,ten_pb ; -- ;

select ma_vtcv,ten_vtcv from ttkd_Bsc.nhanvien_202406 a group by ma_vtcv,ten_vtcv order by ma_vtcv,ten_vtcv ; -- ;
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID 
from nhanvien_202406_lan1
minus
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from ttkd_bsc.nhanvien_202406_lan1 ;
select* from ttkd_bsc.nhanvien where thang = 202406 and donvi = 'VTTP';
select * from nhanvien_202406_lan2 a;
select* from ttkd_bsc.nhanvien;
create table nhanvien_vttp as select* from ttkd_bsc.nhanvien_vttp where 1=2;
left join ttkd_bsc.nhanvien_202406_lan1 b on a.ma_nv = b.ma_nv
where MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID;
-- drop table ttkd_bsc.nhanvien_202404 purge ;
rename ttkd_bsc.nhanvien_lan1 to ttkd_bsc.nhanvien ;
drop index nhanvien_donvi;
create table nhanvien_202406 as select * from nhanvien_202406_lan2;
CREATE INDEX nhanvien_202406_manv ON nhanvien_202406 (MA_NV ASC);
CREATE INDEX nhanvien_202406_manvhrm ON nhanvien_202406 (MANV_HRM ASC);
CREATE INDEX nhanvien_202406_userccbs ON nhanvien_202406(user_ccbs ASC);
CREATE INDEX nhanvien_donvi ON nhanvien(MANV_HRM ASC);
commit;
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
FROM nhanvien_202406 a
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
SELECT A.MA_PB, initcap(A.TEN_PB) ten_PB, '' ACTIVE,'' MA_PB_PTTB,'' PBH_ID FROM ttkd_bsc.nhanvien_202406 a
WHERE NOT EXISTS(SELECT 1 FROM DM_PHONGBAN B WHERE A.MA_PB=B.MA_PB) AND A.MA_PB IS NOT NULL GROUP BY A.MA_PB, A.TEN_PB
; 
commit;


/* ------------------ INSERT FILE NHANVIEN THANG VAO FILE NHANVIEN_TONG ------------------------ */
select distinct thang from ttkd_bsc.nhanvien_tong ;
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
select 202407 THANG,'TTKD' DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH,
NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from ttkd_bsc.nhanvien;
commit;
select* from ttkd_bsc.nhanvien where thang = 202407;
update ttkd_bsc.nhanvien set ma_vtcv ='VNP-HNHCM_BHKV_1'
where ma_nv = 'HCM004899' and thang = 202407;
