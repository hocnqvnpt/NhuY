update ttkd_bsc.nhanvien set thang = 202407 , donvi = 'VTTP' where thang is null;
select* from ttkd_bsc.nhanvien where thang = 202407; and ma_Vtcv = 'VNP-HNHCM_KHDN_18';
rollback;
delete from nhanvien_202501;
select* from nhanvien_202501;
select* from ttkd_Bsc.dm_Ccos where thang = 202410;
update nhanvien_202501 set LOAI_LD = case when ma_nv like 'CTV%' then 'CTV' 
                                        when ma_nv like 'VNP%' then 'Chính Th?c' 
                                        when ma_nv like 'HCM%' then 'Chính Th?c' else null end
where LOAI_LD is null;
update nhanvien_202501 set tinh_bsc = 0 where tinh_bsc = 1;
update nhanvien_202501 set tinh_bsc = 1 where tinh_bsc is null;
update nhanvien_202501 set thaydoi_Vtcv = 0 where thaydoi_Vtcv is null;

commit;
commit;
    /* -------------- UP LAI TEN PHONG BAN TEN_PB ------------------ */
update nhanvien_202501 a set ten_pb=(select distinct ten_pb from ttkd_bsc.nhanvien where ma_pb=a.ma_pb and thang = 202412 and donvi = 'TTKD');
rollback;
commit ;
select* from nhanvien_202410;
/* ------------ UP NHOMLD_ID ------------- */

update nhanvien_202501 a set nhomld_id = 
    (case  when substr(upper(ma_nv),1,3) like 'VNP' then 1
                when substr(upper(ma_nv),1,3)='CTV' then 3                
                when substr(upper(loai_ld),1,4) like '_LCN' then 4
                end) where nhomld_id is null;
select* from nhanvien_202501 where nhomld_id is null
;
update nhanvien_202501 set nhomld_id = 1 where nhomld_id is null;
commit;


select distinct loai_ld from nhanvien_202410 where nhomld_id is null;
rollback;
commit;
select* from nhanvien_202406_lan1-- where thang = 202404;
        

select* from ttkd_Bsc.dm_nhomld;
commit;
SELECT* FROM nhanvien_202501;
/* --------------- UP CASE CAC FIELDS --------------- */
update nhanvien_202501
SET MANV_NNL=UPPER(trim(MANV_NNL)),MANV_HRM=UPPER(trim(MANV_HRM)),MA_NV=UPPER(trim(MA_NV)),TEN_NV=initcap(trim(TEN_NV)),MA_VTCV=UPPER(trim(MA_VTCV)),
    TEN_VTCV=initcap(trim(TEN_VTCV)),
    MA_TO=UPPER(trim(MA_TO)), TEN_TO=initcap(trim(TEN_TO)), MA_PB=UPPER(trim(MA_PB)), TEN_PB=initcap(trim(TEN_PB)),
    LOAI_LD=TRIM(LOAI_LD),mail_vnpt=trim(mail_vnpt),gioitinh=initcap(trim(gioitinh)),cmnd=trim(cmnd), phan_loai=trim(phan_loai), loai_laodong=trim(loai_laodong),user_ipcc=trim(user_ipcc)
   ;
   COMMIT;
UPDATE nhanvien_202501 set  LOAI_LD = 'DLCN',nhomld_id = 4 WHERE  MA_nV IN ('CTV021993','CTV028920','CTV041290','CTV022001');
/* -------------- UP USER_IPCC ----------------- */
update nhanvien_202501 set user_ipcc='' ;
update  nhanvien_202501 set user_ipcc=REPLACE(MAIL_VNPT,'@vnpt.vn','') where user_ipcc is null  ;
commit;
ROLLBACK;
/* ------------------ UP NHANVIEN_ID TU FILE NHAN VIEN THANG TRUOC ----------------- */
update  nhanvien_202501 a set (a.nhanvien_id,a.user_dhsxkd)=(select nhanvien_id, user_dhsxkd from TTKD_BSC.nhanvien where thang = '202412' and donvi = 'TTKD' AND ma_nv=a.ma_nv)
;
commit ;
update nhanvien_202501 set manv_hrm = ma_Nv ;
rollback;

/* ------------------ UP BO SUNG NHANVIEN_ID TU FILE NHAN VIEN TU ONE ----------------- */
update nhanvien_202501 a set a.nhanvien_id=(select nhanvien_id from admin_hcm.nhanvien where ma_nv=a.manv_hrm) 
where a.nhanvien_id is null ;and exists (select 1 from admin_hcm.nhanvien where ma_nv=a.manv_hrm) ;
commit;
select* from nhanvien_202501 where nhanvien_id is null;
----- user ccbs moi
insert into userld_202405_new 
select* from userld_202405_goc where user_ld not in (select user_ld from userld_202404_goc);
/* ------------------ UP BO SUNG USER_DHSXKD TU FILE NHAN VIEN TU ONE ----------------- */
update nhanvien_202501 a set a.user_dhsxkd=(select distinct cast(ma_nd as varchar(100)) from admin_hcm.nguoidung
                                                 where nhanvien_id=a.nhanvien_id and rownum = 1) 
where a.nhanvien_id is not null and exists(select 1 from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) and a.user_dhsxkd is null 
;
select* from ttkd_bsc.userld_202403_goc;
select* from nhanvien_202501 where user_dhsxkd  is null ;
select* from admin_hcm.nguoidung where nhanvien_id = 497448;
update nhanvien_202501 set user_dhsxkd= 'quantd.hcm' where user_dhsxkd  is null ;
commit;
select* from ttkd_Bsc.dm_ccos where thang is null;
-- update user ccos
update ttkd_Bsc.dm_ccos set thang = 202412 where thang is null;
/* ------------------ UP USER CCOS TU ANH KHANG CUNG CAP ----------------- */
update nhanvien_202501 A SET (a.user_ccos,trangthai_ccos)=(select user_ccos, trangthai from ttkd_Bsc.dm_ccos where manv_hrm=a.ma_nv and thang='202412' )
where a.user_ccos is null 
;
select* from dm_ccos;
select* from userld_202410_goc;
commit ;
--- 
update nhanvien_202501 A SET a.user_ccos=lower(a.user_ccos) ;
select* from nhanvien_202501 WHERE USER_Ccos IS NULL and ma_nv in (select manv_hrm from ttkd_Bsc.dm_ccos where thang = 202412);

commit ;
/* ------------ UP USER_CCBS ------------- */
update nhanvien_202501  a SET (a.USER_CCBS,a.tr_thai)=(SELECT USER_CCBS,tr_thai FROM TTKD_bSC.nhanvien WHERE THANG =202412 AND DONVI = 'TTKD'
    AND MANV_HRM=A.MANV_HRM) 
where a.user_ccbs is null;
COMMIT ;
update nhanvien_202501 set manv_Hrm = ma_nv;
/* ------------------ UP BO SUNG USER_CCBS TU FILE NHAN VIEN TU ONE ----------------- */
update nhanvien_202501 a set user_ccbs=(select nd.user_neo from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null)
where exists(select 1 from admin.v_nguoidung@dataguard nd where a.nhanvien_id=nd.nhanvien_id and nd.user_neo is not null) 
and a.user_ccbs is null
;
commit ;
select* from userld_202412_goc;
update nhanvien_202501 a set user_ccbs= (select user_ld from userld_202412_goc where a.mail_Vnpt = email) where a.user_Ccbs is null;

update nhanvien_202501 A SET (a.tr_thai)=(SELECT tr_thai
                                                        FROM userld_202412_goc  WHERE user_ccbs = user_ld) ;    
commit;                                                        
select* from userld_202410_goc;
update nhanvien_202501 set ten_to = 'T? Giám Sát - T?ng H?p' where ten_to = 'T? Gia?M Sa?T - T?ng H?p';

select distinct loai_ld from nhanvien_202501;
update nhanvien_202412_l3 set LOAI_LD = case when ma_nv like 'CTV%' then 'CTV' 
																				when ma_nv like 'VNP%' then 'Chính Th?c' 
																				when ma_nv like 'HCM%' then 'Chính Th?c' else null end
;
commit;
select* from nhanvien_202412;
--- file cu
update ttkd_Bsc.nhanvien A
set (USER_CCBS, TR_THAI	,USER_CCOS, TRANGTHAI_CCOS,NHANVIEN_ID, USER_DHSXKD)
        = (select USER_CCBS, TR_THAI	,USER_CCOS, TRANGTHAI_CCOS,NHANVIEN_ID, USER_DHSXKD
        from nhanvien_202501 where ma_Nv = a.ma_Nv)
where a.thang = 202412 and donvi = 'TTKD';
COMMIT;
drop table nhanvien_202412_thaydoi;
create table nhanvien_202412_thaydoi as 
SELECT * FROM nhanvien_202501 WHERE MA_nV IN  ('CTV087922',
'CTV087161',
'VNP019980',
'CTV042709',
'VNP017488',
'CTV088837',
'CTV088839',
'CTV088841',
'CTV088833',
'CTV088835',
'CTV088836',
'CTV088864',
'CTV088866',
'CTV088867',
'CTV088925',
'CTV088926',
'CTV088945',
'CTV088944',
'CTV088930',
'CTV088927');

update nhanvien_202501 a set a.tr_thai=(select tr_thai from userld_202411_goc b where a.user_ccbs=b.user_ld) ;
commit;
update nhanvien_202501 set ten_to = initcap(ten_to) , ten_vtcv =  initcap(ten_vtcv), ten_pb = initcap(ten_pb);
select* from nhanvien_202501 where user_ccbs is null;
select manv_hrm, mail_vnpt from nhanvien_202501 where mail_vnpt not like '%@vnpt.vn' ;
delete from nhanvien_202501 where ma_Nv is null;
/* KIEM TRA TRUNG MANV */
select manv_hrm,count(*)sl from nhanvien_202501 group by manv_hrm having count(*) > 1 ;
select ma_pb,ten_pb from nhanvien_202501 group by ma_pb,ten_pb order by ma_pb,ten_pb  ; -- ;
select ma_to,ten_to,ten_pb from nhanvien_202501 group by ma_to,ten_to,ten_pb ; -- ;
select ma_pb,ten_pb from ttkd_Bsc.nhanvien where thang = 202407 and donvi = 'TTKD' group by ma_pb,ten_pb order by ma_pb,ten_pb  ; -- ;
select user_ccos from nhanvien_202501 group by user_Ccos having count(ma_nv) > 1;
select ma_vtcv,ten_vtcv from ttkd_Bsc.nhanvien_202406 a group by ma_vtcv,ten_vtcv order by ma_vtcv,ten_vtcv ; -- ;
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID 
from nhanvien_202406_lan1
minus
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS,  USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from nhanvien_202501_202406_lan1 ;
select* from nhanvien_202501; where thang = 202406 and donvi = 'VTTP';
select * from nhanvien_202406_lan2 a;
select* from nhanvien_202501;
create table nhanvien_vttp as select* from nhanvien_202501_vttp where 1=2;
left join nhanvien_202501_202406_lan1 b on a.ma_nv = b.ma_nv
where MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID;
-- drop table nhanvien_202501_202404 purge ;
rename nhanvien_202501_lan1 to nhanvien_202501 ;
drop index nhanvien_donvi;
create table nhanvien_202501 as select * from nhanvien_202406_lan2;
CREATE INDEX nhanvien_202501_manv ON nhanvien_202501 (MA_NV ASC);
CREATE INDEX nhanvien_202501_manvhrm ON nhanvien_202501 (MANV_HRM ASC);
CREATE INDEX nhanvien_202501_userccbs ON nhanvien_202501(user_ccbs ASC);
CREATE INDEX nhanvien_donvi ON ttkd_Bsc.nhanvien(MANV_HRM ASC);
commit;
create table dm_to as 
SELECT * FROM ttkd_bsc.dm_to a ;
SELECT* FROM nhanvien_202501;
/* NHO CAP NHAT DM_TO */
-- INSERT INTO TTKD_BSC.DM_TO(ma_to,ten_to,ma_pb,ten_pb,hieuluc,pbh_id) 
SELECT A.MA_TO, initcap(A.TEN_TO) ten_to, A.MA_PB, initcap(A.TEN_PB) ten_pb, 1, 29 --, '' TBH_ID, '' KENHBH_ID
FROM ttkd_Bsc.nhanvien a
WHERE thang = 202410 and donvi = 'TTKD' AND NOT EXISTS(SELECT 1 FROM TTKD_bSC.DM_TO B WHERE A.MA_TO=B.MA_TO and hieuluc=1) AND A.MA_TO IS NOT NULL GROUP BY A.MA_TO, A.TEN_TO, A.MA_PB, A.TEN_PB  
; 
select* from DM_TO;
COMMIT ;
update dm_to a set ma_to_hrm=ma_to where hieuluc=1 ;
UPDATE nhanvien_202501 SET NHOMLD_ID = 1 ;
update dm_to a set (pbh_id, tbh_id)=(select distinct pbh_id, tbh_id from ttkd_bct.tobanhang where hieuluc=1 and ma_to_hrm=a.ma_to) where hieuluc=1 ;
commit ;

-- INSERT INTO TTKD_BSC.DM_VTCV(ma_vtcv,ten_vtcv) 
SELECT A.MA_VTCV, initcap(A.TEN_VTCV) ten_VTCV
FROM ttkd_Bsc.nhanvien a
WHERE thang = 202410 and donvi = 'TTKD' AND NOT EXISTS    (SELECT 1 FROM TTKD_bSC.DM_VTCV B WHERE A.MA_VTCV=B.MA_VTCV) AND A.MA_VTCV IS NOT NULL GROUP BY A.MA_VTCV, A.TEN_VTCV
; 
commit;
SELECT * FROM DM_VTCV ;
select* from nhanvien_vttp where thang = 202404;
select distinct ma_to from nhanvien_202501_202406_lan1

SELECT distinct A.MA_VTCV, initcap(A.TEN_VTCV) ten_VTCV
FROM nhanvien_202404 a
WHERE NOT EXISTS(SELECT 1 FROM nhanvien_202209 B WHERE A.MA_VTCV=B.MA_VTCV) 
; 

-- INSERT INTO TTKD_bSC.DM_PHONGBAN 
SELECT A.MA_PB, initcap(A.TEN_PB) ten_PB, '' ACTIVE,'' MA_PB_PTTB,'' PBH_ID 
FROM ttkd_Bsc.nhanvien a
WHERE thang = 202410 and donvi = 'TTKD' AND NOT EXISTS(SELECT 1 FROM TTKD_bSC.DM_PHONGBAN B WHERE A.MA_PB=B.MA_PB) AND A.MA_PB IS NOT NULL GROUP BY A.MA_PB, A.TEN_PB
; 
commit;


/* ------------------ INSERT FILE NHANVIEN THANG VAO FILE NHANVIEN_TONG ------------------------ */
select distinct thang from nhanvien_202501_tong ;
delete from ttkd_bsc.nhanvien where thang = 202410 and donvi = 'TTKD';
commit;
insert into nhanvien_tong(THANG, MANV_NNL,MA_NV,TEN_NV,MA_VTCV,TEN_VTCV,MA_TO,TEN_TO,MA_PB,TEN_PB,LOAI_LD,USER_CCBS,TR_THAI,USER_CCBS2, MAIL_VNPT,
                          SDT,NGAYSINH,NHANVIEN_ID,GIOITINH,CMND,USER_DHSXKD,MANV_HRM,USER_CCOS,TRANGTHAI_CCOS,USER_IPCC,PHAN_LOAI,LOAI_LAODONG, nhomld_id)

select '202404',MANV_NNL,MA_NV,TEN_NV,MA_VTCV,TEN_VTCV,MA_TO,TEN_TO,MA_PB,TEN_PB,LOAI_LD,USER_CCBS,TR_THAI,USER_CCBS2, MAIL_VNPT,
                SDT,NGAYSINH,NHANVIEN_ID,GIOITINH,CMND,USER_DHSXKD,MANV_HRM,USER_CCOS,TRANGTHAI_CCOS,USER_IPCC,PHAN_LOAI,LOAI_LAODONG, nhomld_id
from nhanvien_202403

;
insert into ttkd_bsc.nhanvien(THANG, DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT,
NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID,THAYDOI_vTCV,tinh_bsc)
select 202501 THANG,'TTKD' DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH,
NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID,THAYDOI_vTCV,tinh_bsc
from nhanvien_202501;
select* from ttkd_bsc.nhanvien where thang = 202411 and donvi = 'TTKD';
commit;
rollback;

update ttkd_bsc.nhanvien set LOAI_LD = case when ma_nv like 'CTV%' then 'CTV' 
																				when ma_nv like 'VNP%' then 'Chính Th?c' 
																				when ma_nv like 'HCM%' then 'Chính Th?c' else null end
where thang = 202411 and LOAI_LD is null;
select* from ttkd_bsc.nhanvien where thang = 202410; and donvi = 'VTTP';
select* from haithangdau;
update ttkd_bsc.nhanvien set thaydoi_Vtcv = 0 where thaydoi_vtcv is null and  thang = 202410 and donvi = 'TTKD' ;And ma_nv in (select * from chuavaobsc);
where ma_nv = 'HCM004899' and thang = 202407;
SELECT* FROM TTKD_BSC.DM_NHOMLD;
select* from ttkdhcm_ktnv.ds_chungtu_tratruoc; where ma_Ct = 'VCB32598_20240329';
SELECT* FROM ttkd_bsc.NHANVIEN_202403;
UPDATE NHANVIEN_202410 SET NHOMLD_ID = 2 WHERE NHOMLD_ID IS NULL AND MA_nV LIKE 'HCM%';