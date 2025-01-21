select* from css_Hcm.trangthai_Tb 
;
select ma_nv, ten_nv, sdt,ten_pb , ma_pb from ttkd_bsc.nhanvien where thang = 202407 and ten_nv like '%Ti?n';
select  a.* from ttkd_bsc.blkpi_danhmuc_sms a;where rownum = 1;
rollback;
update ttkd_bsc.blkpi_danhmuc_sms set ma_Nv = 'VNP017779',  sdt = '0947212636', ten_Nv ='Bùi Th? Kim Ti?n' where ma_pb= 'VNP0700900';
commit;
insert into ttkd_bsc.blkpi_danhmuc_sms (ma_nv, ten_nv, sdt,ten_pb , ma_pb) values ('VNP017779',	'Bùi Th? Kim Ti?n','0947212636','Phòng Nghi?p V? C??c','VNP0700900');
select distinct  MA_KPI, TEN_KPI,  MA_PB, TEN_PB, sms_content
from ttkd_bsc.v_blkpi_danhmuc a
where thang = 202406l
;
DELETE FROM ttkd_bsc.blkpi_danhmuc_sms
WHERE rowid not in
(SELECT MIN(rowid)
FROM ttkd_bsc.blkpi_danhmuc_sms
GROUP BY MA_NV, TEN_NV, SDT, MA_PB, TEN_PB);
