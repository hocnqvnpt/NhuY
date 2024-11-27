--- bang giao
drop table sldh_bhol;
UPDATE sldh_bhol SET TIEN_KHOP = 1 WHERE HT_tRA_ID = 208;
SELECT* FROM sldh_bhol WHERE TIEN_KHOP = 0;
COMMIT;
select* from css_Hcm.hinhthuc_Tra where ht_tra_id in(214,216);
create table sldh_bhol as
select 202410 thang, b.thuebao_id, b.ma_tb, a.ngay_yc, c.ngay_Ktdc, nv.ma_nv, nv.ten_nv, nv.ma_to, nv.ma_pb, p.ngay_tt, a.ma_gd, p.ht_Tra_id, p.kenhthu_id, p.trangthai, p.tien, p.vat, 
    kieuld_id, loaihd_id  , case 
                            when p.ht_tra_id in (1, 7,204,214) then 1
                            when p.ht_tra_id in (2, 4,5, 207,216,208,6) then 0 else null end tien_khop
from css_Hcm.hd_khachhang a
    join css_hcm.hd_Thuebao b on a.hdkh_id = b.hdkh_id and b.kieuld_id in (551, 550, 24, 13080) --and b.tthd_id <> 7
    join giao_oneb c on b.thuebao_id = c.thuebao_id and c.thang_kt = 202410
    join admin_hcm.nhanvien_onebss d on a.ctv_id = d.nhanvien_id
    join ttkd_Bsc.nhanvien nv on d.ma_nv = nv.ma_nv and to_number(to_char(a.ngay_yc,'yyyymm'))= nv.thang
    left join css_hcm.phieutt_hd p on a.hdkh_id = p.hdkh_id and p.trangthai = 1
where to_number(to_char(a.ngay_yc,'yyyymm')) between 202408 and 202410 and nv.ma_to ='VNP0703003';
SELECT* FROM TTKD_bSC.BANGLUONG_KPI WHERE  'HCM_SL_BHOL_002' =MA_KPI;
COMMIT;

ROLLBACK;
UPDATE TTKD_bSC.BANGLUONG_KPI SET TYLE_THUCHIEN = 72.84, MUCDO_HOANTHANH = ROUND(0.5*72.84/90,2) WHERE 'HCM_SL_BHOL_002' =MA_KPI AND THANG = 202410;
insert into ttkd_Bsc.tl_giahan_Tratruoc(THANG, MA_PB,MA_TO,MA_NV,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen,  DTHU_THANHCONG_DUNG_HEN, TYLE);
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb,'VNP0703003' MA_TO,'VNP017814' MA_nV,'HCM_SL_BHOL_002' loai_tinh, 'KPI_TO'
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, sum(tien) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from SLDH_BHOL a
                                        where thang = 202410
                                        group by  thang, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb
                               )
                group by thang, ma_pb
                order by 2
                ) a
;

select * from ttkd_Bsc.nhanvien where  donvi = 'TTKD' and ma_to ='VNP0703003';
select* from ttkd_Bsc.nhanvien_vttp_potmasco where thang = 202409;
update ttkd_Bsc.nhanvien set thang = 202410 , donvi = 'POT' where thang is null;
commit;
UPDATE ttkd_bsc.bangluong_Kpi SET CHITIEU_GIAO = NULL where thang = 202410 and ma_kpi ='HCM_SL_HOTRO_006';
insert into ttkd_Bsc.tl_Giahan_Tratruoc(thang, ma_kpi, loai_tinh, ma_Nv,ma_to, ma_pb)
select* from ttkd_bsc.bangluong_Kpi where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_029';
SELECT* FROM TTKD_BSC.BLKPI_DANHMUC_KPI where thang = 202410 and ma_kpi ='HCM_SL_HOTRO_006';
select* from ttkd_Bsc.nhanvien where thang= 202410 and user_Ccbs is null and TEN_nV in (select * from userld_202410_goc);
UPDATE ttkd_Bsc.nhanvien a set user_ccbs =
--select* from ttkd_Bsc.nhanvien a where exists
(select user_ld from userld_202410_goc where  upper(a.ten_Nv) = upper(ten_Daydu)  )--upper(a.ten_Nv) = upper(ten_Daydu) 
where thang = 202410 and user_ccbs is null and donvi != 'POT' AND exists
(select user_ld from userld_202410_goc where  upper(a.ten_Nv) = upper(ten_Daydu)  );
rollback;
commit  ;
