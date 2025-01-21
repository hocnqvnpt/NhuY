drop table ttin_ctu;
create table ttin_ctu as 
	select a.chungtu_id,a.ma ma_gd
					,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct,a.tongtra tien_tt
--					,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=a.thungan_tt_id) nhanvien_tt
                    ,(select ma_nv from admin_hcm.nhanvien where nhanvien_id=a.thungan_tt_id) nhanvien_tt
					,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (2)
					    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
									  where chungtu_id=a.chungtu_id and thaotac_id in (2)
									    and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
													    where chungtu_id=a.chungtu_id and thaotac_id in (2)))) nhanvien_cn
--					,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
					,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a where ma='HCM-TT/03071749';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_sub_oneb where ma_Gd='HCM-TT/03071749';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where chungtu_id =445269;

create table ttin_chungtu_t11 as 
select a.* , c.ma_nv manv_tt, d.ma_nv manv_hd, b.ngay_tt from ttin_ctu a 
    left join css_hcm.phieutt_hd b on a.ma_Gd = b.ma_gd
    left join admin_hcm.nhanvien_onebss c on b.thungan_tt_id = c.nhanvien_id
    left join admin_hcm.nhanvien_onebss d on b.thungan_hd_id = d.nhanvien_id
where (c.ma_Nv) = nvl(d.ma_nv,c.ma_Nv) ;

insert into ttkd_Bsc.ct_dongia_Tratruoc (THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, TIEN_DC_CU, MA_TO, MA_PB, MA_TB, MA_NV, KHACHHANG_ID, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, DTHU, 
NGAY_TT, TIEN_KHOP,  NHANVIEN_XUATHD, DONGIA, TYLE_THANHCONG, GHICHU,  TIEN_XUATHD,ipcc)
--create table tmp_Dg_obghtt as;
      select THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, TIEN_DC_CU, MA_TO, MA_PB, MA_TB, MA_NV, KHACHHANG_ID, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  
      manv_Tt NHANVIEN_tt, DONGIA, TYLE_THANHCONG, GHICHU--, manv_Tt, manv_hd, nhanvien_capnhat , thungan_tt_id, thungan_Hd_id
            , case when dongia = 10000 then 10000*nvl(TYLE_THANHCONG,0.4) else 0 end tien_xuathd , ipcc--, tt,ngay_insert
      from 
      (
      select 
       THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv,  KHACHHANG_ID, SOTHANG_DC, 
     HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT,  TIEN_KHOP, ngay_yc, nhanvien_capnhat,dongia, tyle tyle_thanhcong ,manv_Tt,manv_Hd,ipcc,thungan_Tt_id, thungan_hd_id,
     '1' ghichu--, 'bo sung cho 1 thue bao gia han 2 hop dong' ghichu,
                 , row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by dongia desc) rnk, tt,ngay_insert
        from (
        with hs as (select thang, khachhang_id from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                where  xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id ,ipcc--, a.ma_nv, a.ma_to, a.ma_pb
            ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id ,  a.nhomtb_id nhomtb_moi_id, a.ma_to, a.ma_pb--, a.ngay_yc, a.nhanvien_xuathd
                , hs.khachhang_id,sum(cuoc_dc_cu) tien_Dc_Cu, a.ngay_yc, d.ma_nv nhanvien_capnhat, b.nhanvien_cn, b.ngay_tt ngay_gachno,e.thungan_Tt_id, e.thungan_hd_id, 
--                c.ngay_insert,
                b.thuchien, b.manv_Tt, b.manv_hd
        ,case --when tien_khop = 1 and d.ma_Nv is null then 7500
         WHEN tien_khop =1 then 10000
        when tien_khop = 2 then 15000
         when tien_khop = 3 then 12000  
         when tien_khop = 4 then 6000
         else 0
         end dongia 
        ,case 
--         when tien_khop = 1 and to_number(to_char(b.ngay_tt,'yyyymmdd'))  <= to_number(to_char(c.ngay_insert,'yyyymmdd'))+1 then 0.4 
--        when tien_khop = 1 and to_number(to_char(b.ngay_tt,'yyyymmdd'))  > to_number(to_char(c.ngay_insert,'yyyymmdd'))+1 then 0.5 else null end tyle
         when tien_khop = 1 and b.ngay_tt  <= c.ngay_insert+1 then 0.4 
        when tien_khop = 1 and b.ngay_tt  > c.ngay_insert+1 then 0.5 else null end tyle, b.ngay_tt tt, c.ngay_insert
                                    ,max(a.so_Thangdc_MOI) sothang_dc
                                                                  , case
                                            when max(a.so_Thangdc_MOI) >=12 then 1.2
                                            when max(a.so_Thangdc_MOI) < 12 and max(a.so_Thangdc_MOI) >= 6 then 1
                                            when max(a.so_Thangdc_MOI) < 6 and max(a.so_Thangdc_MOI) >= 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)
--
                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(a.ngay_tt) ngay_tt, a.nhomtb_id
                                    , nvl(tien_khop,0) tien_khop
                        from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                                            left join ttin_Chungtu_t11 b on a.ma_Gd = b.ma_gd
                                            left join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c on b.chungtu_id = c.chungtu_id
                                            left join admin_hcm.nhanvien_onebss d on b.nhanvien_cn = d.nhanvien_id
                                            left join css_Hcm.phieutt_hd e on a.phieutt_id = e.phieutt_id
                        where  A.THANG = 202411-- and a.thuebao_id in (select thuebao_id from temp_chot)--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_to, a.ma_pb--, a.ma_nv, a.ma_to, a.ma_pb
                                            , a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id,  a.ngay_yc, a.nhanvien_xuathd, b.nhanvien_cn
                                            , hs.khachhang_id, a.tien_khop, d.ma_nv, b.ngay_tt , c.ngay_insert,b.thuchien, b.manv_Tt, b.manv_hd,ipcc, e.thungan_Tt_id,
                                            e.thungan_hd_id
        ) a
--            join ttkd_bsc.nhanvien_202404 b on a.MANV_THUYETPHUC = b.ma_nv 
            where  dthu > 0 --and not exists (select 1 from ttkd_Bsc.ct_dongia_tratruoc where thang = 202403 and A.THUEBAO_ID = THUEBAO_ID AND A.MANV_THUYETPHUC = MA_NV)
            and not exists (select 1 from ttkd_Bsc.ct_dongia_Tratruoc where thang in  (202410) and tien_khop >0 and thuebao_id = a.thuebao_id and ma_nv = a.MANV_THUYETPHUC and
            loai_tinh = 'DONGIATRA_OB')
        )   where rnk = 1 -- and nvl(manv_Tt ,'a') != nvl(manv_hd,manv_hd) --and nhanvien_capnhat != manv_tt;
       ;
update ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt x
set ma_chungtu = (select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb a
                    join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.chungtu_id = b.chungtu_id
                    where a.ma_Gd = x.ma_gd)
where thang = 202411 and tien_khop = 1;
rollback;
update ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt x
set ma_chungtu = (select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a
                    join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.chungtu_id = b.chungtu_id
                    where a.ma = x.ma_gd)
where thang = 202411 and tien_khop = 1 and ma_chungtu is null;
commit;
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt x where thang = 202411 and tien_khop = 1;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc;
update ttkd_bsc.ct_dongia_tratruoc f set tien_xuathd = 0, ghichu = 'gach no tu dong' , dongia = 7500
--select* from  ttkd_bsc.ct_dongia_tratruoc f

where thang = 202411 and tien_khop = 1 and loai_tinh = 'DONGIATRA_OB' and exists (
select 1-- distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_gd
from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a
    join ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc b on a.ma_Chungtu = b.ma_Ct
where
 a.thang = f.thang and a.thang = 202411 and a.thuebao_id = f.thuebao_id
 and f.ma_Nv = a.manv_thuyetphuc
    and b.tudong = 1
);
commit;
select* from  ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202411;
--- 
-- update 
update ttkd_Bsc.ct_dongia_tratruoc set tien_thuyetphuc = dongia where dongia!= 10000 and loai_tinh = 'DONGIATRA_OB' AND THANG = 202411;
update ttkd_Bsc.ct_dongia_tratruoc set tien_thuyetphuc = (dongia-tien_xuathd) where dongia= 10000 and loai_tinh = 'DONGIATRA_OB' AND THANG = 202411;

-- tong hop don gia
insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, MA_NV, MA_TO, MA_PB, LOAI_TINH, MA_KPI, TIEN)
--select sum(tien) from (
with tien as (
    select thang,ma_Nv,loai_tinh, ma_kpi, ma_Tb, heso_chuky*heso_dichvu*NVL(tien_thuyetphuc,0) tien, ma_pb
    from ttkd_Bsc.ct_dongia_tratruoc 
    where thang = 202411 and loai_tinh = 'DONGIATRA_OB'  --and ghichu = 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
--    and ma_nv = 'VNP017247'
    union all
    select thang,nhanvien_xuathd,loai_tinh, ma_kpi,ma_tb,  heso_chuky*heso_dichvu*NVL(tien_xuathd,0) TIEN,ma_pb
    from ttkd_Bsc.ct_dongia_tratruoc 
    where thang = 202411 and loai_tinh = 'DONGIATRA_OB' --and ghichu= 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
)
select a.thang,a.ma_nv, b.ma_to, b.ma_pb, a.loai_tinh, a.ma_kpi, sum(tien) tien--, ten_pb
from tien a
    join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_nv and a.thang = b.thang
WHERE  b.ma_to not in ('VNP0701505','VNP0702120','VNP0701304','VNP0701104','VNP0701805','VNP0701604','VNP0701205','VNP0701406')
group by  a.thang,a.ma_nv, b.ma_to, b.ma_pb, a.loai_tinh, a.ma_kpi,b.ten_to, b.ten_pb, b.ma_Vtcv, b.ten_vtcv, ten_pb;
--- DOANH THU
insert into ttkd_Bsc.tl_giahan_Tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DTHU_THANHCONG_DUNG_HEN, tien)
select thang, 'DOANHTHU','DONGIA', MA_NV,MA_TO,MA_PB, ROUND(SUM(TIEN)/0.8),  ROUND(SUM(TIEN)/0.8)
from  ttkd_Bsc.tl_giahan_Tratruoc WHERE THANG = 202411 AND LOAI_TINH IN ('DONGIATRA_OB','DONGIA_TS_TP_TT')
GROUP BY THANG,MA_NV,MA_TO,MA_PB;
delete from  ttkd_Bsc.tl_giahan_Tratruoc where thang =202411 and loai_tinh ='DOANHTHU';
commit;
select* from dhsx.v_db_datcoc@dataguard where rownum = 1;