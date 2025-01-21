commit;
      insert into ttkd_bsc.ct_dongia_tratruoc(thang, loai_tinh, ma_kpi, thuebao_id, tien_dc_cu, ma_to, ma_pb, ma_tb, ma_nv,khachhang_id,sothang_Dc,
      heso_Chuky,heso_dichvu,dthu,ngay_tt, tien_khop,ghichu,dongia)
      
      (
        select 
        THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv,  KHACHHANG_ID, SOTHANG_DC, 
     HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT,  TIEN_KHOP, 'bo sung cho 1 thue bao gia han 2 hop dong' ghichu,
                     case  when tien_khop = 1 then 7500
                         WHEN tien_khop =2 then 15000
                         when tien_khop = 3 then 12000
                         when tien_khop = 4 then 6000
                         else 0
                         end dongia 
        from (
        with hs as (select thang, khachhang_id from TTKD_BSC.nhuy_ct_bsc_ipcc_obghtt xu
                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id --, a.ma_nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , hs.khachhang_id,sum(cuoc_dc_cu) tien_Dc_Cu
--                          
                                    ,max(a.so_Thangdc_MOI) sothang_dc
--                                     ,sum(case 
--                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
--                                            else 0 end) ht_tra_online
--                                    
--                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
--                                                        else 0 end) kenhthu_tainha
--                                    
                                    , case
                                            when max(a.so_Thangdc_MOI) >=12 then 1.2
                                            when max(a.so_Thangdc_MOI) < 12 and max(a.so_Thangdc_MOI) >= 6 then 1
                                            when max(a.so_Thangdc_MOI) < 6 and max(a.so_Thangdc_MOI) > 3 then 0.9
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
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.nhomtb_id
                                    , nvl(tien_khop,0) tien_khop, row_number() over (partition by a.thuebao_id order by max(nvl(tien_khop,0))) rnk
                        from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                        where  a.rkm_id is not null AND A.THANG = 202404--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id --, a.ma_nv, a.ma_to, a.ma_pb
                                            , a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id, a.tien_khop
        ) a
            join ttkd_bsc.nhanvien_202404 b on a.MANV_THUYETPHUC = b.ma_nv 
        where  dthu > 0 and not exists (select 1 from ttkd_Bsc.ct_dongia_tratruoc where thang =  202404 AND A.THUEBAO_ID = THUEBAO_ID AND A.MANV_THUYETPHUC = MA_NV)
        and not exists (select 1 from ttkd_Bsc.ct_dongia_Tratruoc where thang in  (202403,202402) and tien_khop = 1 and thuebao_id = a.thuebao_id)
        ) 
       ;
       hcm_mnhhng_25
hcmanhkhoa371
nhminh1023
hcmgnnhattuee9;
phhkhoa
dam30-10
hcmtuoccg
nvs99;
COMMIT;
select* from ct_dongia_temp;-- where ma_tb= 'sonha1021601';
       INSERT INTO ct_dongia_temp 
       with ttin as (
        select thuebao_id, manv_thphuc, ma_tb,phieutt_id, nvl(ngay_tt, ngay_nganhang) ngay_tt, row_number() over (partition by thuebao_id,manv_thphuc order by NGAY_TT) rn
        from ttkd_Bsc.CT_BSC_TRATRUOC_MOI_30DAY
        where rkm_id is not null and thang = 202404
       ) 
        select 
        a.THANG, a.LOAI_TINH, a.MA_KPI, a.MA_NV,a.MA_TO, a.MA_PB, a.PHONG_QL, a.THUEBAO_ID, a.MA_TB, a.TIEN_DC_CU, a.MANV_GIAO, a.MA_NV_CN, a.MANV_THUYETPHUC, a.SOTHANG_DC, a.HT_TRA_ONLINE,
        a.DONGIA, a.DTHU, b.NGAY_TT, a.HESO_GIAO, a.KHDN, a.NHOMTB_ID, a.KHACHHANG_ID, a.HESO_DICHVU, a.TIEN_KHOP, a.GHICHU, a.TYLE_THANHCONG, a.HESO_CHUKY, round(dongia*heso_chuky*heso_dichvu) tien,
        C.hdkh_id, b.phieutt_id , c.ngay_yc, e.ma_nv nhanvien_xuat_hd,
        case when to_number(to_char(b.ngay_Tt,'yyyymmdd')) >= to_number(to_char(ngay_yc,'yyyymmdd'))  then 1 else 0 end hoply
        ,case when to_number(to_char(b.ngay_Tt,'yyyymmdd')) >= to_number(to_char(ngay_yc,'yyyymmdd')) 
                  and a.ma_nv= nvl(e.ma_Nv,a.ma_nv) then round(dongia*heso_chuky*heso_dichvu) 
            when to_number(to_char(b.ngay_Tt,'yyyymmdd')) >= to_number(to_char(ngay_yc,'yyyymmdd')) 
                  and a.ma_nv != nvl(e.ma_Nv,'') then round(0.7*dongia*heso_chuky*heso_dichvu) 
            when to_number(to_char(b.ngay_Tt,'yyyymmdd')) < to_number(to_char(ngay_yc,'yyyymmdd'))
                    then round(0.3*dongia*heso_chuky*heso_dichvu) 
                    end tien_thuyetphuc
        , case when to_number(to_char(b.ngay_Tt,'yyyymmdd')) >= to_number(to_char(ngay_yc,'yyyymmdd')) 
                  and a.ma_nv != e.ma_Nv then round(0.3*dongia*heso_chuky*heso_dichvu) 
                else 0 end tien_xuathd
--            ,case when nvl(a.ngay_tt,0
        from ttkd_bsc.ct_dongia_tratruoc a
            left join ttin b on a.ma_nv = b.MANV_THPHUC and a.thuebao_id = b.thuebao_id  and rn = 1--and a.thang = b.thang
            left join css_hcm.phieutt_hd d on b.phieutt_id = d.phieutt_id
            left join css_hcm.hd_khachhang c on D.hdkh_id = C.hdkh_id
            left join admin_hcm.nhanvien_onebss e on d.thungan_hd_id = e.nhanvien_id
        where a.thang = 202404 and loai_tinh in  ('DONGIA_VTTP') and tien_khop > 0  ; --a and ma_nv = 'CTV081976';
        ;
        COMMIT;
        SELECT* from ct_dongia_temp ;WHERE THUEBAO_ID IN (2806446,8552030,8834841) AND LOAI_TINH = 'DONGIA_TS_TP_TT'--GROUP BY THUEBAO_ID, MA_NV HAVING COUNT(THUEBAO_ID) > 1;
CREATE TABLE CT_TIEN_TP_TEMP AS ;
SELECT SUM(tien_xp) FROM (
SELECT a.* , b.da_giahan sltb_Nhap, b.tien_xp , b.sltb_quydoi sltb_quydoi_nhap 
FROM CT_TIEN_TP_TEMP a
    left join CT_tIEN_XP b on a.ma_nv = b.NHANVIEN_XUAT_HD and a.loai_tinh = b.loai_tinh and a.ma_kpi = b.ma_kpi 
    where a.loai_tinh in  ('DONGIA_VTTP','DONGIA_OB_VTTP')
union all
--SELECT SUM(tien_XP) FROM CT_tIEN_XP WHERE loai_tinh in  ('DONGIA_VTTP','DONGIA_OB_VTTP') AND NHANVIEN_XUAT_HD IS NOT NULL;
select a.thang, a.loai_tinh ,a.ma_kpi, a.NHANVIEN_XUAT_HD, null,null, null,null,null,da_giahan,tien_xp,sltb_quydoi 
from CT_tIEN_XP a where NHANVIEN_XUAT_HD not in (select ma_nv from CT_TIEN_TP_TEMP where loai_tinh in    ('DONGIA_VTTP','DONGIA_OB_VTTP')   )
and loai_tinh in   ('DONGIA_VTTP','DONGIA_OB_VTTP') ) WHERE MA_NV NOT IN (SELECT NHANVIEN_XUAT_HD FROM CT_tIEN_XP);

SELECT* FROM CT_tIEN_XP A where a.loai_tinh in ('DONGIA_VTTP','DONGIA_OB_VTTP') AND NHANVIEN_XUAT_HD IN (SELECT MA_nV FROM CT_TIEN_TP_TEMP where loai_tinh in    ('DONGIA_VTTP','DONGIA_OB_VTTP') );

CREATE TABLE CT_tIEN_XP AS
SELECT SUM(TIEN_XP) FROM (
SELECT thang, loai_tinh, ma_kpi,  NHANVIEN_XUAT_HD
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan--, null
                      , round(sum(TIEN_XUATHD)) tien_XP
                      , ROUND(SUM(HESO_CHUKY*HESO_DICHVU)) SLTB_QUYDOI
                
-- select * from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
--from TTKD_BSC.ct_dongia_tratruoc  a
from ct_dongia_temp  a
where ma_kpi = 'DONGIA' AND NHANVIEN_XUAT_HD IS NOT NULL AND  loai_tinh in   ('DONGIA_VTTP','DONGIA_OB_VTTP') --and loaitb_id in (58,59)
--            and loai_tinh in ('DONGIATRA_OB') and thang = 202404
group by thang, loai_tinh, ma_kpi, NHANVIEN_XUAT_HD ) --, MA_TO, MA_PB;
COMMIT;