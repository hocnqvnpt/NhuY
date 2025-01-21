 with kv as (
                                    select THUEBAO_ID, knv.NHANVIEN_ID, kvtb.khuvuc_id, ma_kv, nv.donvi_id tovt_id, dv.donvi_cha_id ttvt_id, dv.ten_dv TEN_TO, dv.ten_dvql ten_TTVT
                           from css_hcm.dbtb_kv kvtb
                                                join css_hcm.khuvuc kv on kvtb.khuvuc_id = kv.khuvuc_id and kvtb.loaikv_id = 4
                                                left join css_hcm.khuvuc_nv knv on kv.khuvuc_id = knv.khuvuc_id and knv.loaikv_id = kvtb.loaikv_id and knv.loainv_id=51 and knv.nhiemvu=1
                                                left join admin_hcm.nhanvien_onebss nv on knv.nhanvien_id = nv.nhanvien_id
                                                left join admin_hcm.donvi dv on nv.donvi_id = dv.donvi_id
                            where kvtb.kieukv_id = 2
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
                    , hddc as (select i.hdtb_id, k.rkm_id from css_hcm.hdtb_datcoc i join css_hcm.db_datcoc k on i.thuebao_dc_id = k.thuebao_dc_id
                                where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                    )
                    , pttt as(select i.hdtb_id, k.ht_tra_id, k.kenhthu_id, k.ngay_tt 
                                        from css_hcm.hd_thuebao i join css_hcm.phieutt_hd k on i.hdkh_id = k.hdkh_id
                                            where ht_tra_id <> 6 and trangthai = 1 and kenhthu_id <> 6 and tien > 0
                            )
                    , dc as (select a.*, nvl(kmtb.hdtb_id, hddc.hdtb_id) hdtb_id, pttt.ht_tra_id, pttt.kenhthu_id, pttt.ngay_tt
                                                    , row_number() over (partition by a.thuebao_id, a.nhom_datcoc_id order by a.ngay_kt_mg desc) rnk
                                    from ds_giahan_tratruoc2 a
                                                        left join css_hcm.khuyenmai_dbtb kmtb on a.rkm_id = kmtb.rkm_id
                                                        left join hddc on a.rkm_id = hddc.rkm_id
                                                        left join pttt on nvl(kmtb.hdtb_id, hddc.hdtb_id) = pttt.hdtb_id
                                   where a.loaitb_id in (58, 59, 61) and a.thuebao_id = 12152791
                            )     
       select --count(*) a
                      to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) thang, dc.RKM_ID, dc.THUEBAO_ID, dc.MA_TB, dc.LOAITB_ID, dc.ngay_BDDC, dc.ngay_KTDC, dc.ngay_KT_MG, dc.ngay_HUY, dc.ngay_thoai, dc.TIEN_TD
                      , dc.CUOC_DC, dc.SO_THANGDC, dc.so_thangkm, dc.congvan_id, dc.KHUYENMAI_ID, dc.CHITIETKM_ID, dc.SL_DATCOC, dc.NHOMTB_ID, dc.GOI_ID, dc.ht_tra_id, dc.kenhthu_id
                         ----check ds tbao chung goi da dich vu
                     , (select listagg((select ma_tb from css_hcm.db_thuebao where thuebao_id = pkg1.thuebao_id), '; ') within group (order by pkg1.thuebao_id)
                            from css_hcm.bd_goi_dadv pkg1 where pkg1.trangthai = 1 and pkg1.nhomtb_id = dc.nhomtb_id-- group by pkg1.nhomtb_id
                            ) matb_phu
   ---part 1----
                      , tt.tuyenthu_id, nv_tuyenthu.nvtc_id, nv_tuyenthu.nvqltc_id nvnvqltc_id
                      , dmdc.quan_id, dmdc.phuong_id
                      , kv.khuvuc_id, kv.nhanvien_id nvttvt_id, kv.tovt_id, kv.ttvt_id
                          
                            ----check Fiber va KH CN
                   , case when a.loaitb_id in (58, 59) then 1 else 0 end fiber_donle
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
                                                when a.loaitb_id in (210, 222, 224) then 0.5 - nvl(0.3* (select distinct 1 from ds_giahan_tratruoc2 ax
                                                                                                                                                        where ax.loaitb_id in (58, 59)
                                                                                                                                                                        and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id 
                                                                                                                                                                                                            and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                                                                                                        and ax.khachhang_id = dc.khachhang_id 
                                                                                                                                                        and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm'))), 0)
                                               ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                                when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ds_giahan_tratruoc2 ax
                                                                                                                                                        where ax.loaitb_id in (58, 59)
                                                                                                                                                                        and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id 
                                                                                                                                                                                                            and to_number(to_char(ngay_kt_mg, 'yyyymm')) > to_number(to_char(ax.ngay_kt_mg, 'yyyymm')))
                                                                                                                                                        and ax.khachhang_id = dc.khachhang_id
                                                                                                                                                        and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) =  to_number(to_char(ax.ngay_kt_mg, 'yyyymm'))), 0)
                                            else 0 
                                end Heso
                                , decode(dc.congvan_id, 190, 0, (select count(thuebao_id) from ds_giahan_tratruoc2 ax
                                                                                                        where congvan_id  <> 190 
                                                                                                                        and ax.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                                                                                                                        and not exists (select 1 from ds_giahan_tratruoc2 where thuebao_id = ax.thuebao_id 
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
                left join css_hcm.loai_kh lkh on lkh.loaikh_id = dbkh.loaikh_id
                join dc on a.thuebao_id = dc.thuebao_id                   --thang n
                         
                                            join css_hcm.db_thanhtoan tt on a.thanhtoan_id = tt.thanhtoan_id
                                           left join css_hcm.tuyenthu tut on tt.tuyenthu_id = tut.tuyenthu_id
                                --            left join hocnq_ttkd.nv_tuyenthu nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                                            left join nv_tuyenthu on tut.nhanvien_id = nv_tuyenthu.nvtc_id
                                
                                left join css_hcm.diachi_tb dcld on a.thuebao_id = dcld.thuebao_id
                                left join css_hcm.diachi dmdc on dcld.diachild_id = dmdc.diachi_id
                                left join kv on a.thuebao_id = kv.thuebao_id                                             -----change
                                left join tinhcuoc_hcm.dbkh partition for (20231201) khc on a.khachhang_id = khc.khachhang_id                   ----change
                                
                ---end part 1----                
                              
                where  a.trangthaitb_id not in (7, 8, 9) and dc.rnk = 1 and to_number(to_char(dc.ngay_kt_mg, 'yyyymm')) = 202405
                                    and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.thuebao_id = 12152791
                                   --and not exists (select 1 from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id = dc.thuebao_id and ngay_kt_mg > dc.ngay_kt_mg)
							--and dc.thuebao_id = 12078356;
                            ;
                            select* from v_Thongtinkm_all where ma_Tb  = 'hcmvinh2907app'
                                 