select* from v_Thongtinkm_All ;
select * from css_hcm.db_Thuebao where khachhang_id = 2383117;
select from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where  thang = 202409 and ma_Tb ='ttm-linh23';
select* from ct_Bsc_ghtt_bhol where khachhang_id = 9984514;
select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202409 and ma_Nv = 'VNP016898';

  select thang
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                            left join ttvt_chuyen b on a.thuebao_id = b.thuebao_id and a.phong_giao = b.ten_pbh
                                        where thang = 202409  and nvl(phong_giao,'a') != 'Ðvcq Vi?n Thông H? 1' and b.luonggiao_ttvt is null
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2;
                
update ttkd_Bsc.bangluong_kpi set tyle_thuchien = 67.8, mucdo_hoanthanh = 102.8
--select* from ttkd_Bsc.bangluong_kpi 
where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_022' AND TEN_NV LIKE '%Duyên';
commit;
update rmp_Bsc_phong 
set tong= 26747, da_giahan = 18135 , tyle = 67.8;
select* from rmp_Bsc_phong
where thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T ( D?ch v? Fiber, MyTV, Mesh)' and ma_kpi is null;
select* from rmp_Bsc_phong where thang = 202409 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T ( D?ch v? Fiber, MyTV, Mesh)' and ma_kpi is null;
update ttkd_bsc.ct_dongia_tratruoc set ghichu = ghichu || '. Cot ipcc danh dau danh sach giao cho BHKV tren ID688, 1: giao, 0: 0 giao ;'
 , ipcc = case when (ma_Tb, ma_nv) in (select ma_tb, ma_nv from giao_bhkv) then 1 else 0 end
--select * from ttkd_bsc.ct_dongia_tratruoc
where thang = 202409 and ma_pb in (select ma_pb from ttkd_Bsc.nhanvien where thang = 202409 and ten_pb like '%Khu%') and loai_Tinh = 'DONGIATRA_OB';
rollback;
commit;
select ma_pb from ttkd_Bsc.nhanvien where thang = 202409 and ten_pb like '%Khu%';
select* from giao_bhkv ;
select* from ttkd_Bsc.bangluong_kpi where thang = 202409 and ma_kpi = 'HCM_SL_HOTRO_006';
rollback;
update  ttkd_Bsc.bangluong_kpi set diem_tru = null where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_028'
and (diem_cong is not null or diem_tru is not null) and ma_Vtcv = 'VNP-HNHCM_KHDN_4' and ten_nv != 'Thái Trung Kiên';
select* from ttkd_Bsc.nhanvien where thang = 202409 and ma_nv in ('CTV087779','CTV087789') ;
update  ttkd_Bsc.bangluong_kpi a
set thuchien = thuchien + 1
--select* from ttkd_Bsc.bangluong_kpi a
where thang = 202409 and ma_kpi ='HCM_SL_COMBO_006' and ma_pb = 'VNP0701800' and giao is not null;
rollback;
commit;
update  ttkd_Bsc.bangluong_kpi a set giao = 110 where thang = 202409 and a.ma_Vtcv = 'VNP-HNHCM_BHKV_2' and ma_kpi ='HCM_SL_COMBO_006' --and ma_pb = 'VNP0701400'
and ten_nv like  '%Th?p';