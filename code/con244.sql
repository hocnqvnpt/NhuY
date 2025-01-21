update bangluong_dongia_202404 a set
    luong_dongia_ghtt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                                            where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS')
                                                                            and ma_nv = a.MA_NV_HRM
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0)
     ,giamtru_ghtt_cntt = (select -sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                                            where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' and ma_nv = a.MA_NV_HRM
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0)
;
commit;
update  ttkd_Bsc.tl_giahan_tratruoc set thang = 202404 where  ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS') and thang is null;
select a.ma_nv, ten_nv,a.ma_to, ten_to, a.ma_pb, ten_pb, ma_vtcv, ten_vtcv,tien from ttkd_Bsc.tl_giahan_tratruoc a join ttkd_Bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv 
where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS') and a.ma_pb not in ('VNP0702500','VNP0702400','VNP0702300');

select ma_vtcv, ten_vtcv,sum(tien) from ttkd_Bsc.tl_giahan_tratruoc a join ttkd_Bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv 
where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS') and a.ma_pb not in ('VNP0702500','VNP0702400','VNP0702300')
group by ma_vtcv, ten_Vtcv;
select* from bang_gom where thuebao_id not in (select thuebao_id from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt in (202403,202404,202405)) and ngay_bd_moi  > 20240301;

select*from bang_gom where ma_tb in (select ma_tb from ma_tb where nguon = 'nsg');
select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202403 and ma_tb= 'quangtruong11';

select* from bang_gom ;where ma_tb in (select ma_tb from ma_Tb);