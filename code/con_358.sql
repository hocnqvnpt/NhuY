delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202412 and ma_kpi = 'HCM_SL_ORDER_001';
COMMIT;
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ngay_apply is not null;
select* from ttkd_Bsc.ct_bsc_homecombo where thang = 202412 and ma_Tb in ('ducthoi2407','ndv.1','vxh-2');
select* from bangluong_kpi_130125 where thang = 202412  and ma_kpi = 'HCM_TB_GIAHA_024';

select a.ten_nv, b.ten_pb ,a.ten_vtcv, A.GIAO, a.thuchien, b.thuchien
from ttkd_Bsc.bangluong_kpi a 
    join bangluong_kpi_130125 b on a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi
where a.ma_kpi = 'HCM_SL_COMBO_006' and NVL(a.Thuchien,0) != NVL(b.Thuchien,0) AND A.GIAO > A.THUCHIEN and a.giao < b.thuchien
and a.ten_pb != 'Phòng Bán Hàng Khu V?c Nam Sài Gòn';
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_026';

select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_SL_COMBO_006';

with ton as (
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck, thang_kt, thang_bd

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and (ngay_thoai is null or trunc(ngay_thoai)< to_date('20250101','yyyyMMdd'))

and tiengach is null

union all

/*Danh sách rkm_id có thoái c?c t? 1/12/2024*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tien_thoai,0) ton_ck, thang_kt, thang_bd

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and trunc(ngay_thoai)>= to_date('20250101','yyyyMMdd')

and tiengach is null

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch trên form g?ch n? ti?n m?t)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tiengach,0) ton_ck, thang_kt, thang_bd

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and tiengach >=0

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch theo d/s ch? Thanh gui)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(cuoc_tk_lui,0) ton_ck, thang_kt, thang_bd

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241201

and ngay_cn <to_date('20250102','yyyyMMdd')

and cuoc_tk_lui<>0
)
, ghtt as (select  hdtb.thuebao_id, a.ngay_tt, row_number() over (partition by hdtb.thuebao_id order by a.ngay_tt desc) rn
                         from css_hcm.phieutt_hd a
                                            join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                            join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                            join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                                            join gh on hdtb.thuebao_id = gh.thuebao_id --and rnk = 1
--                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                           --             and gh.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')   ----loai taykhi can---
                                  --   and gh.ma_tb like 'nutifood%'
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                    --and
)
select t.*, tt.trangthai_Tb, lh.loaihinh_Tb, x.ngay_tt
from ton t
    left join css.v_db_thuebao@dataguard db on t.thuebao_id = db.thuebao_id
    left join css.trangthai_tb@dataguard tt on db.trangthaitb_id = tt.trangthaitb_id
    left join css.loaihinh_Tb@dataguard lh on db.loaitb_id = lh.loaitb_id
    left join ghtt x on t.thuebao_id = x.thuebao_id and x.rn =1 