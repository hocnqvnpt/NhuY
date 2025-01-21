with km as (select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, chitietkm_id,khuyenmai_id
            from v_thongtinkm_all km 
            where (tyle_sd = 100 or tyle_tb = 100) and khuyenmai_id not in (1977, 2056, 2998, 2999) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                --Thay doi thang                                                              
                               and 202410 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
           union all
            ----------------Trong thoi gian datcoc---------------
            select km.ma_tb, km.thuebao_id, km.loaitb_id, thang_bddc, thang_ktdc, km.thang_huy, km.thang_kt_dc, dulieu,chitietkm_id,khuyenmai_id
            from v_thongtinkm_all km
            where --Thay doi thang
                        khuyenmai_id not in (8731, 1977, 2056, 2150) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                        and 202410 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
            union all
            -- uu dai gi do khong biet
            select km.ma_tb, km.thuebao_id, km.loaitb_id, km.thang_bd_mg, km.thang_kt_mg, km.thang_huy, km.thang_kt_dc, dulieu, chitietkm_id,khuyenmai_id
            from v_thongtinkm_all km
            where --Thay doi thang
                        khuyenmai_id  in (9192 ,   10524  ,   11263) and nvl(nhom_datcoc_id,0) not in (8, 9, 11, 22)
                         and 202410 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
)
select a.ma_Tb,  b.ten_Ctkm
from css_hcm.db_Thuebao a
    join km on a.thuebao_id = km.thuebao_id
    join css_Hcm.ct_khuyenmai b on km.chitietkm_id = b.chitietkm_id;
select* from v_thongtinkm_all;