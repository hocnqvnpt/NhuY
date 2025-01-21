create table kn1 as select MA_TB, KHU_VUC_GOI_MOI, TOCDO_CHUAN, CUOC_SAUKM, GIAGOI_KIENNGHI_1, TOCDO_KIENNGHI_1, TENGOI_KIENNGHI_1
from (
select a.ma_tb, a.KHU_VUC_GOI_MOI, a.tocdo_chuan, a.cuoc_saukm, round(b.GIÁ_GÓI_CHUA_VAT) giagoi_kiennghi_1, b.toc_Do tocdo_kiennghi_1, b.ten_Thuong_hieu tengoi_kiennghi_1, 
    row_number() over ( partition by a.ma_tb order by b.GIÁ_GÓI_CHUA_VAT desc) rn
from db_fiber_le a
    left join goi_moi b on a.KHU_VUC_GOI_MOI = b.khu_vuc_
where a.tocdo_chuan <= b.toc_do and a.cuoc_saukm >= b.GIÁ_GÓI_CHUA_VAT
)
where rn = 1
;
create table kn2 as select MA_TB, KHU_VUC_GOI_MOI, TOCDO_CHUAN, CUOC_SAUKM , GIAGOI_KIENNGHI_2, TOCDO_KIENNGHI_2, TENGOI_KIENNGHI_2
from (
select a.ma_tb, a.KHU_VUC_GOI_MOI, a.tocdo_chuan, a.cuoc_saukm,
round(b.GIÁ_GÓI_CHUA_VAT) giagoi_kiennghi_2, b.toc_Do tocdo_kiennghi_2, b.ten_Thuong_hieu tengoi_kiennghi_2, 
    row_number() over ( partition by a.ma_tb order by b.GIÁ_GÓI_CHUA_VAT) rn
from db_fiber_le a
    left join goi_moi b on a.KHU_VUC_GOI_MOI = b.khu_vuc_
where a.tocdo_chuan <= b.toc_do and a.cuoc_saukm <= b.GIÁ_GÓI_CHUA_VAT
)
where rn = 1
;
select* from css_hcm.hd_khachhang
select* from css_hcm.chuquan where chuquan_id = 264
select chuquan_id from css_Hcm.db_Thuebao a join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and a.thuebao_id in (9035493,9355755)
select* from v_Thongtinkm_all where ma_Tb in ('fvn_truccp416','fvn_tuan6a69')
drop table KN2;
select a.MA_TB, a.KHU_VUC_GOI_MOI, a.TOCDO_CHUAN, a.CUOC_SAUKM, b.GIAGOI_KIENNGHI_1, b.TOCDO_KIENNGHI_1, b.TENGOI_KIENNGHI_1,c.GIAGOI_KIENNGHI_2, c.TOCDO_KIENNGHI_2, c.TENGOI_KIENNGHI_2
from db_fiber_le a
    left join kn1 b on a.ma_tb = b.ma_tb
    left join kn2 c on a.ma_tb = c.ma_tb
where TOCDO_KIENNGHI_1 is null and TOCDO_KIENNGHI_2 is null