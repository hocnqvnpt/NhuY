select mg.*,  d.tenphong pbh_t1, e.tenphong pbh_t12 
from mg 
    left join ttkd_bct.db_Thuebao_ttkd b on mg.ma_Tb = b.ma_Tb
    left join ttkd_bct.db_Thuebao_ttkd_202411 c on mg.ma_Tb= c.ma_Tb
    left join ttkd_bct.phongbanhang d on b.mapb_ql = d.ma_pb_hrm
        left join ttkd_bct.phongbanhang e on c.mapb_ql = e.ma_pb_hrm
;
where b.mapb_ql != c.mapb_ql;
select* from ttkd_bct.phongbanhang;;
upda