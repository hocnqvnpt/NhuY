select phong_Khac, count(distinct thuebao_id) 
tong
, count( distinct case when nvl(tien_khop,0) >= 1 then thuebao_id else null end) da_giahan_thanhcong
,  (count( distinct case when nvl(tien_khop,0) >= 1 then thuebao_id else null end))*100 / ( (count(distinct thuebao_id) ) )tyle
--select count(distinct thuebao_id) 
from (
select a.*,b.ma_Nv nv , b.ten_to, b.ten_pb, c.ma_Gd, c.ngay_Tt, c.tien_Thanhtoan, c.vat_Thanhtoan, c.tien_khop , b.ma_pb, c.ma_pb phong_khac
from  ttkd_Bsc.nhuy_ct_ipcc_obghtt a
    left join ttkd_bsc.nhanvien b on a.thang = b.thang and a.ma_Nv = b.ma_nv
    left join ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt c on a.nhanvien_id = c.nvtuvan_id and a.thuebao_id = c.thuebao_id    
where to_char(ngay_ktdc,'yyyymm') = '202408' and b.ma_pb = 'VNP0703000' and c.ma_pb != 'VNP0703000'  --and a.thang > b.thang
)
--where ma_Gd is not null
group by phong_Khac ;
select  ten_pb, count(distinct thuebao_id) tong, count( distinct case when nvl(tien_khop,0) >= 1 then thuebao_id else null end) da_giahan_thanhcong
,  (count( distinct case when nvl(tien_khop,0) >= 1 then thuebao_id else null end))*100 / ( (count(distinct thuebao_id) ) )tyle
from (
select a.*,b.ma_Nv nv , b.ten_to, b.ten_pb, c.ma_Gd, c.ngay_Tt, c.tien_Thanhtoan, c.vat_Thanhtoan, c.tien_khop from  ttkd_Bsc.nhuy_ct_ipcc_obghtt a
    left join ttkd_bsc.nhanvien b on a.thang = b.thang and a.ma_Nv = b.ma_nv
    left join ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt c on a.nhanvien_id = c.nvtuvan_id and a.thuebao_id = c.thuebao_id    
where to_char(ngay_ktdc,'yyyymm') = '202408' and b.ma_pb = 'VNP0703000'  and c.ma_pb = 'VNP0703000'   --and a.thang > b.thang

and not exists (Select 1 from  ttkd_Bsc.nhuy_ct_ipcc_obghtt d
    left join ttkd_bsc.nhanvien e on d.thang = e.thang and d.ma_Nv = e.ma_nv
    left join ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt f on d.nhanvien_id = f.nvtuvan_id and d.thuebao_id = f.thuebao_id    
where to_char(ngay_ktdc,'yyyymm') = '202408'  and d.thuebao_id = a.thuebao_id and e.ma_pb !=  'VNP0703000' )
)
group by ten_pb ;

;
select* from ttkd_bsc.