update xxxx
set sltb_lechky = (select count(distinct thang_kt)
                        from khdn
                        where xxxx.thang_kt != khdn.thang_kt and xxxx.khachhang_id = khdn.khachhang_id 
                        and xxxx.thuebao_id != khdn.thuebao_id);
                        commit;
               select* from dumb         
update dumb a
set ds2 = (select LISTAGG( distinct b.ma_Tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id)
                        from dumb b
                        where (nvl(a.quan_id,0) =nvl(b.quan_id,0) and  nvl(a.tinh_id,0)= nvl(b.tinh_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0))
                        and a.khachhang_id = b.khachhang_id  and  b.ma_tt!= a.ma_Tt-- is null 
            group by nvl(quan_id,0), nvl(tinh_id,0), nvl(phuong_id,0)) where  SLTB_khac_matt > 0 and ds2 is null ;
                        commit;
                        select* from dumb where khachhang_id = 9795170 and tinh_id = 28 and quan_id = 3992 and phuong_id = 54067
select khachhang_id, SLTB_khac_matt, ds2 from dumb-- where SLTB_khac_matt > 0 and ds2 is null ;
select* from dumb;
select* from ttkd_b
drop table xxxx;
alter table 
create table dumb as
select a.*, row_number() over (partition by nvl(quan_id,0), nvl(tinh_id,0), nvl(phuong_id,0), khachhang_id order by thuebao_id)  rnk_diachi
from xxxx a,
commit;
update dumb a set sltb_khac_diachi = (select count(b.rnk_diachi)
                        from dumb b
                        where (nvl(a.quan_id,0) !=nvl(b.quan_id,0) or  nvl(a.tinh_id,0)!= nvl(b.tinh_id,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id,0))
                        and a.khachhang_id = b.khachhang_id  and b.rnk_diachi = 1);
update dumb a set ds3 = (select LISTAGG(b. ma_Tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id)
                        from dumb b
                        where (nvl(a.quan_id,0) !=nvl(b.quan_id,0) or  nvl(a.tinh_id,0)!= nvl(b.tinh_id,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id,0))
                        and a.khachhang_id = b.khachhang_id  and b.rnk_diachi = 1
                        );                       