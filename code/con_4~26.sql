with tien as (
    select thang,ma_Nv, tien_dongia
    from ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')   and thang = 202410

)
,sl as (
    select ma_tb, ma_nv,dthu, thang
    from  ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  and ten_nv is not null   and thang = 202410
)
,abc as (
    select ma_nv, count(distinct ma_tb) sltb, sum(dthu) dthu from sl group by ma_nv
) 

select  a.ma_nv,b.ten_nv, b.ten_pb,  abc.sltb, abc.dthu,a.thang,sum(tien_dongia) tien
from tien a
      join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_Nv and a.thang = b.thang and donvi = 'POT'
      left join abc on a.ma_nv = abc.ma_nv
group by a.ma_nv, b.ten_nv, b.ten_pb,abc.sltb, abc.dthu, a.thang
having sum(tien_dongia)>  0;
select* from ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')   and thang = 202410;
    update ttkd_Bsc.dongia_vttp a set ten_Nv=  (select ten_Nv from ttkd_Bsc.nhanvien where thang = 202410 and donvi = 'POT' AND MA_NV=  A.MA_nv)
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')   and thang = 202410;
SELECT MA_tB FROM TTKD_bSC.DONGIA_VTTP WHERE THANG = 202409 AND TIEN_DONGIA > 0 GROUP BY MA_TB, MA_nV HAVING COUNT(MA_tB) > 1;
