update ttkd_Bsc.nhanvien set thaydoi_vtcv = 1
where thaydoi_vtcv = 0 and thang = 202409 and donvi = 'TTKD' and ma_Nv in (select* from haithangdau);
rollback;
commit;
select m�_nv_hrm from haithangdau group by  m�_nv_hrm having count(m�_nv_hrm) >2;
rollback;
select* from haithangdau where m�_nv_hrm not in (select ma_nv from  ttkd_Bsc.nhanvien 
where thaydoi_vtcv = 1 and thang = 202409 and donvi = 'TTKD');
select* from ttkd_Bsc.nhanvien where ten_nv like '%H?ng Nga';
select* from final_ctu_tien;