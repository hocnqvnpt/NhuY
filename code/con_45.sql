update khdn  set ds_lk = (select   RTRIM(XMLAGG(XMLELEMENT(E,B.MA_TB,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATB
    from khdn a join final_kh b on a.thuebao_id != b.thuebao_id and a.khachhang_id = b.khachhang_id
    where  to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm'))
    and khdn.thuebao_id = a.thuebao_id
    group by a.khachhang_id, a.thuebao_id
--    having count (a.thuebao_id) > 1
    )
    select khdn.thuebao_id, ffff.ds from khdn WHERE SLTB_CHUAGHEP_GOIDADV > 0 AND DS_GOI IS Not null
    select* from khdn;
        select* from ffff;

    select khdn.thuebao_id,khdn.SLTB_KHAC_DIACHI, ds from khdn join ffff on khdn.thuebao_id = ffff.thuebao_id
    
    insert into khdn(thang_kt,khdn) values (-10,1)
    rollback;
select khachhang_id ,SLTB_KHAC_DIACHI from khdn where thuebao_id = 1307096;
select* from ffff where thuebao_id = 1307096;
sel

select* from 