create or replace
procedure ddssf(    my_var in varchar2)
as


begin
--    my_var := 'Hello World';
    dbms_output.put_line(my_var);
end;
begin
    ddssf('222');
end;
set serveroutput on;
select* from ttkd_Bsc.nhanvien where thang = 202409 and ten_nv like '%Tuy?t%';
