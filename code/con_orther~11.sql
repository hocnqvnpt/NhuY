create or replace FUNCTION CHECK_TELCONAME_NEW (SDT IN VARCHAR2) RETURN VARCHAR2 IS   --1: vnpt, 0: dnk

     TYPE T_Cursor IS REF CURSOR;
     m_cursorbuf       T_Cursor;
     vsdt        VARCHAR2(158);
     v_telco         VARCHAR2(158);
    ssql varchar2(4000);
    vsothuebao  VARCHAR2(158);
    vmangdich  VARCHAR2(158);
 --   v_file VARCHAR2(10) := case when to_char(sysdate,'dd')>= '01' then to_char(trunc(ADD_MONTHS(sysdate, 0),'MONTH'),'yyyymmdd') else to_char(trunc(ADD_MONTHS(sysdate, -1),'MONTH'),'yyyymmdd') end;

BEGIN
--VTL DD, I-TELECOM, HT
   if substr(SDT,1,1)='0' then
        vsdt:='84'||substr(sdt,2);
    elsif substr(SDT,1,1)<>'0' and LENGTH(sdt)=9 then
        vsdt:='84'||sdt;
    else
        vsdt:=sdt;
    end if;
    open m_cursorbuf for
    select a.telco  from ttkdhcm_ktnv.brand_mnp a where sodt=vsdt;
    fetch m_cursorbuf into v_telco;

    if m_cursorbuf%NOTFOUND then
        open m_cursorbuf for
        select mang
        from ttkdhcm_ktnv.dauSoQTAN
        where substr(vsdt,1,length(dauso1))=dauso1
        AND length(vsdt)=chieudai+1
        and convert=1;
        fetch m_cursorbuf into v_telco;

        if m_cursorbuf%NOTFOUND then
           v_telco:=null;
        end if;
    end if;
  if m_cursorbuf%isopen then Close m_cursorbuf; end if;
  if v_telco='VTL DD' then
    v_telco:='VTL';
  elsif v_telco='I-TELECOM' then
    v_telco:='ITEL';
  elsif v_telco='HT' then
    v_telco:='VNM';
  end if;

    return v_telco;
END;
