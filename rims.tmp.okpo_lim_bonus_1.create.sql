/* ���� ����� ������, ������� �������� ������ �������� ������� ������� ���������� ������ ��� ������ ��������� ��� VIP ��������.
����� ������������ �����, ��� ��������� ��������� ���������� ���������� ������, ������ �� �������� ������� (��������� ���� 1-�� 
������������� �����), ���� �������� (������ ����� ������������ ������� 2 �������� ���������� � ��������� ����� ������ ��������������
����� ����, ��� 50 �� 50), ����������� ��������� ����� ������ ��� ������� �� ������� �� ��������� (������ ����� ������������ � ������ 
�������� �������), ����������� ��������� ����� ���������� ������, ������ �� ��� ��������������� �� ������ ��� ����������� �� 3 ������. 
��������� ����, �.�. �������� �������� � ������ � ���� �-�� �������� � ������ ����� �������� ���������. 
��� �� ����� ������ ������� ���������� ������ ��� �������������� ������� � ����������� ������ ��� ������� �� ����� ���������.
� ��������������� ������ �� ������ ������� ��������� ��� ����������� �������, ��������� ����. �������� ������ ������������ ����� */

/*******************************************************************************************************************************************************
 �����:                     ���������� �.�.
 ����:                      ������������� ������ (����������)
 ��������:                  #############
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ������ ����������:         ############# / ��� DB: Sybase IQ             
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ������� ��� ��������:      tmp.okpo_lim_bonus_1 / tmp.gr_num_free_limit
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ������������� ����������:  ��������� #############
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ����������:                1. ���� ����������    - 11.02.2020
                            2. �������� ��������� - #############
                            3. �������� ��������� - #############
                            4. Jira               - #############
*******************************************************************************************************************************************************/
-- �������� ������ �� ��������� �������
drop table if exists tmp.gr_num_free_limit; commit;

select t1.gr_num,
       round(t1.FREE_LIMIT + t1.NEW_CALC_LIM - 5000,-4) as "FREE_LIMIT",
       round(t1.FREE_LIMIT + t1.NEW_CALC_LIM - 5000,-4) as "LIMIT_REST"
  into tmp.gr_num_free_limit
  from vip.group_limit as t1
 where t1.FREE_LIMIT + t1.NEW_CALC_LIM >= 10000 
;
commit;
create HG index gr_num_HG on tmp.gr_num_free_limit (gr_num);

-- ���� �� ����� �� �������� 
drop table if exists tmp.okpo_down_group_; commit;

select distinct 
       t1.okpo
  into tmp.okpo_down_group_        -- 2020-10-13
  from vip.OfertaVipMain as t1
  join vip.group_limit   as t2 on t2.gr_num = t1.gr_num
 where t2.FREE_LIMIT + NEW_CALC_LIM <= 0
;
commit;
create LF index okpo_LF on tmp.okpo_down_group_ (okpo); 

-- �������� ���� � ������������ ����������
drop table if exists tmp.okpo_lim_bonus_1;commit;

select t1.gr_num,
       t1.okpo,
       t1.LOF_TYPE,
       t1.GLP_TYPE,
       t1.FIN_REIT,
       t1.LVF_Q9PB,
       t1.GLP_Q9PB,
       cast(0 as int) as "NEED_LVF",
       cast(0 as int) as "NEED_GLP",
       cast(0 as int) as "BONUS_LVF",
       cast(0 as int) as "BONUS_GLP"
  into tmp.okpo_lim_bonus_1
  from vip.OfertaVipMain as t1
 where t1.LOF_TYPE in ('LVF','LVZ')
   and t1.FIN_CALC_LVF > t1.LVF_Q9PB
   and okpo not in (select okpo from tmp.okpo_down_group_)
 union 
select gr_num,
       okpo,
       LOF_TYPE,
       GLP_TYPE,
       FIN_REIT,
       LVF_Q9PB,
       GLP_Q9PB,
       cast(0 as int) as "NEED_LVF",
       cast(0 as int) as "NEED_GLP",
       cast(0 as int) as "BONUS_LVF",
       cast(0 as int) as "BONUS_GLP"
  from vip.OfertaVipMain
 where GLP_TYPE in ('GLP','LGS')
   and FIN_CALC_GLP > GLP_Q9PB
   and okpo not in (select okpo from tmp.okpo_down_group_)
;
commit;
create HG index gr_num_HG on tmp.okpo_lim_bonus_1 (gr_num);
create HG index okpo_HG   on tmp.okpo_lim_bonus_1 (okpo);

-- ��������� ���� ����������� �������
update tmp.okpo_lim_bonus_1
   set t1.NEED_LVF = t2.FIN_CALC_LVF - t2.LVF_Q9PB
  from tmp.okpo_lim_bonus_1 as t1
  join vip.OfertaVipMain    as t2 on t2.gr_num = t1.gr_num
                                          and t2.okpo   = t1.okpo 
 where t1.LOF_TYPE in ('LVF','LVZ')
   and t2.FIN_CALC_LVF > t2.LVF_Q9PB
;
commit;

update tmp.okpo_lim_bonus_1
   set t1.NEED_GLP = t2.FIN_CALC_GLP - t2.GLP_Q9PB
  from tmp.okpo_lim_bonus_1 as t1
  join vip.OfertaVipMain    as t2 on t2.gr_num = t1.gr_num
                                          and t2.okpo   = t1.okpo 
 where t1.GLP_TYPE in ('GLP','LGS')
   and t2.FIN_CALC_GLP > t2.GLP_Q9PB
;
commit;

-- �������� ��������, ������� ����� ��������� �� ������������� � ���� ��������������� ���������� ������ ��� ������������ ������ �� LVF
select t1.okpo
  into #okpo_lvf_del
  from tmp.okpo_lim_bonus_1 as t1
  join tmp.gr_num_free_limit                       as t2 on t2.gr_num = t1.gr_num
 where t2.FREE_LIMIT + t1.LVF_Q9PB < 50000
   and t1.NEED_GLP = 0
 union 
select t1.okpo
  from tmp.okpo_lim_bonus_1 as t1
  join tmp.gr_num_free_limit                       as t2 on t2.gr_num = t1.gr_num
 where t2.FREE_LIMIT / 2 + t1.LVF_Q9PB < 50000
   and t1.NEED_GLP > 0
 union 
select t1.okpo
  from tmp.okpo_lim_bonus_1 as t1
  join tmp.gr_num_free_limit                       as t2 on t2.gr_num = t1.gr_num
 where (t2.FREE_LIMIT - t1.NEED_GLP) + t1.LVF_Q9PB < 50000
   and t1.NEED_GLP > 0
;
commit;
create LF index okpo_LF on #okpo_lvf_del(okpo);

  
---------------------------------------------------------------------------------------------------------------------------------------------------------
-- ������������� ������
---------------------------------------------------------------------------------------------------------------------------------------------------------
create variable @RaitStartDown int;
create variable @RaitEndDown   int;

set @RaitEndDown = (select max(FIN_REIT) from tmp.okpo_lim_bonus_1)

set @RaitStartDown = 1;

while(@RaitStartDown <= @RaitEndDown )

begin
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_LVF = case when t2.LIMIT_REST > t1.NEED_LVF 
                           then t1.NEED_LVF
                           else t2.LIMIT_REST
                       end
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown 
   and t1.NEED_GLP = 0
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_GLP = case when t2.LIMIT_REST > t1.NEED_GLP 
                           then t1.NEED_GLP
                           else t2.LIMIT_REST
                       end
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown 
   and t1.NEED_LVF = 0
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_GLP = case when t2.LIMIT_REST > t1.NEED_GLP 
                           then t1.NEED_GLP
                           else t2.LIMIT_REST
                       end
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown
   and t1.okpo in (select okpo from #okpo_lvf_del)
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_LVF = t1.NEED_LVF,
       t1.BONUS_GLP = t1.NEED_GLP
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown
   and t1.NEED_LVF > 0
   and t1.NEED_GLP > 0
   and t2.LIMIT_REST / 2 >= t1.NEED_LVF
   and t2.LIMIT_REST / 2 >= t1.NEED_GLP
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_LVF = t2.LIMIT_REST / 2,
       t1.BONUS_GLP = t2.LIMIT_REST / 2
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown 
   and t1.NEED_LVF > 0
   and t1.NEED_GLP > 0
   and t2.LIMIT_REST / 2 <= t1.NEED_LVF
   and t2.LIMIT_REST / 2 <= t1.NEED_GLP
   and t2.LIMIT_REST / 2 >= 50000
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;

update tmp.okpo_lim_bonus_1
   set t1.BONUS_LVF = 50000,
       t1.BONUS_GLP = t2.LIMIT_REST - 50000
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown 
   and t1.NEED_LVF > 0
   and t1.NEED_GLP > 0
   and t2.LIMIT_REST / 2 <= t1.NEED_LVF
   and t2.LIMIT_REST / 2 <= t1.NEED_GLP
   and t2.LIMIT_REST / 2 < 50000
   and t2.LIMIT_REST > 50000
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;

update tmp.okpo_lim_bonus_1
   set t1.BONUS_GLP = t2.LIMIT_REST
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown 
   and t1.NEED_LVF > 0
   and t1.NEED_GLP > 0
   and t2.LIMIT_REST / 2 <= t1.NEED_LVF
   and t2.LIMIT_REST / 2 <= t1.NEED_GLP
   and t2.LIMIT_REST < 50000
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_LVF = t1.NEED_LVF,
       t1.BONUS_GLP = case when t2.LIMIT_REST / 2 + (t2.LIMIT_REST / 2 - t1.NEED_LVF) > t1.NEED_GLP
                           then t1.NEED_GLP
                           else t2.LIMIT_REST / 2 + (t2.LIMIT_REST / 2 - t1.NEED_LVF)
                       end
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown
   and t1.NEED_LVF > 0
   and t1.NEED_GLP > 0
   and t2.LIMIT_REST / 2 > t1.NEED_LVF
   and t2.LIMIT_REST / 2 < t1.NEED_GLP
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
update tmp.okpo_lim_bonus_1
   set t1.BONUS_LVF = case when t2.LIMIT_REST / 2 + (t2.LIMIT_REST / 2 - t1.NEED_GLP) > t1.NEED_LVF
                           then t1.NEED_LVF
                           else t2.LIMIT_REST / 2 + (t2.LIMIT_REST / 2 - t1.NEED_GLP)
                       end,
       t1.BONUS_GLP = t1.NEED_GLP
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown 
   and t1.NEED_LVF > 0
   and t1.NEED_GLP > 0
   and t2.LIMIT_REST / 2 < t1.NEED_LVF
   and t2.LIMIT_REST / 2 > t1.NEED_GLP
   and t1.okpo not in (select okpo from #okpo_lvf_del)
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
-- ���������� ����������� ������
update tmp.gr_num_free_limit
   set t2.LIMIT_REST = t2.LIMIT_REST - (t1.BONUS_LVF + t1.BONUS_GLP)
  from tmp.okpo_lim_bonus_1  as t1
  join tmp.gr_num_free_limit as t2 on t2.gr_num = t1.gr_num
 where t1.FIN_REIT = @RaitStartDown
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
set @RaitStartDown = @RaitStartDown + 1;

end
;
commit;
---------------------------------------------------------------------------------------------------------------------------------------------------------
-- ����� �������