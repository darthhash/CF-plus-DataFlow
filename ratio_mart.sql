with 
  voice_charts as ( --count voice inserts by day
    select 
      date(timestamp) day,
      company,
      user,
      patientId,
      count(distinct insertId) inserts

    from 
      logs.backend_logs 
    where 
      msg='session started'
    group by 
      1,2,3,4

  ), 

  manual as ( --count manual inserts by day, voice inserts with same day and user excluded here
    select 
      i.*
    from (
      select 
        date(timestamp) day,
        company,
        user,
        patientId,
        count(distinct insertId) inserts
      from 
        logs.integraion_logs
      group by 
        1,2,3,4
    ) i 
    left join 
      voice_charts
    on i.day = voice_charts.day
    and i.patientId = voice_charts.patientId
    and i.user = voice_charts.user
    where 
      voice_charts.user is null
  ) 

  select 
    union_all.*,
    first_value(day) over (partition by user order by day) first_onboarded_day,
    case when first_value(day) over (partition by user order by day)>'2022-04-01' then 'new_onboarding' else 'old_onboarding' end as onboarding_type
  from ( -- union all data in one mart to calculate Voice Ratio metric
      select 
          'voice' type,
          day,
          company,
          user,
          patientId,
          inserts,
          1 as charts
      from 
        voice_charts

      union all

      select 
          'manual' type,
          day,
          company,
          user,
          patientId,
          inserts,
          1 as charts
      from 
        manual
  ) union_all
