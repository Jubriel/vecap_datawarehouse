
import pandas as pd
import random as rd
from datetime import date, datetime

# # date dimension
# def date_dim(sch:str, target):    
#     # write the SQL query inside the text() block
#     sql = text(f'''INSERT INTO {sch}.date_dim
#             SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
#                 datum AS date_actual,
#                 EXTRACT(EPOCH FROM datum) AS epoch,
#                 TO_CHAR(datum, 'fmDDth') AS day_suffix,
#                 TO_CHAR(datum, 'TMDay') AS day_name,
#                 EXTRACT(ISODOW FROM datum) AS day_of_week,
#                 EXTRACT(DAY FROM datum) AS day_of_month,
#                 datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
#                 EXTRACT(DOY FROM datum) AS day_of_year,
#                 TO_CHAR(datum, 'W')::INT AS week_of_month,
#                 EXTRACT(WEEK FROM datum) AS week_of_year,
#                 EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
#                 EXTRACT(MONTH FROM datum) AS month_actual,
#                 TO_CHAR(datum, 'TMMonth') AS month_name,
#                 TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
#                 EXTRACT(QUARTER FROM datum) AS quarter_actual,
#                 CASE
#                     WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
#                     WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
#                     WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
#                     WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
#                     END AS quarter_name,
#                 EXTRACT(YEAR FROM datum) AS year_actual,
#                 datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
#                 datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
#                 datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
#                 (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
#                 DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
#                 (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
#                 TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
#                 TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
#                 TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
#                 TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
#                 CASE
#                     WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
#                     ELSE FALSE
#                     END AS weekend_indr
#             FROM (SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
#                 FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
#                 GROUP BY SEQUENCE.DAY) DQ
#             ORDER BY 1;
#             COMMIT;''')
#     return target.execute(sql)


# Person_dimension
def person_dim(sch:str, target, source):
    # Extract
  d1 = pd.read_sql(f'''select p.id, p.title, p.first_name, p.middle_name, p.last_name, u.name, p.member_status,
                            p.gender, p.dob, p.race, p.highest_qualification,p.zip, p.state, p.country, p.city, p.volunteer_type,
                            p.profession, p.job_title, p.hobbies,p.fam_id, p.grade, p.employment_status, p.membership_date, p.conversion_date,
                            p.professional_interests, p.industry,p.join_date, p.how_join, p.baptism_date,
                            p.spiritual_challenge, p.spiritual_need,
                            p.is_flag
                      from {sch}.person as p
                      join {sch}.user_type as u 
                      on p.user_type_id = u.id''', con=source)
  d3 = pd.read_sql(f'select * from {sch}.fam_dim', con=target)

  #  Transform
  d1 = d1.rename(columns={'zip':'zipcode', 'state':'state_province', 'name':'user_type',
                      'country':'country_name', 'professional_interests':'interests',
                        'is_flag':'flag_status'})
  
  today = date.today()
  d1['fullname'] = d1.first_name + ' ' + d1.last_name
  d1['gender'] = [i.title() for i in d1.gender]
  d1['dob'] = pd.to_datetime(d1.dob)
  d1['per_age'] = d1['dob'].apply(lambda x: today.year - x.year - ((today.month, today.day) < (x.month, x.day)))

  d1['per_age_interval'] = ['1 - 12' if i in range(1,13)
                      else '13 - 16' if i in range(13,17)
                      else '17 - 40' if i in range(17,41)
                      else '41 - 64' if i in range(41,65)
                      else '65+'
                      for i in d1.per_age]
  
  d1['baptism_status'] = [False if i == '' or i == 'Null' else True for i in d1.baptism_date]
  d1['spiritual_maturity'] = ['Matured' for i in range(len(d1))] # Not defined
  d1['kid_class'] = [d1.grade[i] if d1.per_age_interval[i] == '1 - 12' else 'Not a Kid' for i in range(len(d1))]
  d1['updated_at'] = date.today()
  d1['spiritual_need'] = d1['spiritual_challenge']  # Not defined
  d1['flag_status'] = [False for i in range(len(d1))]
  d1['volunteer_status'] = ["Not Volunteer" if i == False or i == 'Null' else True for i in d1.volunteer_type]

  # Filling missing values
  d1['city'].fillna('NA', inplace=True)
  d1['state'].fillna('NA', inplace=True)
  d1['country'].fillna('NA', inplace=True)
  d1['city'].fillna('NA', inplace=True)
  d1['member_status'].fillna('Not Member', inplace = True)
  d1['spiritual_challenge'].fillna('NA', inplace = True)
  d1['member_status'].fillna('Not Member', inplace = True)
  
  # Merging table
  d1 = d1.merge(d3[['fam_key','fam_id']], on =['fam_id'])

  d1 = d1.rename(columns={'hobbies':'per_hobbies', 'join_date':'per_join_date',
                        'industry':'per_industry', 'profession':'per_profession'})

  d1['person_key'] = d1.index + 1

  # Load
  col = ['person_key','id', 'title', 'fullname', 'user_type', 'member_status','fam_key', 'volunteer_status',
      'gender', 'dob', 'race', 'highest_qualification', 'per_age', 'per_age_interval', 'employment_status', 'membership_date',
      'kid_class', 'per_profession', 'job_title', 'per_hobbies', 'city','state', 'country', 'country_code', 'conversion_date',
      'interests', 'per_industry', 'per_join_date', 'how_join', 'baptism_status',
      'spiritual_challenge', 'spiritual_need', 'spiritual_maturity',
      'flag_status', 'load_date']
  d1 = d1[col]

  
  try:
    d1.to_sql('person_dim', schema=sch, con = target, if_exists='append', index=False)
    # upsert_df(d1, 'person_dim', sch, target, 'person_key')
    print("Data import Successful")
  except Exception as e:
    print(f"Data import error : {e}")
    breakpoint



# Family_dimension
def fam_dim(sch:str, target, source):
  # Extract
  fam = pd.read_sql(f'SELECT * from {sch}.fam;', con=source)
  per = pd.read_sql(f'SELECT * from {sch}.person;', con=source)

  # Tansform
  fam = fam.rename(columns={'id':'fam_id', 'last_name': 'fam_name', 
                  'created_at': 'fam_date_created'})

  fam['no_fam_members'] =  fam.fam_id.map(per.fam_id.value_counts()) #fam['fam_size']
  fam['updated_at'] = date.today()

  # Filling missing values
  fam['fam_city'].fillna('NA', inplace=True)
  fam['fam_state'].fillna('NA', inplace=True)
  fam['fam_country'].fillna('NA', inplace=True)

  fam['no_fam_members'].fillna(0, inplace=True)

  fam['fam_key'] = fam.index + 1

  # Load
  col = ['fam_key','fam_id', 'fam_name', 'no_fam_members','fam_city','fam_state', 'fam_country',
      'fam_wedding_date', 'fam_date_created', 'load_date']
  fam = fam[col]
    
  try:
    fam.to_sql('fam_dim', schema=sch, con = target, if_exists='append', index=False)
    # upsert_df(fam, 'fam_dim', sch, target, 'fam_key')
    print("Data import Successful")
  except Exception as e:
    print(f"Data import error : {e}")
    breakpoint


# Process task dimension
def process_dim(sch:str, source, target):
    # Extract
    d1 = pd.read_sql(f"""select *
                        from {sch}.process as p
                        join {sch}.task as t
                        on (p.id = t.process_id )
                        join {sch}.task_assignee as a
                        on (t.id = a.task_id)
                    """, con=source)
    # Transform     
    d1['task_duration'] = (datetime(d1['task_actual_enddate']) - datetime(d1['task_actual_startdate'])).total_seconds()/3600  # in hours
    d1['process_duration'] = (datetime(d1['process_actual_endtime']) - datetime(d1['process_actual_starttime'])).total_seconds()/3600
    d1['process_key'] = d1.index + 1 

    col = ['process_key', 'process_name','process_type','process_duration','process_status',
            'task_id', 'task_type', 'task_priority', 'task_status', 'task_duration', 'followup_user', 'followup_user_role'
            'task_date_created', 'task_date_completed', 'load_date', 'updated_at']
    d1 = d1[col]
    
    # Load
    try:
        d1.to_sql('process_task_dim', schema=sch, con = target, if_exists='append', index=False)
        # upsert_df(d1, ''process_task_dim', sch, target, 'process_key')
        print("Data import Successful")
    except Exception as e:
        print(f"Data import error : {e}")
        breakpoint
    return


# Person_fact
def per_fact(sch:str, target):

    # Extract
    d1 = pd.read_sql(f'SELECT * from {sch}.person_dim;', con=target)

    # Transform
    today = date.today()
    d1['no_of_days_sincejoined'] = pd.to_datetime(d1.per_join_date).apply(lambda x: (today.year - x.year - ((today.month, today.day) < (x.month, x.day)))*365)
    d1['no_of_testimony'] = [rd.choices([0, 2, 3, 5, 8], weights=[8,5,3,2,1])[0] for _ in range(len(d1))] # randomly generated
    d1['no_of_service_rqst'] = [rd.choice(range(10)) for _ in range(len(d1))] # randomly generated
    d1['amount_pledged'] = [round(rd.choice(range(330000)), -3) for _ in range(len(d1))] # randomly generated
    
    d1['date_key'] = str(date.isoformat(date.today())).replace('-', '')
    d1['load_date'] = today
    
    # Filling missing values
    d1['no_of_service_rqst'].fillna(0, inplace=True)
    d1['no_of_testimony'].fillna(0, inplace=True)
    d1['amount_pledged'].fillna(0, inplace=True)

    d1['person_fact_key'] = d1.index + 1

    col = ['person_fact_key','date_key', 'person_key', 'no_of_days_sincejoined',
        'no_of_testimony', 'no_of_service_rqst', 'amount_pledged', 'load_date']

    d1 = d1[col]

    # Load
    try:
        d1.to_sql('person_fact', schema=sch, con = target, if_exists='append', index=False)
        # upsert_df(d1, 'person_fact', sch, target, 'person_fact_key')
        print("Data import Successful")
    except Exception as e:
        print(f"Data import error : {e}")
        breakpoint

# per_fact()

# Church_fact
def church_fact(sch:str, target):
    # Extract
    d1 = pd.read_sql(f'SELECT * from {sch}.person_dim;', con=target)
    d2 = pd.read_sql(f'SELECT * from {sch}.fam_dim;', con=target)
    d3 = pd.read_sql(f'SELECT * from {sch}.process_task_dim;', con=target)


    #  Transform
    d4 = {}
    d4['no_of_families'] = d2.fam_id.nunique() 
    d4['avg_fam_size'] = d2['no_fam_members'].mean()
    d4['total_population'] = d1.id.nunique()
    d4['no_of_females'] = d1['gender'].tolist().count('Female')
    d4['no_of_males'] = d1['gender'].tolist().count('Male')
    d4['no_of_kids'] = d1['per_age_interval'].tolist().count('1 - 12')
    d4['no_of_teenagers'] = d1['per_age_interval'].tolist().count('13 - 16')
    d4['no_of_youths'] = d1['per_age_interval'].tolist().count('17 - 40')
    d4['no_of_adults'] = d1['per_age_interval'].tolist().count('41 - 64')
    d4['no_of_elders'] = d1['per_age_interval'].tolist().count('65+')
    d4['no_of_visitors'] = d1['user_type'].tolist().count('Visitor')
    d4['no_of_baptisms'] = d1['baptism_status'].tolist().count(True)
    d4['no_of_volunteers'] = d1['volunteer_status'].count(True)
    d4['followup_completion_time'] = 
    d4['max_followup_time'] = 
    d4['min_followup_time'] = 
    d4['visitorconversion_rate'] = 
    d4['no_of_tasks'] = 
    d4['no_of_tasks_todo'] = 
    d4['no_of_tasks_inprogress'] = 
    d4['no_of_tasks_completed'] = 
    d4['no_of_vistors_followedup'] = 
    d4['avg_followup_duration'] = 

    d4['no_of_active_members'] = d1['member_status'].tolist().count('Active')
    d4['no_of_inactive_members'] = d1['member_status'].tolist().count('Inactive')
    d4['no_of_exmembers'] = d1['member_status'].tolist().count('Ex-Member')
    d4['no_of_deceased_members'] = d1['member_status'].tolist().count('Deceased')
    d4['no_of_churned_members'] = abs(d4['no_of_active_members'] - d4['no_of_inactive_members'])
    d4['no_of_retained_members'] = d4['total_population'] - (d4['no_of_churned_members'] + d4['no_of_deceased_members'])
    d4['load_date'] = datetime.today()
    d4['date_key'] = str(date.isoformat(date.today())).replace('-', '')
        
    
    def celebrate(m):
        today = date.today()
        m =[]
        for i in pd.to_datetime(m):
            if today.day == i.day and today.month == i.month:
                m.append(i)
        return len(m)

    d4['no_of_birthdays'] = celebrate(d1.dob)
    d4['no_of_anniversaries'] = celebrate(d2.fam_wedding_date)
    d4['no_of_new_members'] = len(d1.per_join_date.apply(lambda x: pd.to_datetime(x) == date.today()))

    d4 = pd.DataFrame(d4, index=[0])
    # d4['church_fact_key'] = d4.index + 1

    # Load
    d4 = d4[['date_key',
        'total_population', 'no_of_families', 'no_of_males', 'no_of_females',
        'no_of_kids', 'no_of_teenagers', 'no_of_youths', 'no_of_adults',
        'no_of_elders', 'no_of_birthdays', 'no_of_anniversaries',
        'no_of_baptisms', 'no_of_new_members', 'no_of_active_members',
        'no_of_inactive_members', 'no_of_exmembers', 'no_of_visitors',
        'no_of_deceased_members', 'no_of_churned_members',
        'no_of_retained_members', 'load_date']]
    return d4.to_sql('church_fact', con=target, schema = sch , if_exists='append', index=False)
