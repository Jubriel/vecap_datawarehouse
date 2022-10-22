
import pandas as pd
import random as rd
from datetime import date, datetime
from upsert_df import *

# Geography_dimension
def geo_dim(sch:str, target, source):
    # Extract
    dp = pd.read_sql(f'SELECT * from {sch}.person;', con= source)
    df = pd.read_sql(f'SELECT * from {sch}.fam;', con= source)

    # Transform
    # Renaming columns
    dp.rename(columns={'state': 'state_province', 'country':'country_name',
                   'zip': 'zipcode'},
          inplace=True, errors='raise')
    df.rename(columns={'fam_state': 'state_province', 'fam_country':'country_name',
                   'fam_zip': 'zipcode'},
          inplace=True, errors='raise')
    dp = pd.concat([dp, df], axis=0)
    
    dp = dp.dropna(subset =['state_province'])
    dp['load_date'] = date.today()
    dp['geography_key'] = dp.index + 1
    # Filling missing values
    dp = dp.drop_duplicates(subset = ["state_province"])
    dp['country_code'].fillna('NA', inplace=True)
    dp['country_name'].fillna('NA', inplace=True)

    
    # Load
    dp = dp[['geography_key','state_province','zipcode','country_code','country_name', 'load_date']] #,'created_at', 'updated_at'
    dp.to_sql('geography_dim', con=target, schema = sch, if_exists='append', index=False)
    return  {"Table processed ": "Data imported successful"}


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
  d2 = pd.read_sql(f'select * from {sch}.geography_dim', con=target)
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
  d1['member_status'].fillna('Not Member', inplace = True)
  d1['per_age_interval'] = ['1 - 12' if i in range(1,13)
                      else '13 - 16' if i in range(13,17)
                      else '17 - 40' if i in range(17,41)
                      else '41 - 64' if i in range(41,65)
                      else '65+'
                      for i in d1.per_age]
  
  d1['baptism_status'] = [False if i == '' or i == 'Null' else True for i in d1.baptism_date]
  d1['spiritual_maturity'] = ['Matured' for i in range(len(d1))]
  d1['kid_class'] = [d1.grade[i] if d1.per_age_interval[i] == '1 - 12' else 'Not a Kid' for i in range(len(d1))]
  d1['load_date'] = date.today()
  d1['spiritual_need'] = d1['spiritual_challenge']
  d1['flag_status'] = [False for i in range(len(d1))]
  d1['volunteer_status'] = ["Not Volunteer" if i == False or i == 'Null' else True for i in d1.volunteer_type]

  # Filling missing values
  # filling date format with today's date ~~~~~ need to be corrected
  d1['dob'].fillna(date.today(), inplace=True)
  d1['join_date'].fillna(date.today(), inplace=True)
  d1['baptism_date'].fillna(date.today(), inplace=True)
  # d1['membership_date'].fillna(date.today(), inplace=True)
  d1['city'].fillna('NA', inplace=True)


  # # filing objects with NA
  # for i in d1.columns:
  #     if d1[i].dtype == object:
  #         d1[i].fillna('NA', inplace=True)
  
  # Merging tables
  d1 = d1.merge(d2[['geography_key','city']], on =['city'])
  d1 = d1.merge(d3[['fam_key','fam_id']], on =['fam_id'])

  d1 = d1.rename(columns={'hobbies':'per_hobbies', 'join_date':'per_join_date',
                        'industry':'per_industry', 'profession':'per_proffession'}) #wrong spelling

  d1['person_key'] = d1.index + 1

  # Load
  col = ['person_key','id', 'title', 'fullname', 'user_type', 'member_status', 'geography_key','fam_key', 'volunteer_status',
      'gender', 'dob', 'race', 'highest_qualification', 'per_age', 'per_age_interval', 'employment_status', 'membership_date',
      'kid_class', 'per_proffession', 'job_title', 'per_hobbies', 'city', 'conversion_date',
      'interests', 'per_industry', 'per_join_date', 'how_join', 'baptism_status',
      'spiritual_challenge', 'spiritual_need', 'spiritual_maturity',
      'flag_status', 'load_date']
  d1 = d1[col]

  
  try:
    upsert_df(d1, 'person_dim', sch, target, 'person_key')
    print("Data import Successful")
  except Exception as e:
    print(f"Data import error : {e}")
    breakpoint



# Family_dimension
def fam_dim(sch:str, target, source):
  # Extract
  fam = pd.read_sql(f'SELECT * from {sch}.fam;', con=source)
  per = pd.read_sql(f'SELECT * from {sch}.person;', con=source)
  geo = pd.read_sql(f'select * from {sch}.geography_dim', con=target)


  # Tansform
  fam = fam.rename(columns={'fam_city':'city', 'id':'fam_id', 'last_name': 'fam_name', 
                  'created_at': 'fam_date_created'})

  fam['no_fam_members'] =  fam.fam_id.map(per.fam_id.value_counts()) #fam['fam_size']
  fam['load_date'] = date.today()

  # # Filling missing values
  # dat = [i for i in fam.columns if 'date' in i]
  # for i in fam.columns:
  #   if fam[i].dtype == object and i in dat:
  #     fam[i].fillna('NA', inplace=True)

  fam['fam_city']= fam['city']
  fam['fam_wedding_date'].fillna(date.today(), inplace=True)
  fam = fam.merge(geo[['geography_key','city']], on = 'city')
  fam['fam_key'] = fam.index + 1

  # Load
  col = ['fam_key','fam_id', 'fam_name', 'no_fam_members','geography_key','fam_city',
      'fam_wedding_date', 'fam_date_created', 'load_date']
  fam = fam[col]
  famd = famd[col]
    
  try:
    upsert_df(fam, 'fam_dim', sch, target, 'fam_key')
    print("Data import Successful")
  except Exception as e:
    print(f"Data import error : {e}")
    breakpoint





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
  
  d1['person_fact_key'] = d1.index + 1

  col = ['person_fact_key','date_key', 'person_key', 'no_of_days_sincejoined',
      'no_of_testimony', 'no_of_service_rqst', 'amount_pledged', 'load_date']

  d1 = d1[col]

  # Load
  try:
    upsert_df(d1, 'person_fact', sch, target, 'person_fact_key')
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


    #  Transform
    d4 = {}
    d4['no_of_families'] = d2.fam_id.nunique() 
    # d4['avg_fam_size'] = d2['no_fam_members'].mean()
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
    # d4['no_of_volunteer'] = d1['volunteer_status'].count(True)


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
