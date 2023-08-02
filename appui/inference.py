import pandas as pd
from datetime import datetime
import numpy as np
from joblib import load
import os

filepath = '/home/ubuntu/Pulsar_Real_Time_Fraud_Detection/appui/this_is_a_model.pkl'
model=load(filepath)

def get_age_group(row):
    ans=[0,0,0,0,0,0,0,0,0]
    rows=row
    if rows <20:
        ans[8]=1
    elif rows >=20 and rows<30:
        ans[0]=1
    elif rows >=30 and rows<40:
        ans[1]=1
    elif rows >=40 and rows<50:
        ans[2]=1
    elif rows >=50 and rows<60:
        ans[3]=1
    elif rows >=60 and rows<70:
        ans[4]=1
    elif rows >=70 and rows<80:
        ans[5]=1
    elif rows >=80 and rows<90:
        ans[6]=1
    else:
        ans[7]=1
    return ans
def get_times(dates):
    dt = datetime.strptime(dates,'%Y-%m-%d %H:%M:%S')
    return {'year':dt.year, 'month':dt.month, 'hour':dt.hour, 'day':dt.day, 'weekday':dt.weekday()}

def get_age(dob,trans_date):
    dt = datetime.strptime(dob,'%Y-%m-%d').year
    dt2 = datetime.strptime(trans_date,'%Y-%m-%d %H:%M:%S').year
    return abs(dt-dt2)    

def get_state_encode(state):
    state_grp = [0] * 51
    if state == 'AK':
        state_grp[0] = 1
    elif state == 'AL':
        state_grp[1] = 1
    elif state == 'AR':
        state_grp[2] = 1
    elif state == 'AZ':
        state_grp[3] = 1
    elif state == 'CA':
        state_grp[4] = 1
    elif state == 'CO':
        state_grp[5] = 1
    elif state == 'CT':
        state_grp[6] = 1
    elif state == 'DC':
        state_grp[7] = 1
    elif state == 'DE':
        state_grp[8] = 1
    elif state == 'FL':
        state_grp[9] = 1
    elif state == 'GA':
        state_grp[10] = 1
    elif state == 'HI':
        state_grp[11] = 1
    elif state == 'IA':
        state_grp[12] = 1
    elif state == 'ID':
        state_grp[13] = 1
    elif state == 'IL':
        state_grp[14] = 1
    elif state == 'IN':
        state_grp[15] = 1
    elif state == 'KS':
        state_grp[16] = 1
    elif state == 'KY':
        state_grp[17] = 1
    elif state == 'LA':
        state_grp[18] = 1
    elif state == 'MA':
        state_grp[19] = 1
    elif state == 'MD':
        state_grp[20] = 1
    elif state == 'ME':
        state_grp[21] = 1
    elif state == 'MI':
        state_grp[22] = 1
    elif state == 'MN':
        state_grp[23] = 1
    elif state == 'MO':
        state_grp[24] = 1
    elif state == 'MS':
        state_grp[25] = 1
    elif state == 'MT':
        state_grp[26] = 1
    elif state == 'NC':
        state_grp[27] = 1
    elif state == 'ND':
        state_grp[28] = 1
    elif state == 'NE':
        state_grp[29] = 1
    elif state == 'NH':
        state_grp[30] = 1
    elif state == 'NJ':
        state_grp[31] = 1
    elif state == 'NM':
        state_grp[32] = 1
    elif state == 'NV':
        state_grp[33] = 1
    elif state == 'NY':
        state_grp[34] = 1
    elif state == 'OH':
        state_grp[35] = 1
    elif state == 'OK':
        state_grp[36] = 1
    elif state == 'OR':
        state_grp[37] = 1
    elif state == 'PA':
        state_grp[38] = 1
    elif state == 'RI':
        state_grp[39] = 1
    elif state == 'SC':
        state_grp[40] = 1
    elif state == 'SD':
        state_grp[41] = 1
    elif state == 'TN':
        state_grp[42] = 1
    elif state == 'TX':
        state_grp[43] = 1
    elif state == 'UT':
        state_grp[44] = 1
    elif state == 'VA':
        state_grp[45] = 1
    elif state == 'VT':
        state_grp[46] = 1
    elif state == 'WA':
        state_grp[47] = 1
    elif state == 'WI':
        state_grp[48] = 1
    elif state == 'WV':
        state_grp[49] = 1
    elif state == 'WY':
        state_grp[50] = 1

    return state_grp

def get_prediction(data):
    '''
    data = {
    dob: %Y-%m-%d,
    trans_date_trans_time: %Y-%m-%d %H:%M:%S, # Here year can be either 2019 or 2020
    lat:
    long:
    merch_lat:
    merch_long:
    gender:
    amt:
    state:
    }
    '''
    dob = data['dob']
    trans_date = data['trans_date_trans_time']
    age = get_age(dob,trans_date)
    age_group = get_age_group(age)
    latitude_diff = abs(data['lat']-data['merch_lat'])
    longitude_diff = abs(data['long']-data['merch_long'])
    dist= np.sqrt(pow(latitude_diff*110,2)+pow(longitude_diff*110,2))
    dat=get_times(trans_date)
    gender=data['gender']
    amt=data['amt']
    pred_data=[amt,dat['hour'],dat['month'],dat['weekday'],dat['day'],dist]
    if gender=='F':
        pred_data=pred_data+[1,0]
    else:
        pred_data=pred_data+[0,1]

    if dat['year']==2019:
        pred_data=pred_data+[1,0]
    else:
        pred_data=pred_data+[0,1]
    pred_data=pred_data+age_group
    state = get_state_encode(data['state'])
    pred_data=pred_data+state
    print(model)
    result = model.predict(np.array([pred_data]))
    return np.argmax(result)
data = {

    'dob': "1983-03-09",
    'trans_date_trans_time': "2019-01-01 00:00:18", # Here year can be either 2019 or 2020
    'lat':36.0788,
    'long':-81.1781,
    'merch_lat':36.011293,
    'merch_long':-82.048315,
    'gender':'F',
    'amt':4.97,
    'state':'NC'
}
