def compare_value_thresholds(value, StID, dstr_ind, Thresholds):

    StationID = StID[0]
    dstr_indicator = dstr_ind[0]

    if dstr_indicator == 'Water':
        # find the element of the Thresholds (_WL dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):

            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_WL = ['#00FF00']  # green
                     MNote_WL = ['Water Level OK - Moderate Crisis Level']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = ['#FFFF00']  # yellow
                     MNote_WL = ['Water Level exceeds 1st alarm threshold - Medium Crisis Level']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = ['#FFA500']  # orange
                     MNote_WL = ['Water Level exceeds 2nd alarm threshold - High Crisis Level']
                 else:  # value >= item['Alarm3']:
                     MCol_WL = ['#FF0000'] # red
                     MNote_WL = ['Water Level exceeds 3rd alarm threshold - Very High Crisis Level']


        MColNote_WL = [MCol_WL, MNote_WL]

        return MColNote_WL

    if dstr_indicator == 'Precipitation':
        # find the element of the Thresholds (_PR dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):
            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_PR = ['#00FF00']  # green
                     MNote_PR = ['Precipitation Level OK - Moderate Crisis Level']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_PR = ['#FFFF00']  # yellow
                     MNote_PR = ['Precipitation Level exceeds 1st alarm threshold - Medium Crisis Level']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_PR = ['#FFA500']  # orange
                     MNote_PR = ['Precipitation Level exceeds 2nd alarm threshold - High Crisis Level']
                 else:  # value >= item['Alarm3']:
                     MCol_PR = ['#FF0000'] # red
                     MNote_PR = ['Precipitation Level exceeds 3rd alarm threshold - Very High Crisis Level']

        MColNote_PR = [MCol_PR, MNote_PR]

        return MColNote_PR

#---------------------------------------------------------------------------------------
# Compare value with alarm thresholds and return a color, note, scale and scale_note
#
def compare_value_scale_thresholds(value, StID, dstr_ind, Thresholds):

    StationID = StID[0]
    dstr_indicator = dstr_ind[0]

    if dstr_indicator == 'Water':
        # find the element of the Thresholds (_WL dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):

            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_WL = ['#00FF00']  # green
                     MNote_WL = ['Water Level OK - Moderate Crisis Level']
                     Scale_WL = 0 #['0']
                     Note_Scale_WL = ['Trivial']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = ['#FFFF00']  # yellow
                     MNote_WL = ['Water Level exceeds 1st alarm threshold - Medium Crisis Level']
                     Scale_WL = 1 #['1']
                     Note_Scale_WL = ['Low']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = ['#FFA500']  # orange
                     MNote_WL = ['Water Level exceeds 2nd alarm threshold - High Crisis Level']
                     Scale_WL = 2 #['2']
                     Note_Scale_WL = ['Medium']
                 else:  # value >= item['Alarm3']:
                     MCol_WL = ['#FF0000'] # red
                     MNote_WL = ['Water Level exceeds 3rd alarm threshold - Very High Crisis Level']
                     Scale_WL = 3 #['3']
                     Note_Scale_WL = ['High']

        MColNote_WL = [MCol_WL, MNote_WL, Scale_WL, Note_Scale_WL]

        return MColNote_WL

    if dstr_indicator == 'Precipitation':
        # find the element of the Thresholds (_PR dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):
            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_PR = ['#00FF00']  # green
                     MNote_PR = ['Precipitation Level OK - Moderate Crisis Level']
                     Scale_PR = 0 #['0']
                     Note_Scale_PR = ['Trivial']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_PR = ['#FFFF00']  # yellow
                     MNote_PR = ['Precipitation Level exceeds 1st alarm threshold - Medium Crisis Level']
                     Scale_PR = 1 #['1']
                     Note_Scale_PR = ['Low']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_PR = ['#FFA500']  # orange
                     MNote_PR = ['Precipitation Level exceeds 2nd alarm threshold - High Crisis Level']
                     Scale_PR = 2 #['2']
                     Note_Scale_PR = ['Medium']
                 else:  # value >= item['Alarm3']:
                     MCol_PR = ['#FF0000'] # red
                     MNote_PR = ['Precipitation Level exceeds 3rd alarm threshold - Very High Crisis Level']
                     Scale_PR = 3 #['3']
                     Note_Scale_PR = ['High']

        MColNote_PR = [MCol_PR, MNote_PR, Scale_PR, Note_Scale_PR]

        return MColNote_PR

#---------------------------------------------------------------------------------------
#
# Calculate the Crisis Classification Level for each River Section
#   If the value exceeds one of the predefined thresholds then
#   the corresponding note, color and scale will be assigned to it and
#   the flag will be set as true. If flag_extreme is equal to false then the
#   value
#
def compare_forecast_scale_thresholds(value, Thresholds):

    flag_extreme = False

    # Compare maximum observation of the Obs_yv with the Thresholds
    if value >= Thresholds[0] and value < Thresholds[1]:
        meas_color = ['#FFFF00']  # yellow
        meas_note = ['Water level overflow: exceeding of the 1st alarm threshold']
        scale = 1
        scale_note = ["Low"]
        flag_extreme = True
    elif value >= Thresholds[1] and value < Thresholds[2]:
        meas_color = ['#FFA500']  # orange
        meas_note = ['Water level overflow: exceeding of the 2nd alarm Thresholds']
        scale = 2
        scale_note = ["Medium"]
        flag_extreme = True
    elif value >= Thresholds[2]:
        meas_color = ['#FF0000']  # red
        meas_note = ['Water level overflow: exceeding of the 3rd alarm threshold']
        scale = 3
        scale_note = ["High"]
        flag_extreme = True
    else:
        meas_color = ['#00FF00']  # green
        meas_note = ['Water level OK']
        scale = 0
        scale_note = ["Trivial"]
        flag_extreme = True

    if flag_extreme == True:
        resp_comp = [meas_color, meas_note, scale, scale_note, flag_extreme]
    else:
        resp_comp = ["None", "None", "None", "None", flag_extreme]

    return resp_comp

#--------------------------------------------------------------------------------------------------
# Calculates the generalized (power) mean
#
def generalized_mean(freqs, scale, p):

    from math import pow

    len_sc = len(scale)
    len_freqs = len(freqs)

    if len_freqs == len_sc and p != 0:
        sum_freq_sc = 0
        for i in range(0, len_freqs):
            sum_freq_sc = sum_freq_sc + freqs[i]*pow(scale[i], p)

        sum_freq = sum(freqs)

        gen_mean = pow( sum_freq_sc/sum_freq, 1.0/p)
    else:
        print("\n The generalized mean could not be calculated. Either p is non zero "
              "either the length of scale is different from freqs!!!")
        gen_mean = None

    return(gen_mean)
