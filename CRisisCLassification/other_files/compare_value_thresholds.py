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