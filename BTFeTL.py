#!/usr/bin/env python3
from datetime import datetime, date
import logging
import linecache
import sys

import pandas as pd


def get_exception() -> str:
    """
    A function which returns Exception details in the format
    'Failure at {file_name}: {line_number} [{line}] - {exception_message}'

    Args:

    Returns: Formatted string with exception details

    """
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    line_no = tb.tb_lineno
    file_name = f.f_code.co_filename
    linecache.checkcache(file_name)
    line = linecache.getline(file_name, line_no, f.f_globals)
    return f'Failure at {file_name}: {line_no} [{line.strip()}] - {exc_obj}'


def duration_string(x: int) -> str:
    string = ''
    if int(x / 3600) > 0:
        string += f'{int(x / 3600)} hours'
    string += f" {int(x / 60 % 60)} minutes"
    return string


def data_transformation(raw_data):

    # Added this line to remove warnings coming from pandas library modules
    pd.options.mode.chained_assignment = None

    logging.info('----------------------- STARTED BTF DATA TRANSFORMATION ----------------------------\n')
    start_time = datetime.now()

    ## Added for multiple unload spot
    tags = pd.read_csv('./data/Tags Mapping.csv')
    ungrnd_tanks = {'12': 122506, '8944': 156692, '8945': 217054, '2230E': 25374, '2230W': 25965, '2232E': 29767, '2232W': 26286,
                    '2633E': 32914, '2633W': 32914, '30C': 25815, '30E': 21822, '30W': 21585, '40E': 35015, '40W': 34858}

    results_df = pd.DataFrame()
    counter = 0

    ## Added for multiple unload spots
    for index, row in tags.iterrows():
        tank = row['Tank']
        extra_pumps = row['Extra Pumps']

        # The MainDf from BTFT2Query.py
        # TODO: Replace temporary csv data source with data from btf_data_extraction.py
        logging.info(f'WORKING ON TANK {tank}')

        try:
            if f'Discharge {tank}' in raw_data.columns:
                main_df = raw_data[[f'Level {tank}', f'Temp. {tank}', f'Density {tank}', f'Kilos {tank}', f'GCAS {tank}', f'{tank}', 'Time', f'Discharge {tank}']]
            else:
                main_df = raw_data[[f'Level {tank}', f'Temp. {tank}', f'Density {tank}', f'Kilos {tank}', f'GCAS {tank}', f'{tank}', 'Time']]
                main_df[f'Discharge {tank}'] = pd.NA

            if pd.notna(extra_pumps):
                main_df[f'{tank}'].ffill(inplace=True)
                try:
                    main_df[f'{tank}'] = main_df[f'{tank}'].astype(int)
                except:
                    main_df[f'{tank}'] = 0

                extra_pumps = extra_pumps.split(', ')
                for extra_pump in extra_pumps:
                    if extra_pump in raw_data.columns:
                        main_df = pd.merge(main_df, raw_data[extra_pump + ['Time']], on='Time', how='left')
                        main_df[extra_pump].ffill(inplace=True)
                        main_df[extra_pump].bfill(inplace=True)
                        try:
                            main_df[extra_pump] = main_df[extra_pump].astype(int)
                        except:
                            main_df[extra_pump] = 0
                        main_df[f'{tank}'] = main_df[f'{tank}'] | main_df[extra_pump]
                main_df.drop(columns=extra_pumps, errors='ignore', inplace=True)

            main_df.columns = ['Level', 'Temp.', 'Density', 'Kilos', 'GCAS', 'PumpRunning', 'Time', 'Disc_Output']
            main_df['Level'] = pd.to_numeric(main_df['Level'])
            main_df['Temp.'] = pd.to_numeric(main_df['Temp.'])
            main_df['Density'] = pd.to_numeric(main_df['Density'])
            main_df['Kilos'] = pd.to_numeric(main_df['Kilos'])
            main_df['PumpRunning'] = pd.to_numeric(main_df['PumpRunning'])
            main_df['Disc_Output'] = pd.to_numeric(main_df['Disc_Output'])

            main_df.reset_index(drop=True, inplace=True)
            main_df['Level(-1)'] = main_df['Level'].shift(periods=1)    # CopyWarning
            main_df['Level ROC'] = (main_df['Level'] - main_df['Level(-1)']) * 100    # CopyWarning

            logging.info('Performing initial clean-up operations')
            main_df['Time'] = pd.to_datetime(main_df['Time'])    # CopyWarning
            main_df.loc[0:1, 'Level(-1)'].bfill(inplace=True)
            main_df.loc[0:1, 'Level ROC'].bfill(inplace=True)

            # Filling in any missing data using linear interpolation
            main_df['Level'].interpolate(method='linear', inplace=True, limit_direction='forward')
            main_df['Temp.'].interpolate(method='linear', inplace=True, limit_direction='forward')
            main_df['Density'].interpolate(method='linear', inplace=True, limit_direction='forward')
            main_df['Kilos'].interpolate(method='linear', inplace=True, limit_direction='forward')
            main_df['Level(-1)'].interpolate(method='linear', inplace=True, limit_direction='forward')
            main_df['Level ROC'].interpolate(method='linear', inplace=True, limit_direction='forward')
            main_df['PumpRunning'].ffill(inplace=True)
            main_df['Disc_Output'].interpolate(method='linear', inplace=True, limit_direction='forward')

            # Filling in NA values in the first record
            main_df['Pump Filling'] = main_df['PumpRunning']    # CopyWarning
            main_df['Level ROC(-1)'] = main_df['Level ROC'].shift(periods=1)    # CopyWarning
            main_df['PumpRunning(-1)'] = main_df['PumpRunning'].shift(periods=1)    # CopyWarning

            # Populating Event_Id
            main_df['Event_Id'] = main_df['PumpRunning'] - main_df['PumpRunning(-1)']    # CopyWarning
            main_df.loc[0, 'Event_Id'] = 1
            main_df.loc[~(main_df['Event_Id'] == 1), 'Event_Id'] = pd.NA
            main_df.loc[main_df['Event_Id'] == 1, 'Event_Id'] = main_df['Time']

            main_df['Event_Id'].ffill(inplace=True)

            # Adding 'OnePercentLITDelta' and 'UsableTankVolume' fields
            main_df['OnePercentLITDelta'] = main_df['Kilos'] / main_df['Level']    # CopyWarning
            main_df['UsableTankVolume'] = (main_df['Kilos'] / main_df['Level']) * 100    # CopyWarning
            logging.info('Data cleaned and formatted. Ready for transformation')

            logging.info('Starting GCAS Density calculations')
            # GCAS DENSITY CALCULATIONS
            # Grouping by 'Event_Id' and dates
            gcas_calc = main_df.groupby(pd.Grouper(key='Event_Id', freq='1D')) \
                            .agg(Mean_OnePercentLITDelta=('OnePercentLITDelta', 'mean'),
                                    Mean_UsableTankVolume=('UsableTankVolume', 'mean'),
                                    Min_UsableTankVolume=('UsableTankVolume', min),
                                    Min_OnePercentLITDelta=('OnePercentLITDelta', min),
                                    Max_OnePercentLITDelta=('OnePercentLITDelta', max),
                                    Max_UsableTankVolume=('UsableTankVolume', max),
                                    Mean_Density=('Density', 'mean'))
            gcas_calc.dropna(how='any', inplace=True)

            # Doing post-ops and clean up
            gcas_calc.rename(columns={'Mean_OnePercentLITDelta': 'Mean(OnePercentLITDelta)',
                                    'Mean_UsableTankVolume': 'Mean(UsableTankVolume)',
                                    'Min_UsableTankVolume': 'Min*(UsableTankVolume)',
                                    'Min_OnePercentLITDelta': 'Min*(OnePercentLITDelta)',
                                    'Max_OnePercentLITDelta': 'Max*(OnePercentLITDelta)',
                                    'Max_UsableTankVolume': 'Max*(UsableTankVolume)',
                                    'Mean_Density': 'Mean(Density)'}, inplace=True)
            gcas_calc['Year'] = gcas_calc.index.year
            gcas_calc['Month (number)'] = gcas_calc.index.month
            gcas_calc['Day of month'] = gcas_calc.index.day
            gcas_calc.reset_index(drop=True, inplace=True)

            logging.info('Completed GCAS Density calculations')

            logging.info('Starting Discharge calculations')
            # DISCHARGE CALCULATIONS
            # Grouping by 'Event_Id'
            discharge_calc = main_df[main_df['PumpRunning'] == 0].groupby(by='Event_Id', as_index=False) \
                                                                .agg(Time_First=('Time', min),
                                                                    Time_Last=('Time', max),
                                                                    Level_Mean=('Level', 'mean'),
                                                                    Level_ROC_Mean=('Level ROC', 'mean'),
                                                                    Level_ROC_Min=('Level ROC', min),
                                                                    Level_ROC_Max=('Level ROC', 'max'),
                                                                    Level_Min=('Level', min),
                                                                    Level_Max=('Level', max),
                                                                    Temp_Mean=('Temp.', 'mean'),
                                                                    Temp_Min=('Temp.', min),
                                                                    Temp_Max=('Temp.', max),
                                                                    Total_volume=('UsableTankVolume', 'mean'),
                                                                    OnePercentLITDelta_Mean=('OnePercentLITDelta', 'mean'),
                                                                    UsableTankVolume_Mean=('UsableTankVolume', 'mean'),
                                                                    Density_Mean=('Density', 'mean'),
                                                                    Disc_Output_Mean=('Disc_Output', 'mean'))

            # Renaming columns
            discharge_calc.rename(columns={'Time_First': 'Time (First)',
                                        'Time_Last': 'Time (Last)',
                                        'Level_Mean': 'Level (Mean)',
                                        'Level_ROC_Mean': 'Level ROC (Mean)',
                                        'Level_ROC_Min': 'Level ROC (Min*)',
                                        'Level_ROC_Max': 'Level ROC (Max*)',
                                        'Level_Min': 'Level (Min*)',
                                        'Level_Max': 'Level (Max*)',
                                        'Temp_Mean': 'Temp. (Mean)',
                                        'Temp_Min': 'Temp. (Min*)',
                                        'Temp_Max': 'Temp. (Max*)',
                                        'Total_volume': 'Total Volume',
                                        'Density_Mean': 'Density (Mean)',
                                        'OnePercentLITDelta_Mean': 'OnePercentLITDelta (Mean)',
                                        'UsableTankVolume_Mean': 'UsableTankVolume (Mean)',
                                        'Disc_Output_Mean': 'Supply discharge pump%'}, inplace=True)

            # Doing post-ops to get calculated fields
            discharge_calc['Time (Duration)'] = discharge_calc['Time (Last)'] - discharge_calc['Time (First)']
            discharge_calc['Seconds'] = discharge_calc['Time (Duration)'].dt.total_seconds()
            discharge_calc['Minutes'] = discharge_calc['Seconds'] / 60
            discharge_calc['Hours'] = discharge_calc['Seconds'] / 3600
            discharge_calc['Time (Duration)'] = discharge_calc['Seconds'].apply(duration_string)

            size = main_df['UsableTankVolume'].mean()
            discharge_calc['Event_rate(Appr.)'] = (discharge_calc['Level (Max*)'] - discharge_calc['Level (Min*)']) / 100
            discharge_calc['Event_rate(Appr.)'] = discharge_calc['Event_rate(Appr.)'] / (1 / size)
            discharge_calc['Event_rate(Appr.)'] = discharge_calc['Event_rate(Appr.)'] / discharge_calc['Seconds'] * 60

            discharge_calc['Event_rate'] = (discharge_calc['Level (Max*)'] - discharge_calc['Level (Min*)']) * discharge_calc['Density (Mean)']
            discharge_calc['Event_rate'] = discharge_calc['Event_rate'] * discharge_calc['OnePercentLITDelta (Mean)']
            discharge_calc['Event_rate(min)'] = discharge_calc['Event_rate'] / discharge_calc['Minutes']
            discharge_calc['Event_rate(hr)'] = discharge_calc['Event_rate'] / discharge_calc['Hours']


            discharge_calc['Quantity'] = (discharge_calc['Level (Max*)'] - discharge_calc['Level (Min*)'])
            if tank in ungrnd_tanks:
                discharge_calc['Quantity'] = discharge_calc['Quantity'] * ungrnd_tanks[tank]
            else:
                discharge_calc['Quantity'] = discharge_calc['Quantity'] * discharge_calc['Density (Mean)'] * discharge_calc['OnePercentLITDelta (Mean)']

            # Discarding events < 20 mins
            discharge_calc = discharge_calc[discharge_calc['Minutes'] > 20]

            discharge_calc['Event Type'] = 'Discharge'
            discharge_calc['Tank'] = tank
            discharge_calc = discharge_calc[['Event_Id', 'Time (Duration)', 'Time (First)', 'Time (Last)', 'Level (Mean)', 'Level (Min*)',
                                            'Level (Max*)', 'Temp. (Mean)', 'Temp. (Min*)', 'Temp. (Max*)', 'Seconds', 'Total Volume',
                                            'Event_rate(Appr.)', 'Minutes', 'Hours', 'Quantity', 'Event_rate(hr)', 'Event_rate(min)',
                                            'Event Type', 'Tank', 'Supply discharge pump%']]

            results_df = results_df.append(discharge_calc, ignore_index=True)

            logging.info('Completed Discharge calculations')
            # discharge_calc.to_csv('./data/disc.csv', index=False)

            # UNLOADING CALCULATION
            logging.info('Starting Unload calculations')
            # Grouping by 'Event_Id'
            unloading_calc = main_df[main_df['PumpRunning'] == 1].groupby(by='Event_Id', as_index=False) \
                                                                .agg(Time_First=('Time', min),
                                                                    Time_Last=('Time', max),
                                                                    Level_Mean=('Level', 'mean'),
                                                                    Level_ROC_Mean=('Level ROC', 'mean'),
                                                                    Level_ROC_Min=('Level ROC', min),
                                                                    Level_ROC_Max=('Level ROC', 'max'),
                                                                    Level_Min=('Level', min),
                                                                    Level_Max=('Level', max),
                                                                    Temp_Mean=('Temp.', 'mean'),
                                                                    Temp_Min=('Temp.', min),
                                                                    Temp_Max=('Temp.', max),
                                                                    Total_volume=('UsableTankVolume', 'mean'),
                                                                    OnePercentLITDelta_Mean=('OnePercentLITDelta', 'mean'),
                                                                    UsableTankVolume_Mean=('UsableTankVolume', 'mean'),
                                                                    Density_Mean=('Density', 'mean'),
                                                                    Disc_Output_Mean=('Disc_Output', 'mean'))

            # Renaming columns
            unloading_calc.rename(columns={'Time_First': 'Time (First)',
                                        'Time_Last': 'Time (Last)',
                                        'Level_Mean': 'Level (Mean)',
                                        'Level_ROC_Mean': 'Level ROC (Mean)',
                                        'Level_ROC_Min': 'Level ROC (Min*)',
                                        'Level_ROC_Max': 'Level ROC (Max*)',
                                        'Level_Min': 'Level (Min*)',
                                        'Level_Max': 'Level (Max*)',
                                        'Temp_Mean': 'Temp. (Mean)',
                                        'Temp_Min': 'Temp. (Min*)',
                                        'Temp_Max': 'Temp. (Max*)',
                                        'Total_volume': 'Total Volume',
                                        'Density_Mean': 'Density (Mean)',
                                        'OnePercentLITDelta_Mean': 'OnePercentLITDelta (Mean)',
                                        'UsableTankVolume_Mean': 'UsableTankVolume (Mean)',
                                        'Disc_Output_Mean': 'Supply discharge pump%'}, inplace=True)

            # Performing post-ops to get calculated fields
            unloading_calc['Time (Duration)'] = unloading_calc['Time (Last)'] - unloading_calc['Time (First)']
            unloading_calc['Seconds'] = unloading_calc['Time (Duration)'].dt.total_seconds()
            unloading_calc['Minutes'] = unloading_calc['Seconds'] / 60
            unloading_calc['Hours'] = unloading_calc['Seconds'] / 3600
            unloading_calc['Time (Duration)'] = unloading_calc['Seconds'].apply(duration_string)

            unloading_calc['Event_rate(Appr.)'] = (unloading_calc['Level (Max*)'] - unloading_calc['Level (Min*)']) / 100
            unloading_calc['Event_rate(Appr.)'] = unloading_calc['Event_rate(Appr.)'] / (1 / size)
            unloading_calc['Event_rate(Appr.)'] = unloading_calc['Event_rate(Appr.)'] / unloading_calc['Seconds'] * 60

            unloading_calc['Event_rate'] = (unloading_calc['Level (Max*)'] - unloading_calc['Level (Min*)']) * unloading_calc['Density (Mean)']
            unloading_calc['Event_rate'] = unloading_calc['Event_rate'] * unloading_calc['OnePercentLITDelta (Mean)']
            unloading_calc['Event_rate(min)'] = unloading_calc['Event_rate'] / unloading_calc['Minutes']
            unloading_calc['Event_rate(hr)'] = unloading_calc['Event_rate'] / unloading_calc['Hours']

            unloading_calc['Quantity'] = unloading_calc['Level (Max*)'] - unloading_calc['Level (Min*)']
            if tank in ungrnd_tanks:
                unloading_calc['Quantity'] = unloading_calc['Quantity'] * ungrnd_tanks[tank]
            else:
                unloading_calc['Quantity'] = unloading_calc['Quantity'] * unloading_calc['Density (Mean)'] * unloading_calc['OnePercentLITDelta (Mean)']

            unloading_calc['TFMEUnloadSpotRail'] = unloading_calc['Quantity'] / 79000

            # Discarding events < 20 mins
            unloading_calc = unloading_calc[unloading_calc['Minutes'] > 20]

            unloading_calc['Event Type'] = 'Unloading'
            unloading_calc['Tank'] = tank
            unloading_calc = unloading_calc[['Event_Id', 'Time (Duration)', 'Time (First)', 'Time (Last)', 'Level (Mean)', 'Level (Min*)',
                                            'Level (Max*)', 'Temp. (Mean)', 'Temp. (Min*)', 'Temp. (Max*)', 'Seconds', 'Total Volume',
                                            'Event_rate(Appr.)', 'Minutes', 'Hours', 'Quantity', 'Event_rate(hr)', 'Event_rate(min)',
                                            'Event Type', 'Tank', 'TFMEUnloadSpotRail', 'Supply discharge pump%']]

            results_df = results_df.append(unloading_calc, ignore_index=True)

            logging.info('Completed Unloading calculations')
            logging.info(f'Tank {tank} Done\n')

        except Exception as e:
            logging.exception(f'Tank {tank} Exception Occured!\n')
            logging.exception(get_exception())


        counter += 1
        print(f'Transformation counter: {counter} / {len(tags)}\r', end='')

    # Adding as per the PowerBI team's request
    # Converting all the volume columns from Kg to MT
    results_df['Total Volume'] = results_df['Total Volume'] / 1000
    results_df['Event_rate(Appr.)'] = results_df['Event_rate(Appr.)'] / 1000
    results_df['Quantity'] = results_df['Quantity'] / 1000
    results_df['Event_rate(hr)'] = results_df['Event_rate(hr)'] / 1000
    results_df['Event_rate(min)'] = results_df['Event_rate(min)'] / 1000

    results_df['TFMEUnloadSpotRail'].fillna(0, inplace=True)
    results_df['TFMEWeek_'] = pd.to_datetime(results_df['Event_Id']).dt.strftime('%U')
    results_df['Site'] = 'Lima'
    results_df['Plant Code'] = 1702

    results_df = results_df[~((results_df['Level (Max*)'] > 100) | (results_df['Level (Max*)'] < 0))]    # Removing faulty max values
    results_df = results_df[~((results_df['Level (Min*)'] > 100) | (results_df['Level (Min*)'] < 0))]    # Removing faulty min values
    results_df = results_df[~((results_df['Level (Mean)'] > 100) | (results_df['Level (Mean)'] < 0))]    # Removing faulty mean values

    results_df.to_csv(f'./data/Daily Results {date.today()}.csv', index=False)

    end_time = datetime.now()
    logging.info(f'Time for transformation of data: {(end_time - start_time).seconds} seconds')
    logging.info('----------------------- ENDED BTF DATA TRANSFORMATION ----------------------------\n\n')
