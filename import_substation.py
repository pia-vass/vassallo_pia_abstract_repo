#!/Users/mjl/anaconda/bin/python

__title__ = "Process and clean substation 1/2 hourly electricity data"
__author__ = "Mathew Lipson"
__version__ = "2024-10-23"
__email__ = "m.lipson@unsw.edu.au"

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import glob

pd.set_option('display.width', 150)

oshome=os.getenv('HOME')
projpath = f'.'
datapath = f'{projpath}/data'
obspath =  f'{projpath}/data/BOMdata'
plotpath = f'{projpath}/figures'

# create plotpath if it doesn't exist
if not os.path.exists(plotpath):
    os.makedirs(plotpath)

# year start and end date if wanted
sdate,edate = None, None

# select state domains
# domains = ['vic','nsw','qld','wa','tas','sa', 'act']
domains = ['nsw']

#linearly fill gaps max size
fill_gaps = False


def get_demand_data(suppliers,domain,sdate,edate):
    '''Main function to process substation data for a given domain and suppliers'''

    print(f'processing {domain} substations for {suppliers} from {sdate} to {edate}')

    # get bureau of meteorology half hourly temperature data for site within domain
    obs = read_bom_half_hourly()[sdate:edate]

    demand_list = []
    info_list = []
    for supplier in suppliers:
        print(supplier)
        demand,info = get_supplier_demand(supplier)
        # clean data
        demand = clean_data(demand,obs)
        # append to list
        demand_list.append(demand)
        info_list.append(info)

    info_all = pd.concat(info_list,axis=0)
    info_all = info_all.sort_values(by='Residential',ascending=False)

    demand_all = pd.concat(demand_list,axis=1)
    demand_all = demand_all[info_all.index.to_list()]
    demand_all = demand_all[sdate:edate]

    return demand_all, info_all, obs

def read_bom_half_hourly():
    '''
    Reads BOM half hourly temperature data
    Data cost ~$100, see bom website for details
    Data is zipped
    return:
        obs (dataframe): the observation data
    '''
   
    tmp = pd.read_csv(f'{obs_fpath}.zip', na_values='', low_memory=False) # usecols=obs_cols,

    obs = tidy_aws_columns(tmp)
    obs = obs[['year', 'month', 'day', 'hour', 'minute', 't2m','t2m_30max','t2m_30min']]

    # set datetime, replace missing with nan and convert to float
    obs.index = pd.to_datetime(obs[['year','month','day','hour','minute']]).values
    obs = obs.drop(columns=['year','month','day','hour','minute'])
    obs = obs.replace(r'^\s*$', np.nan, regex=True).astype('float')

    return obs

def tidy_aws_columns(df):

    col_map = {
       'Latitude to four decimal places - in degrees'   : 'latitude',
       'Longitude to four decimal places - in degrees'  : 'longitude',
       'Year Month Day Hour Minutes in YYYY'            : 'year',
       'MM'                                             : 'month',
       'DD'                                             : 'day',
       'HH24'                                           : 'hour',
       'MI format in Local standard time'               : 'minute',
       'Air Temperature in degrees Celsius'             : 't2m',
       'Highest air temperature in last 30 minutes in degrees Celsius where observations count >= 12' : 't2m_30max',
       'Lowest air temperature in last 30 minutes in degrees Celsius where observations count >= 12'  : 't2m_30min',
    }

    df.columns = df.columns.str.strip()
    df = df.rename(columns=col_map)

    return df

def get_substation_data(filename):
    '''
    Opens csv substation half-hourly data as collated by the CSIRO NEAR program.
    See https://near.csiro.au/assets/d9b9c3e6-8342-4ab0-832d-82be7317c4f3

    args:
        filename (string): the file to open
    return:
        subst (dataframe): timeseries dataframe for each substation in supply region
        names (dataframe): full string of names with ID as index
    '''

    # import raw data    
    subst = pd.read_csv(filename)           

    # reindex and drop time columns
    year = subst.StartDeliveryTime[0][:4]
    subst.index = pd.date_range(start=f'{year}0101-0000', periods=len(subst), freq='30Min')
    subst = subst.iloc[:,3:]

    # save col id and names
    col_id = [col.split("'")[1] for col in subst.columns]
    col_names = [col.split("'")[3] for col in subst.columns]

    if supplier == 'ausgrid': # ausgrid has different naming convention
        # drop numeric values from col_names
        to_drop = [col.split()[-1] for col in col_names]
        # remove substring from col_names if it is in to_drop, and strip whitespace from end of string
        col_names = [col[:-len(drop)].rstrip() if col.endswith(drop) else col for col,drop in zip(col_names,to_drop)]
        # elements in col_id may have one or two words, create list from first 2 letters of each word in uppercase seperated by underscore
        col_id = [f"{name.split()[0][:2].upper()}_{name.split()[1][:2].upper()}" if len(name.split())>1 else f"{name.split()[0][:5].upper()}" for name in col_names]

    assert len(col_id) == len(set(col_id)), 'col_id not unique'

    # rename columns for simplicity
    subst.columns = col_id

    return subst

def get_domain_info(domain):
    '''
    Get domain specific information
    args:
        domain (string): the domain name (e.g. 'vic' or 'nsw')
    return:
        suppliers (list): list of valid suppliers in domain
        supplier (string): the default supplier
        obs_fpath (string): the path to the BOM observation file for the domain
    '''

    if domain == 'vic':
        suppliers = ['citipower','powercor','ausnet','jemena','united']
        supplier = 'jemena'
        obs_fpath = f'{datapath}/BOMdata/HD01D_Data_086068_9999999910402967.txt'
    if domain == 'nsw':
        suppliers = ['ausgrid']
        supplier = 'ausgrid'
        obs_fpath = f'{datapath}/BOMdata/HD01D_Data_066194_541079810413811.txt'
    if domain == 'qld':
        suppliers = ['energex']
        supplier = 'energex'
    if domain == 'wa':
        suppliers = ['western']
        supplier = 'western'
        obs_fpath = f'{datapath}/BOMdata/HD01D_Data_009225_546889910504794.txt'

    return suppliers, supplier, obs_fpath


def get_supplier_info(supplier):
    '''
    Gets metadata regarding supplier
    args:
        supplier (string): the supplier name
    return:
        subset_info (dataframe): as above with other metadata
    '''

    # DNSP Zone Substation Characteristics
    # https://near.csiro.au/public/aremi/dataset/dnsp_zs_characteristics.csv
    all_info = pd.read_csv(f'{datapath}/DNSP_Zone_Substation_Characteristics.csv')
    
    # convert supplier name to first word, lower case
    all_info['Distribution Network Service Provider'] = all_info['Distribution Network Service Provider'].str.split(pat=' ',expand=True)[0].str.lower()

    # get single provider zone info
    info = all_info[all_info['Distribution Network Service Provider']==supplier]

    # drop unessary columns
    info = info.drop(columns={'Distribution Network Service Provider'})

    # rename longnames
    info = info.rename(columns = {
        'Zone Substation Name':'Name',
        'Zone Substation ID':'ID', 
        'Zone Substation Area (km2)':'Area',
        })

    if supplier == 'ausgrid': # ausgrid has different naming convention
        # drop numeric values from Name
        col_names = [s.split()[0] + ' ' + ' '.join([word for word in s.split()[1:] if not word[0].isdigit()]) for s in info['Name']]
        # make name sentance case and with no trailing whitespace
        col_names = [s.title().rstrip() for s in col_names]
        # elements in col_id may have one or two words, create list from first 2 letters of each word in uppercase seperated by underscore
        col_id = [f"{name.split()[0][:2].upper()}_{name.split()[1][:2].upper()}" if len(name.split())>1 else f"{name.split()[0][:5].upper()}" for name in col_names]

        assert len(col_id) == len(set(col_id)), 'col_id not unique'
        info['Name'] = col_names
        info['ID'] = col_id

    info.set_index('ID',inplace=True)

    return info

def get_supplier_demand(supplier,
                        area_min=0,res_min=0,res_max=1,com_min=0,com_max=1,ind_min=0,ind_max=1,farm_max=1):
    '''
    Get supplier demand data and characteristics
    args:
        supplier (string): the supplier name
        area_min (float): minimum area of site (km2)
        res_min (float): minimum residential fraction of site (0-1)
        res_max (float): maximum residential fraction of site (0-1)
        com_min (float): minimum commercial fraction of site (0-1)
        com_max (float): maximum commercial fraction of site (0-1)
        ind_min (float): minimum industrial fraction of site (0-1)
        ind_max (float): maximum industrial fraction of site (0-1)
        farm_max (float): maximum primary production fraction (0-1)
    return:
        demand (dataframe): the demand data
        info (dataframe): the metadata
    '''

    info = get_supplier_info(supplier)
    fnames = sorted(glob.glob(f'{datapath}/{supplier}/collated_standardized_{supplier}*.csv'))

    # get half hourly substation data and info
    demand = pd.concat([get_substation_data(fname) for fname in fnames], sort=False)

    # print any demand column that is not in info index
    print('following columns in demand are not in info index:')
    print(demand.columns[~demand.columns.isin(info.index)].tolist())
    print('removing these columns from demand')

    # remove columns in demand that don't appear in info index
    demand = demand.loc[:,demand.columns.isin(info.index)]

    print(f'number of substations in {supplier} substation info: {len(info)}')
    print(f'number of substations in {supplier} substation data: {len(demand.columns)}')

    # select sites based on area and land use
    sites = info.loc[select_sites(
        info,area_min,res_min,res_max,com_min,com_max,ind_min,ind_max,farm_max
        )].sort_values(by='Residential', ascending=False)
    print('following sites match selection criteria:')
    print(sites)
    demand = demand.loc[:,demand.columns.isin(sites.index)]
    info = info.loc[demand.columns]

    return demand, info

def clean_data(demand_orig,obs):

    demand = demand_orig.copy()

    # create temperature bins for later grouping
    bins = [-np.inf] + list(range(0, 55, 5)) + [np.inf]
    labels,key = ['<0'] + [f'{i}-{i+4}' for i in range(0, 50, 5)] + ['>50'], 't2m'
    obs[f'{key}_bin'] = pd.cut(obs[key], bins=bins, labels=labels)

    print('removing negative values')
    demand = demand.where(demand>0)

    print(f'removing values outside of 5 standard deviations, within {key} bins')
    demand = demand.groupby(obs[f'{key}_bin'], observed=False, group_keys=False).apply(clean_data_sigma,sigma=5)

    print('removing constant values')
    demand = clean_data_constant(demand,window='2h')

    if fill_gaps:
        print('linearly filling gaps')
        demand = demand.apply(linearly_fill_gaps, max_gap=4, result_type='expand')
    else:
        print('not filling gaps, set fill_gaps=True to enable')

    return demand

def clean_data_sigma(df,sigma):
    '''a function that cleans data outside of x standard deviations'''
    # calculate mean and standard deviation
    mean = df.mean()
    std = df.std()
    # calculate upper and lower bounds
    lower = mean - sigma*std
    upper = mean + sigma*std
    # replace values outside of bounds with nan
    df = df.where((df > lower) & (df < upper))

    return df

def clean_data_constant(df,window='2h'):
    '''a function that cleans data that is constant for more than x hours'''
    # calculate rolling standard deviation
    std = df.rolling(window=window).std()
    # replace values outside of bounds with nan
    mean = df.mean()
    df = df.where(std > mean/1000)

    return df

def linearly_fill_gaps(ser_to_fill : pd.Series, max_gap=4) -> pd.Series:
    ''' linearly fill gaps where gap is smaller than max_gap
    args
        ser_to_fill (pd.Series): the series to fill
        max_gap (int): the maximum gap to fill
    return
        filled (pd.Series): the filled series
    '''

    new_group_list = []

    ser_test = ser_to_fill.copy()

    # break series into groups (unless series is shorter than max_gap)
    if max_gap < len(ser_test):

        # find break points
        isna = pd.Series( np.where(ser_test.isna(), 1, np.nan), index=ser_test.index )
        isna_sum = isna
        for n in range(1,max_gap+1):
            isna_sum = isna_sum + isna.shift(n)
        break_idxs = isna_sum.dropna().index

        # # add start series
        prev_break = ser_test.index[0]
        
        for next_break in break_idxs:
            group = ser_test[prev_break:next_break]

            # skip to next loop if no values in group (for efficiency)
            if group.count() == 0:
                continue

            new_group = group.interpolate(method='linear',limit=max_gap, limit_area='inside')
            new_group_list.append(new_group)

            prev_break = next_break

        # append final group without interpolation
        group = ser_test[prev_break:]

    else: #simply group entire series
        group = ser_test

    new_group = group.interpolate(method='linear',limit=max_gap, limit_area='inside')
    new_group_list.append(new_group)

    # concatenate all groups
    filled = pd.concat(new_group_list).sort_index()
    filled = filled[~filled.index.duplicated(keep='first')]

    assert len(filled) == len(ser_to_fill), 'length of filled series is different to original'
    print('values filled linearly: %s ' %(filled.count() - ser_to_fill.count()))

    return filled

def select_sites(info,area_min,res_min,res_max,com_min,com_max,ind_min,ind_max,farm_max):
    '''
    Selects sites based on area and land use
    args:
        info (dataframe): the supplier info dataframe
        area_min (float): minimum area of site
        res_min (float): minimum residential fraction of site
        res_max (float): maximum residential fraction of site
        com_min (float): minimum commercial fraction of site
        com_max (float): maximum commercial fraction of site
        ind_min (float): minimum industrial fraction of site
        ind_max (float): maximum industrial fraction of site
        farm_max (float): maximum primary production fraction
    return:
        sites (list): list of sites
    '''

    # select sites based on area and land use
    sites = info[(info['Area']>area_min) & 
                (info['Residential']>res_min) & 
                (info['Residential']<res_max) &
                (info['Commercial']>com_min) &
                (info['Commercial']<com_max) & 
                (info['Industrial']>ind_min) &
                (info['Industrial']<ind_max) &
                (info['Primary Production']<farm_max)].index.to_list()

    return sites

###############################################################################

if __name__ == '__main__':

    for domain in domains:
        suppliers, supplier, obs_fpath = get_domain_info(domain)
        demand, info, obs = get_demand_data(suppliers,domain,sdate,edate)

        print('\nsubstation info:\n', info)
        print('\ncleaned demand data:\n', demand)
        print('\nBoM obs:\n', obs)

        print('Varibles: demand, info, obs')
