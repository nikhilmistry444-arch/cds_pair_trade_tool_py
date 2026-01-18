rating_factor = {
    'AAA': 5,
    'AA+': 5,
    'AA': 5,
    'AA-': 5,
    'A+': 5,
    'A': 5,
    'A-': 5,
    'BBB+': 5,
    'BBB': 5,
    'BBB-': 5,
    'BB+': 10,
    'BB': 15,
    'BB-': 15,
    'B+': 25,
    'B': 25,
    'B-': 25,
    'CCC+': 50,
    'CCC': 50,
    'CCC-': 50,
    'CC': 50,
    'C': 100,
    'D': 100,
    'DDD': 100,
    'WR': 5,
    'NR': 5,
    'Aaa': 5,
    'Aa1': 5,
    'Aa2': 5,
    'Aa3': 5,
    'A1': 5,
    'A2': 5,
    'A3': 5,
    'Baa1': 5,
    'Baa2': 5,
    'Baa3': 5,
    'Ba1': 10,
    'Ba2': 15,
    'Ba3': 20,
    'B1': 25,
    'B2': 25,
    'B3': 25,
    'Caa1': 50,
    'Caa2': 50,
    'Caa3': 50,
    'Ca': 100,
    'NULL': 7
}

###use the below to attribute a transaction cost to a trade since rating isnt given or available
seniority_factor = {
    '1st Lien': -2,
    '1.5 Lien': -2,
    '2nd Lien': -2,
    '3rd Lien': -2,
    'Asset Backed': -2,
    'Sr Secured': -2,
    'Secured': 0,
    'Sr Preferred': 0,
    'Sr Unsecured': 0,
    'Sr Unsec': 0,
    'Sr Non Preferred': 0,
    'Unsecured': 0,
    'Senior': 0,
    'Sr Subordinated': 5,
    'Subordinated': 5,
    'Sub': 5,
    'Jr Subordinated': 5,
    'Jr Sub': 5
}

tenor_factor = {
    '6M': 2,
    '1Y': 2,
    '2Y': 2,
    '3Y': 2,
    '4Y': 2,
    '5Y': 0,
    '7Y': 2,
    '10Y': 5,
    '15Y': 5,
    '20Y': 5,
    '30Y': 5
}

sector_multiplier = {
    'Communication Services': 0,
    'Consumer Discretionary': 0,
    'Consumer Staples': 0,
    'Energy': 0,
    'Financials': 0,
    'Government': 0,
    'Health Care': 0,
    'Industrials': 0,
    'Information Technology': 0,
    'Materials': 0,
    'Real Estate': 0,
    'Utilities': 0
}

cdx_em_index_tranche_bp_cost = {
    '0-0.15': 20,
    '0.15-0.25': 8,
    '0.25-0.35': 6,
    '0.35-1': 4,
    '0-1': 0.5
}

cdx_ig_index_tranche_bp_cost = {
    '0-0.03': 15,
    '0.03-0.07': 10,
    '0.07-0.15': 3,
    '0.15-1': 2,
    '0-1' : 0.35
}

cdx_hy_index_tranche_bp_cost = {
    '0-0.15': 25,
    '0.15-0.25': 12,
    '0.25-0.35': 9,
    '0.35-1': 3,
    '0-1': 3
}

itraxx_ig_index_tranche_bp_cost = {
    '0-0.03': 15,
    '0.03-0.06': 10,
    '0.06-0.12': 4,
    '0.12-1': 1,
    '0-1' : 0.3
}

itraxx_hy_index_tranche_bp_cost = {
    '0-0.1': 20,
    '0.1-0.2': 12,
    '0.2-0.35': 10,
    '0.35-1': 5,
    '0-1' : 3
}

def calculate_transaction_cost_bp(rating='NULL', seniority='Null', tenor='Null', sector='Null', product='Null', attachment='Null', detachment='Null',index_short_name='Null'):
    # add to adjust for liquidity of cds, whether its in main or cross in case no rating, or from a manual override list
    exclusion_list = ['WR', 'NR', 'NULL']
    if product == 'CDS':
        if rating in exclusion_list:
            cost_bp = 5 + seniority_factor[seniority] + tenor_factor[tenor]
        else:
            cost_bp = rating_factor[rating]

    elif (product == 'index') or (product == 'tranche'):
        att_detach = str(int(attachment) if attachment==1 or attachment==0 else attachment) + '-' + str(int(detachment) if detachment==1 or detachment==0 else detachment)
        if index_short_name == 'CDX HY':
            cost_bp = cdx_hy_index_tranche_bp_cost[att_detach]
        elif index_short_name == 'CDX IG':
            cost_bp = cdx_ig_index_tranche_bp_cost[att_detach]
        elif index_short_name == 'CDX EM':
            cost_bp = cdx_em_index_tranche_bp_cost[att_detach]
        elif index_short_name in  ['ITRAXX XOVER','ITRAXX FINS SUB']:
            cost_bp = itraxx_hy_index_tranche_bp_cost[att_detach]
        elif index_short_name in ['ITRAXX MAIN','ITRAXX FINS SNR']:
            cost_bp = itraxx_ig_index_tranche_bp_cost[att_detach]
        else:
            cost_bp = 3

        #multiply by maturity_years_date due to liquidity
        if (product == 'index') or (product == 'tranche'):
            if 0 <= tenor < 1:
                cost_bp = cost_bp * 2
            elif 1<= tenor < 2:
                cost_bp = cost_bp * 2
            elif 2 <= tenor < 3:
                cost_bp = cost_bp * 1.5
            elif 3 <= tenor < 4.4:
                cost_bp = cost_bp * 1.25
            elif 4.4 <= tenor < 5:
                cost_bp = cost_bp * 1
            elif 5 <= tenor < 7:
                cost_bp = cost_bp * 1.25
            elif 7 <= tenor < 10:
                cost_bp = cost_bp * 1.5
            elif 10 <= tenor < 15:
                cost_bp = cost_bp * 1.75
            else:
                cost_bp = cost_bp
    else:
        cost_bp = 5

    return cost_bp