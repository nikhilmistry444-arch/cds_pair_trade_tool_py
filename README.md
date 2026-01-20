# cds_pair_trades


beta_historically_adjusted_attribute_changes (disabled, beta on top level regression not single name basis):
	- Takes rating, sector, duration etc
	- Ratings change adjustor. 60days. Assume spreads adopt for it in advance so assume proxy rating
	- Calculate beta per day. Then beta from now/today vs that day to create a ratio to multiply historic quotes to realign historical quotes

forward_momentum (0 assigned for unbias testing):
	- Formula on ticker(60%), sector, region
	- Find sector average spread by rating
	- Take tickers rating, find spread ranges from one rating to another and find ratio based on momentum score (-5downgrade,+5upgrade). This finds spread move

cds_upfront_calculator:
	- Uses IR swap curve, currency, coupon frequency, Running Coupon-Traded Spread, maturity

compute_daily_vm_im_margin_macro:
	- Takes product, IG/HY, delta for IM. Takes running PnL/default loss for variable margin (very conservative approach. Depends on institution). [Cash Usage = Upfront + Margin]

calculate_transaction_cost_bp (input):
	- Takes product, attach/detach, duration. Predefined t cost for each product tranche in module. Formula to compute a conservative bid-ask range based on duration
	- Bid-Ask done by taking EOD mids, then tcost/2. +/- onto mid quote

liquidity_analysis (input):
	- Input Excel. Set OTR 5 year max notional. Formula based. How old the series, and what level of tranche. Multiplied against OTR Notional

historic_spread_ranges_generator:
	- Rolling 3 years, finds all quotes where the 'product' historically has a duration +/- 3 months of the current product evaluated
	- Finds max, min, average spread, percentile (bottom percentile distribution)

calculate_realised_loss_and_defaults: (input) 
	- For the exact product/series/seniority, find default count, total weight of the cds constituents. CDS defaults saved and added as and when

calculate_basis:
	- Basis/nav + basis list. Index->  cds constituent spread * weight. Tranches -> order highest to lowest spread. Generate cumulative loss by multiplying by spread * weight * 1-r. Find CDS constituents involved for attach/detach. Use those names and spread to generate basis 
	- Super senior, likely not have CDS. Set basis/NAV as 0. Very Unlikely that tranche gets impacted

cds_index_tranche_rolldown_carry (set to 12months, R+C same series):
	- R+C Same Series -> same product and series with tenor(1,3,5,7,10yr) ranked in order. Then Linear rolldown
	- R+C Diff Series -> same product with all series ordered by duration. Then taking rolldown from that
	- Formula : (spread quote x carry years) + (rolldown spread x duration at end of carry date) - cash usage carry

RC signal:
	- If rtn on cash > required threshold. 0-1 rtn risk over index spread quote, vs >1. factors cash usage against rtn to risk

net_carry_to_maturity_bps:
	- Notional * spread quote * years to maturity

net_carry_to_maturity_default_bps:
	- Net carry to maturity +/- default payout (default count * loss rate * extrapolate on duration)
	- CDX IG/MAIN 3 names, CDX HY/XOVER 8 names. IG 40% recovery, HY 20% recovery

beta_historical_regression_function:
	- Calculate beta by historical regression by 'rolling tenor of product'
	- Lookback 6 months too. Takes max z score of p1(since inception start) and p2 (lookback start) for z score
	- Ensure z scores same direction -> shows trade hasn’t reverted and recent moves aren't permanent

Z-score analysis:
	- T cost threshold: Z score of current difference * std of current difference * min(fx rate) > t cost fx adjusted (assume move can only one leg worst case)
	- Z score difference>0, (1: sell protection, 2: buy protection)
	- Directional Analysis/Net return cr01 move: (duration1 * zscore1*std1 *fx rate) - (duration2 * zscore2*std2 *fx rate*1/beta ratio)
		○ net_direction_cr01_move = direction_1_cr01_move - direction_2_cr01_move - net_direction_cr01_transaction_cost
		○ pair_sub_direction_matters == 'Y': net_direction_cr01_move >0. Beta and duration mutually exclusive

Results Evaluators:
	- Net Carry > 0, signal_score += net_core_signal_value
		○ Net Basis: signal_score += signal_score * net_basis/(net_basis+(10/net_basis))
			§ Net r+c: signal_score += net_core_signal_value * (net_r_c/net_carry)
				□ Subordination rank: str('<long risk senior vs buy risk junior>') str('<pair with primary defaults returns>')str('<default underlying payout from excess notional. same index series>')
		○ Fast Liquid Dislocation (IG, HY)
		
	








 


<img width="1302" height="2393" alt="image" src="https://github.com/user-attachments/assets/71d616de-2c0a-4a7e-a2cd-9f5f072793f1" />
