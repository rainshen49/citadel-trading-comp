# citadel-trading-comp
Citadel Trading Competition Code

# Strategies

- Main/Alternative Exchange Arbitradge
  - When the bid-ask spreads of the same underlying company cross on two exchanges, arbitradge
  - Detail: when cross-over is big, send a market order for ensured execution. when cross-over is small, send limit order to ensure execution price, with the risk of partially stuck in an order
 
- Index Arbitradge
  - Similar to exchange arbitradge, take the sum of highest bids of constituents of an ETF, and compare with ETF ask. Arbitradge if there is an opportunity. Vice versa using lowest asks of constituents with ETF bid
 
- News response
  - When a negative shock is informed, immediately short. 3 ticks later, long same amount. Both are market orders for timely execution. 
  - When a positive shock is informed, the other way round
  - For small shocks, ignore
