import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
from quantopian.pipeline.factors import CustomFactor
from quantopian.pipeline.data import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS, StaticAssets

import numpy as np
import pandas as pd

from quantopian.pipeline import Pipeline

from quantopian.pipeline.filters import Q500US

from quantopian.pipeline.data import Fundamentals
from quantopian.pipeline.data.builtin import USEquityPricing

def initialize(context):
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_open(hours=1),
    )

    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )

    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    q500us = Q500US()
    dollar_volume = AverageDollarVolume(mask=q500us, window_length=270)
   
    my_etfs = StaticAssets([sid(8554)]) # SPY

    screen = (dollar_volume.top(5) & q500us) | my_etfs
    
    kf = (AverageDollarVolume(mask=q500us, window_length=130) - AverageDollarVolume(mask=q500us, window_length=260)) > 0.0      
    kf.mask = screen

    pipe = Pipeline(
        columns = {'kf': kf})
    pipe.set_screen(screen)
    return pipe


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = algo.pipeline_output('pipeline')['kf']
    context.security_list = context.output.index

def rebalance(context, data):
    selected_stocks = []
    for stock in context.output.index:  
        v = context.output.at[stock] 
        if v > 0:
            selected_stocks.append(stock)
    for stock in list(context.portfolio.positions.iterkeys()):
        if stock not in selected_stocks:
            print("closing position for " + str(stock))
            order_target_percent(stock, 0)
    
    old_stocks = list(context.portfolio.positions.iterkeys())
    if set(old_stocks) != set(selected_stocks):
        for stock in selected_stocks:
            v = 1.0/len(selected_stocks)
            print("open position for " + str(stock) + " " + str(v))
            order_target_percent(stock, v)

def record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    record(leverage=context.account.leverage)



def handle_data(context, data):
    """
    Called every minute.
    """
    pass
