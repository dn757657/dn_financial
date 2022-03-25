from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time

DATE_FORMAT = '%Y-%m-%d'


def parse_prefix(line, fmt):
    """ parse date into desired format without knowing its format - returns datetime """
    try:
        t = datetime.strptime(line, fmt)
    except ValueError as v:
        if len(v.args) > 0 and v.args[0].startswith('unconverted data remains: '):
            line = line[:-(len(v.args[0]) - 26)]
            t = datetime.strptime(line, fmt)
        else:
            raise
    return t


def populate_dates(x_min, x_max, resolution):
    """ return list of tuples of x_data containing lower and upper bounds of x points"""
    x_data = []
    lower_bound = x_min
    upper_bound = x_min + resolution

    while upper_bound <= x_max:
        x_data.append((lower_bound, upper_bound - timedelta(days=1)))
        lower_bound += resolution
        upper_bound += resolution

    # for item in x_data:
    #     print(item)
    # print("\n\n")
    return x_data


def get_date_res(window_unit, window_size):
    """ return resolution as timedelta given string window unit """

    window = None
    if window_unit == 'day':
        window = relativedelta(days=int(window_size))
    elif window_unit == 'week':
        window = relativedelta(weeks=int(window_size))
    elif window_unit == 'month':
        window = relativedelta(months=int(window_size))
    elif window_size == 'year':
        window = relativedelta(years=int(window_size))

    return window


def get_first_day(date, period):
    """ get first day of denoted period

    args:
        date            datetime object within period
        period          string = week, month, year

    return:
        first day of period as datetime
    """

    if period == 'week':
        x_min = date - timedelta(days=date.isoweekday() % 7)
    elif period == 'month':
        x_min = date.replace(day=1)
    elif period == 'year':
        x_min = date.replace(day=1, month=1)


def process_window(window_size=None, window_unit=None, x_min=None, x_max=None):
    """ return [x_min and x_max] as x param data type based on window params and passed x value

    :argument
        x_min               datetime or integer object indicating minimum x value
        x_max               datetime or integer object indicating maximum x value
        window_size         integer size of display range/window
        window_unit         integer size of display range/window
    """
    # define window size
    window = None

    if isinstance(x_min, datetime) or isinstance(x_max, datetime):
        # determine window if window method used
        if window_size and window_unit:
            window = get_date_res(window_unit=window_unit, window_size=window_size)

        # set empty window limit based on window and passed limit
        if x_min:
            x_max = x_min + window
        elif x_max:
            x_min = x_max - window

    elif isinstance(x_min, int) or isinstance(x_max, int):
        # window unit not required for int types
        if x_min and window:
            x_max = x_min + window_size

        elif x_max and window:
            x_min = x_max - window_size

    else:
        print("x_min and x_max are of incompatible type")

    # x_data = populate_x_data(x_min=x_min, x_max=x_max, resolution=resolution)

    return [x_min, x_max]