from datetime import datetime

def option_name(date, K, right, currency='BTC'):
    """
    Generates option name in Deribit universe
    :param date: %Y-%m-%d format (string)
    :param K: strike price (int)
    :param right: put('P') or call('C')
    :return:
    """
    date_dt = datetime.strptime(date, format='%Y-%m-%d')
    date_str = date_dt.strftime(fmt="%d%B%Y")

    return "{currency}-{date}-{strike}-{right}".format(currency=currency,
                                              date=date_str,
                                              strike=K,
                                              right=right)


def to_timestamp(dt):
    return int(dt.timestamp() * 1000)


def from_timestamp(ts):
    return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%dT%H:%M:%S.%f')
