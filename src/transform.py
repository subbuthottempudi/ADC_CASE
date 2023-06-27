from dateutil.parser import parser, parse



def convert_str_to_number(x):
    total_stars = 0
    num_map = {'K':1000, 'M':1000000, 'B':1000000000}
    if x.isdigit():
        total_stars = int(x)
    else:
        if len(x) > 1:
            total_stars = float(x[:-1]) * num_map.get(x[-1].upper(), 1)
    return int(total_stars)


def convert_str_to_months(x):
    str_months = x.split(".")
    months = (int(str_months[0])* 12)
    print(months)
    return months


def convert_decimal_to_int(x):
    int_value = int(round(x))
    print(int_value)
    return int_value




# Check if passed date is complete and valid
def is_date_complete (date, must_have_attr=("year","month","day")):
    parse_res, _ = parser()._parse(date)
    # If date is invalid `_parse` returns (None,None)
    if parse_res is None:
        return False
    # For a valid date `_result` object is returned. E.g. _parse("Sep 23") returns (_result(month=9, day=23), None)
    for attr in must_have_attr:
        if getattr(parse_res,attr) is None:
            return False
    return True

## your code section
if not is_date_complete(date):
    return 'invalid'
else:
    return parse(date).isoformat()

complete_date = 'May 4, 2017'
_parse(complete_date, default=jan_01_2001)



DEFAULT_DATE = datetime.datetime(datetime.MINYEAR, 1, 1)
def parse_no_default(dt_str):    
    dt = parser.parse(dt_str, default=DEFAULT_DATE).date()
    time = parser.parse(dt_str, default=datetime.datetime(2019, 10, 14, 00, 00, 00), yearfirst=True).replace(tzinfo=pytz.utc)

    if dt != DEFAULT_DATE:
       return dt
    else:
       return None