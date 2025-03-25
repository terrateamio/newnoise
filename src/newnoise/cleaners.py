from . import data

def usage_type(row):
    """
    the usagetype for ec2 instances has meaningful deviations
    from how the data normally looks. it must be cleaned
    before matching.
    """
    if 'usagetype' in row[data.ATTRIBUTES]:
        ut = row[data.ATTRIBUTES]['usagetype']
        # take part before :
        ut_parts = ut.split(':')
        if len(ut_parts) > 1:
            ut = ut_parts[0]
        # take part after hyphen
        ut_parts = ut.split('-')
        if len(ut_parts) > 1:
            ut = ut_parts[1]
        return ut
    else:
        return None

