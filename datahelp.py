import re
from cassandra.concurrent import execute_concurrent_with_args


def strip(val):
    # remove spaces and pipes from beginning/end
    return val.strip().strip('|')


def parse_headers_into_list(data):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))

    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)

    # separate headers from actual data and remove extra spaces from them
    headers = [unicode(h.strip()) for h in rows.pop(0).split('|')]
    return headers


def get_row_multiplier(row):
    # find prefix like *1234 meaning create 1,234 rows
    row_cells = [l.strip() for l in row.split('|')]
    m = re.findall('\*(\d+)$', row_cells[0])

    if m:
        return int(m[0])

    return None


def row_has_multiplier(row):
    if get_row_multiplier(row) is not None:
        return True

    return False


def parse_row_into_dict(row, headers, format_funcs=None):
    row_cells = [l.strip() for l in row.split('|')]

    if row_has_multiplier(row):
        row_multiplier = get_row_multiplier(row)
        row = '|'.join(row_cells[1:])  # cram remainder of row back into foo|bar format
        multirows = []

        for i in range(row_multiplier):
            multirows.append(
                parse_row_into_dict(row, headers, format_funcs=format_funcs)
            )
        return multirows

    row_map = dict(zip(headers, row_cells))

    if format_funcs:
        for colname, value in row_map.items():
            func = format_funcs.get(colname)

            if func is not None:
                row_map[colname] = func(value)

    return row_map


def parse_data_into_dicts(data, format_funcs=None):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))

    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)

    # remove headers
    headers = parse_headers_into_list(rows.pop(0))

    values = []

    for row in rows:
        if row_has_multiplier(row):
            values.extend(parse_row_into_dict(row, headers, format_funcs=format_funcs))
        else:
            values.append(parse_row_into_dict(row, headers, format_funcs=format_funcs))

    return values


def create_rows(data, cursor, table_name, cl=None, format_funcs=None, prefix='', postfix=''):
    """
    Creates db rows using given cursor, with table name provided,
    using data formatted like:

    |colname1|colname2|
    |value2  |value2  |

    format_funcs should be a dictionary of {columnname: function} if data needs to be formatted
    before being included in CQL.

    Returns a list of maps describing the data created.
    """
    values = []
    dicts = parse_data_into_dicts(data, format_funcs=format_funcs)

    # use the first dictionary to build a prepared statement for all
    prepared = cursor.prepare(
        "{prefix} INSERT INTO {table} ({cols}) values ({vals}) {postfix}".format(
            prefix=prefix, table=table_name, cols=', '.join(dicts[0].keys()),
            vals=', '.join('?' for k in dicts[0].keys()), postfix=postfix)
    )
    if cl is not None:
        prepared.consistency_level = cl

    query_results = execute_concurrent_with_args(cursor, prepared, [d.values() for d in dicts])

    for i, (status, result_or_exc) in enumerate(query_results):
        # should maybe check status here before appening to expected values
        values.append(dicts[i])

    return values


def flatten_into_set(iterable):
    # use flatten() then convert to a set for set comparisons
    return set(flatten(iterable))


def flatten(list_of_dicts):
    # flatten list of dicts into list of strings for easier comparison
    # and easier set membership testing (e.g. foo is subset of bar)
    flattened = []

    for _dict in list_of_dicts:
        sorted_keys = sorted(_dict)
        items = ['{}__{}'.format(k, _dict[k]) for k in sorted_keys]
        flattened.append('__'.join(items))

    return flattened
