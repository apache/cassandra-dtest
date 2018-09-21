"""
This module is a data-creation utility which allows creating data using markdown-style tables.

For example, this 'data' string specifies data to be created in 5 rows.
            data = "
                |id| value          |
                |--+----------------|
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                "

To take the markdown-stye string above and insert data, call create_rows:

expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

create_rows returns a data structure which represents what the data _should_ be like in the database.
It's meant to be used in tests when comparing expected to actual data, for validation.

For more examples reference paging_test.py
"""
import re

from cassandra.concurrent import execute_concurrent_with_args


def strip(val):
    # remove spaces and pipes from beginning/end
    return val.strip().strip('|')


def parse_headers_into_list(data):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = list(map(strip, data.split('\n')))

    # remove any remaining empty lines (i.e. '') from data
    rows = [_f for _f in rows if _f]

    # separate headers from actual data and remove extra spaces from them
    headers = [str(h.strip()) for h in rows.pop(0).split('|')]
    return headers


def get_row_multiplier(row):
    # find prefix like *1234 meaning create 1,234 rows
    row_cells = [l.strip() for l in row.split('|')]
    m = re.findall(r'\*(\d+)$', row_cells[0])

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

    row_map = dict(list(zip(headers, row_cells)))

    if format_funcs:
        for colname, value in list(row_map.items()):
            func = format_funcs.get(colname)

            if func is not None:
                row_map[colname] = func(value)

    return row_map


def row_describes_data(row):
    """
    Returns True if this appears to be a row describing data, otherwise False.

    Meant to be used in conjunction with filter to prune out those rows
    that don't actually describe data, such as empty strings or decorations
    that delimit headers from actual data (i.e. '+----|----|-----+')
    """
    if row:
        if row.startswith('+') and row.endswith('+'):
            return False

        return True

    return False


def parse_data_into_dicts(data, format_funcs=None):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = list(map(strip, data.split('\n')))

    # remove any remaining empty/decoration lines (i.e. '') from data
    rows = list(filter(row_describes_data, rows))

    # remove headers
    headers = parse_headers_into_list(rows.pop(0))

    values = []

    for row in rows:
        if row_has_multiplier(row):
            values.extend(parse_row_into_dict(row, headers, format_funcs=format_funcs))
        else:
            values.append(parse_row_into_dict(row, headers, format_funcs=format_funcs))

    return values


def create_rows(data, session, table_name, cl=None, format_funcs=None, prefix='', postfix=''):
    """
    Creates db rows using given session, with table name provided,
    using data formatted like:

    |colname1|colname2|
    +--------+--------+
    |value2  |value2  |

    format_funcs should be a dictionary of {columnname: function} if data needs to be formatted
    before being included in CQL.

    Returns a list of maps describing the data created.
    """
    values = []
    dicts = parse_data_into_dicts(data, format_funcs=format_funcs)

    # use the first dictionary to build a prepared statement for all
    prepared = session.prepare(
        "{prefix} INSERT INTO {table} ({cols}) values ({vals}) {postfix}".format(
            prefix=prefix, table=table_name, cols=', '.join(list(dicts[0].keys())),
            vals=', '.join('?' for k in list(dicts[0].keys())), postfix=postfix)
    )
    if cl is not None:
        prepared.consistency_level = cl

    query_results = execute_concurrent_with_args(session, prepared, [list(d.values()) for d in dicts])

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
