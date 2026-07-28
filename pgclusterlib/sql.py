def quote_ident(value):
    return '"%s"' % value.replace('"', '""')


def quote_literal(value):
    return "'%s'" % str(value).replace("'", "''")
