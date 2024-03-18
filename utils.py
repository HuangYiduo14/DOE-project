


def manhattan(a, b):
    '''
    :param a: list like [x0,x1,x2,...]
    :param b: list like [x0,x1,x2,...]
    :return: manhattan dist from a to b
    '''
    return sum(abs(val1-val2) for val1, val2 in zip(a,b))