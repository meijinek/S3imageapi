from decimal import Decimal


#  this is needed because dynamodb returns float type as Decimal object which cannot be json serialised
def decimal_to_float(dct):
    if isinstance(dct, dict):
        for k, v in dct.items():
            if isinstance(v, dict) or isinstance(v, list):
                decimal_to_float(v)
            elif isinstance(v, Decimal):
                dct[k] = float(v)
    elif isinstance(dct, list):
        for i in range(len(dct)):
            if isinstance(dct[i], dict) or isinstance(dct[i], list):
                decimal_to_float(dct[i])
            elif isinstance(dct[i], Decimal):
                dct[i] = float(dct[i])

    return dct


def convert_to_decimal(value):
    #  if the float is not converted to string then Decimal adds a silly number of digits after .
    return Decimal(str(value))


def remove_image_from_dicts(lst):
    for dct in lst:
        try:
            del dct['image']
        except KeyError as ex:
            print(f'Image does not exist for item {dct["name"]}', ex)

    return lst
