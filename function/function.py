def round_to_nearest(number, distance):

    a= round(number / distance) * distance
    return a


rounded_number = round_to_nearest(22525, 50)
print(rounded_number)