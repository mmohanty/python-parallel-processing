import re
from itertools import permutations


def parse_date(date_str):
    # Split the date using any of the allowed separators
    parts = re.split(r"[\/\-.]", date_str)
    if len(parts) != 3:
        return None  # Invalid format
    return parts


def compare_dates(str1, str2):
    date1 = parse_date(str1)
    date2 = parse_date(str2)

    if not date1 or not date2:
        return "MISMATCH"  # Invalid input

    # Extract possible year
    possible_years1 = [date1[i] for i in range(3) if len(date1[i]) == 4]
    possible_years2 = [date2[i] for i in range(3) if len(date2[i]) == 4]

    if not possible_years1 or not possible_years2 or possible_years1[0] != possible_years2[0]:
        return "MISMATCH"  # Years must match

    year = possible_years1[0]  # Common year
    remaining1 = [x for x in date1 if x != year]
    remaining2 = [x for x in date2 if x != year]

    # Check all possible day-month combinations
    for perm1 in permutations(remaining1):
        for perm2 in permutations(remaining2):
            if perm1 == perm2:
                return "MATCH"

    return "MISMATCH"


# Example usage
print(compare_dates("12/03/2024", "03-12-2024"))  # MATCH
print(compare_dates("2024.12.03", "2024/03/12"))  # MATCH
print(compare_dates("03-12-2024", "12-04-2024"))  # MISMATCH
