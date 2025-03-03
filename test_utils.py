import re
import csv
from thefuzz import fuzz, process  # thefuzz is the updated version of fuzzywuzzy


def split_tokens(text):
    return re.split(r'[;,]', text)


def format_number(num_str):
    num = float(num_str)  # Convert string to float
    if num.is_integer():
        return str(int(num))  # Remove decimal if it's an integer
    else:
        return f"{num:.2f}".rstrip('0').rstrip('.')  # Format to 2 decimal places, remove trailing zeros


def parse_names(name_string):
    """Correctly splits a comma-separated string while handling names with commas."""
    return list(csv.reader([name_string]))[0]


def find_name_match(name, name_string, threshold=85):
    """Finds an exact or close match for a name in a comma-separated list."""
    names_list = parse_names(name_string)  # Parse names correctly

    if name in names_list:
        return f"Exact match found: {name}"

    # Use fuzzy matching to find the best close match
    best_match, score = process.extractOne(name, names_list)

    if score >= threshold:
        return f"Close match found: {best_match} (Similarity: {score}%)"

    return "No match found"


def clean_text(text):
    # Remove special characters (except alphanumeric and spaces) and replace with space
    text = re.sub(r'[^\w\s]', ' ', text)

    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text).strip()

    return text

if __name__ == '__main__':
    # Test cases
    test_numbers = ["8.9000", "8.92333", "8.926666", "8.0", "8.00", "8", "8.01", "8.001"]
    formatted_numbers = [format_number(num) for num in test_numbers]

    print(formatted_numbers)  # Output: ['8.9', '8.92', '8.92', '8', '8', '8']

    test_string = "apple,banana;cherry,grape;orange"
    tokens = split_tokens(test_string)
    print(tokens)  # Output: ['apple', 'banana', 'cherry', 'grape', 'orange']

    # Example usage
    name_list = 'John, Doe, Alice, Jane, Smith, Bob'
    name_to_search = "Jane Smith"

    result = find_name_match(name_to_search, name_list)
    print(result)

    # Example Usage
    input_text = "Hello!!!   World, this  is   a   test!!  "
    cleaned_text = clean_text(input_text)
    print(cleaned_text)  # Output: "Hello World this is a test"