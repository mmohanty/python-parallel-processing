def format_number(num_str):
    num = float(num_str)  # Convert string to float
    if num.is_integer():
        return str(int(num))  # Remove decimal if it's an integer
    else:
        return f"{num:.2f}".rstrip('0').rstrip('.')  # Format to 2 decimal places, remove trailing zeros




if __name__ == '__main__':
    # Test cases
    test_numbers = ["8.9000", "8.92333", "8.926666", "8.0", "8.00", "8", "8.01", "8.001"]
    formatted_numbers = [format_number(num) for num in test_numbers]

    print(formatted_numbers)  # Output: ['8.9', '8.92', '8.92', '8', '8', '8']