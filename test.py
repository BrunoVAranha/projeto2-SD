import webbrowser

# Prompt the user to input a status code
status_code = input("Enter the status code (e.g., 404, 500): ")

# Construct the URL with the provided status code
url = f"https://http.cat/{status_code}"

# Open the URL in the default web browser
try:
    webbrowser.open(url)
    print(f"URL opened successfully: {url}")
except Exception as e:
    print(f"An error occurred: {e}")
