import sqlite3


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connection = sqlite3.connect('tickerTracker.db')
    cursor = connection.cursor()
    create_db = """
    CREATE TABLE fund (
    symbol VARCHAR(10) PRIMARY KEY
    """
    connection.close()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
