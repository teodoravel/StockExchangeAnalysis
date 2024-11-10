import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('publishers.db')
cursor = conn.cursor()

# Execute a query to see the data
cursor.execute("SELECT * FROM publishers")

# Fetch all the results
rows = cursor.fetchall()

# Print the results
for row in rows:
    print(row)

# Close the connection
conn.close()
