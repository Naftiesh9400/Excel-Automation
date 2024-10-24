import pandas as pd
import mysql.connector
from mysql.connector import Error
import tkinter as tk
from tkinter import filedialog, messagebox

# Hard-coded database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'naresh_db',
    'user': 'root',
    'password': 'Naresh@123'
}

def create_database_if_not_exists(db_config):
    """Create database if it does not exist."""
    try:
        # Connect to MySQL server (without specifying a database)
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']};")
        connection.commit()
        print(f"Database '{db_config['database']}' checked/created successfully.")

    except Error as e:
        print(f"Error creating database: {e}")
        messagebox.showerror("Database Error", str(e))
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def upload_excel_to_mysql(excel_file_path, db_config):
    """Upload data from a CSV file to MySQL database."""
    connection = None
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(excel_file_path)
        print(f"DataFrame loaded successfully with {len(df)} rows.")

        # Ensure all necessary columns are present
        required_columns = ['PANcard', 'Count', 'Segments', 'Date']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Convert 'Date' column to the correct format
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')  # Convert to YYYY-MM-DD format

        # Drop rows with invalid dates (if any)
        df = df.dropna(subset=['Date'])

        # Connect to MySQL database
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor = connection.cursor()

            # Create table if it does not exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS agent_activation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                PAN VARCHAR(50),
                Count INT,
                Segments VARCHAR(50),
                Date DATE
            );
            """
            cursor.execute(create_table_query)

            # Prepare the insert statement
            insert_query = """
            INSERT INTO agent_activation (PAN, Count, Segments, Date)
            VALUES (%s, %s, %s, %s);
            """

            # Insert data in batches
            batch_size = 1000
            total_inserted = 0
            for start in range(0, len(df), batch_size):
                end = start + batch_size
                batch_data = df.iloc[start:end][required_columns].values.tolist()

                try:
                    cursor.executemany(insert_query, batch_data)
                    connection.commit()
                    total_inserted += len(batch_data)
                except Error as e:
                    print(f"Error inserting batch starting at row {start}: {e}")
                    connection.rollback()  # Roll back the transaction on error

            # Report total uploaded records
            total_attempted = len(df)
            print(f"Successfully uploaded {total_inserted} records out of {total_attempted} attempted.")
            messagebox.showinfo("Success", f"Successfully uploaded {total_inserted} records.")

    except ValueError as ve:
        print(f"Value Error: {ve}")
        messagebox.showerror("Value Error", str(ve))
    except Error as e:
        print(f"Database Error: {e}")
        messagebox.showerror("Database Error", str(e))
    except Exception as e:
        print(f"General Error: {e}")
        messagebox.showerror("Error", str(e))
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def select_file(entry):
    """Open a file dialog to select a CSV file and set it in the entry widget."""
    file_path = filedialog.askopenfilename(title="Select CSV File", filetypes=[("CSV files", "*.csv")])
    entry.delete(0, tk.END)  # Clear current entry
    entry.insert(0, file_path)  # Insert selected file path

def upload_data(entry_file):
    """Collect user inputs and call the upload function."""
    file_path = entry_file.get()
    create_database_if_not_exists(DB_CONFIG)  # Ensure database exists
    if file_path:
        upload_excel_to_mysql(file_path, DB_CONFIG)
    else:
        messagebox.showwarning("Warning", "Please select a file.")

def create_gui():
    """Create the main GUI window."""
    window = tk.Tk()
    window.title("Upload CSV to MySQL")

    # Labels and Entries
    tk.Label(window, text="CSV File Path:").grid(row=0, column=0, padx=10, pady=10)
    entry_file = tk.Entry(window, width=40)
    entry_file.grid(row=0, column=1, padx=10, pady=10)
    tk.Button(window, text="Browse", command=lambda: select_file(entry_file)).grid(row=0, column=2, padx=10, pady=10)

    tk.Button(window, text="Upload", command=lambda: upload_data(entry_file)).grid(row=1, columnspan=3, pady=20)

    window.mainloop()

if __name__ == "__main__":
    create_gui()
