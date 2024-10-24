import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import mysql.connector  # Import MySQL connector
from mysql.connector import Error

class AgentActivationApp:
    
    def __init__(self, root):
        self.root = root
        self.root.title("Agent Activation")

        # Initialize file paths and data frames
        self.paths = {}
        self.final_transactions_database = pd.DataFrame()
        
        # Create and place widgets
        self.create_widgets()
    
    def create_widgets(self):
        # Create labels and entries for each file path
        file_types = ["USERDUMP", "AEPS", "DMT", "BC", "PAAM", "Branch Master"]
        for i, file_type in enumerate(file_types):
            tk.Label(self.root, text=f"{file_type} File Path:").grid(row=i, column=0, padx=5, pady=5, sticky="e")
            entry_widget = tk.Entry(self.root, width=50)
            entry_widget.grid(row=i, column=1, padx=5, pady=5, sticky="w")
            setattr(self, f"{file_type.lower()}_entry", entry_widget)
            tk.Button(self.root, text=f"Import {file_type} File", command=lambda ft=file_type.lower(): self.import_file(ft)).grid(row=i, column=2, padx=5, pady=5)

        # Process Data Button
        self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
        self.process_button.grid(row=len(file_types), column=0, columnspan=3, pady=10)
        
        # Export Data Button
        self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
        self.export_button.grid(row=len(file_types) + 1, column=0, columnspan=3, pady=10)

        # Send to MySQL Button
        self.mysql_button = tk.Button(self.root, text="Send to MySQL", command=self.send_to_mysql, state=tk.DISABLED)
        self.mysql_button.grid(row=len(file_types) + 2, column=1, pady=10)

    def import_file(self, file_type):
        """Prompt user to select a file and update the file paths dictionary and Entry widget."""
        file_path = filedialog.askopenfilename(title=f"Select {file_type.upper()} File", filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            self.paths[file_type] = file_path
            entry_widget = getattr(self, f"{file_type}_entry")
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, file_path)
            
            # Enable the process button if all paths are set
            if all(self.paths.values()):
                self.process_button.config(state=tk.NORMAL)

    def process_data(self):
        try:
            userpath = self.paths['userdump']
            aepspath = self.paths['aeps']
            dmtpath = self.paths['dmt']
            bcpath = self.paths['bc']
            paampath = self.paths['paam']
            branchmaster = self.paths['branch master']
            
            # Read user data
            df = pd.read_excel(userpath)
            new_df = df[['Username', 'PANCard', 'KYCApprovedDate']].drop_duplicates(subset='Username')
            
            # Read transaction data
            aeps = pd.read_excel(aepspath)
            dmt = pd.read_excel(dmtpath)
            BC = pd.read_excel(bcpath)
            DMT = dmt[['Operator', 'Username', 'Status', 'RechargeDate', 'ServiceId']]
            AEPS = aeps[['Operator', 'Username', 'Status', 'RechargeDate', 'ServiceId']]
            digi_df = pd.concat([DMT, AEPS], ignore_index=True)
            digipay = pd.merge(digi_df, new_df, on='Username', how='left')
            digipay['RechargeDate'] = pd.to_datetime(digipay['RechargeDate'])
            digipay['Date'] = digipay['RechargeDate'].dt.date
            digipayfinal = pd.merge(digipay, BC, on='Operator', how='left')
            digipay_filter = digipayfinal[(digipayfinal['Segments'] != 'No Revenue') & (digipayfinal['Status'] == 'Success')]
            
            # Prepare digipay data
            final_digi = digipay_filter[['PANCard',  'Status','Segments', 'Date']]
            # final_digi.rename(columns={'PANCard': 'PAN'}, inplace=True)
            final_digi.rename(columns={'Status': 'Count'}, inplace=True)
            pivot_digi = final_digi.groupby(['PANCard','Date', 'Segments']).count().reset_index()
            pivot_digi.rename(columns={'PANCard': 'PAN'}, inplace=True)
            
            # Read PAAM data
            paam_data = pd.read_excel(paampath)
            paam_data['PAN'] = paam_data['No Of PAN Resident'] + paam_data['No Of PAN NRI']
            paam_data['TAN'] = paam_data['No Of App TAN']
            paam_data['TDS'] = paam_data['< 100 eTDS'] + paam_data['100 To 1000 eTDS'] + paam_data['> 1000 eTDS']
            paam_final = paam_data[['Date', 'Branch Code', 'PAN', 'TAN', 'TDS']]
            paam_unpivot = pd.melt(paam_final, id_vars=['Date', 'Branch Code'], var_name='Segments', value_name='Count')
            paam_filter = paam_unpivot[paam_unpivot['Count'] != 0]

            # Read Branch Master data
            branch_master = pd.read_excel(branchmaster)
            b_master = branch_master[['Branch_code', 'PanNumber']]
            b_master['PanNumber'].fillna(b_master['Branch_code'], inplace=True)
            b_master.rename(columns={'Branch_code': 'Branch Code'}, inplace=True)
            merged_paam = pd.merge(paam_filter, b_master, on='Branch Code')
            merged_paam.drop(columns=['Branch Code'], inplace=True)
            pivot_paam = merged_paam.groupby(['Date', 'PanNumber', 'Segments']).sum().reset_index()
            pivot_paam.rename(columns={'PanNumber': 'PANCard'}, inplace=True)
            pivot_paam.rename(columns={'PANCard': 'PAN'}, inplace=True)
            # Combine results
            self.final_transactions_database = pd.concat([pivot_digi, pivot_paam], ignore_index=True)

            messagebox.showinfo("Info", "Data processed successfully.")
            self.export_button.config(state=tk.NORMAL)  # Enable export button
            self.mysql_button.config(state=tk.NORMAL)  # Enable MySQL button after processing
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def export_data(self):
        try:
            if self.final_transactions_database.empty:
                messagebox.showwarning("Warning", "No data to export. Please process data first.")
                return

            file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                     filetypes=[("Excel files", "*.xlsx")],
                                                     title="Save FINAL TRANSACTION DATABASE File")
            if file_path:
                self.final_transactions_database.to_excel(file_path, index=False)
                messagebox.showinfo("Info", "Data exported successfully.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def send_to_mysql(self):
        """Send processed data to MySQL database."""
        connection = None  # Initialize connection variable
        try:
            # Ensure 'Date' is formatted correctly
            self.final_transactions_database['Date'] = pd.to_datetime(self.final_transactions_database['Date']).dt.date
            
            # Define MySQL connection parameters
            connection = mysql.connector.connect(
                host='localhost',
                database='naresh_db',
                user='root',
                password='Naresh@123',
            )

            if connection.is_connected():
                cursor = connection.cursor()

                # Create table if it does not exist, using an auto-increment ID
                create_table_query = """
                CREATE TABLE IF NOT EXISTS Agent_activation (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    PAN VARCHAR(50),
                    Count INT,
                    Segments VARCHAR(50),
                    Date DATE
                );
                """
                cursor.execute(create_table_query)

                # Prepare SQL insert statement
                insert_query = """
                INSERT INTO Agent_activation (PAN, Count, Segments, Date)
                VALUES (%s, %s, %s, %s);
                """
                
                # Insert data
                inserted_rows = 0
                for _, row in self.final_transactions_database.iterrows():
                    cursor.execute(insert_query, (row['PAN'], row['Count'], row['Segments'], row['Date']))
                    inserted_rows += 1
                    
                connection.commit()
                messagebox.showinfo("Info", f"Data sent to MySQL successfully. Inserted {inserted_rows} new records.")
        except Error as e:
            messagebox.showerror("Error", f"Error while connecting to MySQL: {e}")
        except Exception as e:
            messagebox.showerror("Error", str(e))
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

if __name__ == "__main__":
    root = tk.Tk()
    app = AgentActivationApp(root)
    root.mainloop()
