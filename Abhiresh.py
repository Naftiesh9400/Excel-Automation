
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import mysql.connector  # Import MySQL connector
from PIL import Image, ImageTk  # For image handling
from mysql.connector import Error
import os
import time
      
class PAAMDataProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("PAAM All Transactions")
        
        # Set the window icon (ensure the path to 'data-transformation.ico' is correct)
        self.root.iconbitmap('data-transformation.ico')

        # Initialize widgets
        self.create_widgets()

        # Initialize variables
        self.paam = None
        self.processed_data = None
        self.db_connection = None  # Add a variable for DB connection

    def create_widgets(self):
        # File Path Label and Entry
        tk.Label(self.root, text="Data File Path:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.file_entry = tk.Entry(self.root, width=50)
        self.file_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        tk.Button(self.root, text="Import Data", command=self.import_data).grid(row=0, column=2, padx=5, pady=5)

        # Process Data Button
        self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
        self.process_button.grid(row=1, column=0, columnspan=3, pady=10)

        # Export Data Button
        self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
        self.export_button.grid(row=2, column=0, columnspan=3, pady=10)

        # Upload Data to MySQL Button
        self.upload_button = tk.Button(self.root, text="Upload Data to MySQL", command=self.upload_to_mysql, state=tk.DISABLED)
        self.upload_button.grid(row=3, column=0, columnspan=3, pady=10)

        # Connect to MySQL Button
        self.connect_button = tk.Button(self.root, text="Connect to MySQL", command=self.connect_mysql)
        self.connect_button.grid(row=4, column=0, columnspan=3, pady=10)

        # Footer with copyright notice
        self.footer_label = tk.Label(self.root, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
                                    relief=tk.SUNKEN, anchor='w')
        self.footer_label.grid(row=5, column=0, columnspan=3, sticky="w", pady=5)

    def connect_mysql(self):
        try:
            # self.processed_data['Date'] = pd.to_datetime(self.processed_data['Date']).dt.date
            
            # Connection parameters (customize as needed)
            self.db_connection = mysql.connector.connect(
                host='localhost',
                database='naresh_db',
                user='root',
                password='Naresh@123'
            )
            messagebox.showinfo("Success", "Connected to MySQL database!")
        except mysql.connector.Error as err:
            messagebox.showerror("Error", f"Failed to connect to MySQL: {err}")
            
    def import_data(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            try:
                self.paam = pd.read_excel(file_path)
                # Update Entry widget with the file path
                self.file_entry.delete(0, tk.END)
                self.file_entry.insert(0, file_path)
                self.process_button.config(state=tk.NORMAL)
                messagebox.showinfo("Success", "Data imported successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to import data: {e}")

    def process_data(self):
        if self.paam is not None:
            try:
                # Define COMM DataFrame
                Com_df = {
                    'Operator': ['PAN', 'TAN', 'M.PAN', 'TDS 0-<100', 'TDS 100-1000', 'TDS 1000>'],
                    'Segments': ['PAN', 'TAN', 'M.PAN', 'TDS', 'TDS', 'TDS'],
                    'Per unit Revenue': [29, 14, 17, 27.5, 135, 435],
                    'Per unit Payout': [12, 5, 7, 12.5, 67.5, 225]
                }
                COMM = pd.DataFrame(Com_df)

                # Process PAAM Data
                paam = self.paam.copy()
                paam['TDS 0-<100'] = paam['< 100 eTDS']
                paam['TDS 100-1000'] = paam['100 To 1000 eTDS']
                paam['TDS 1000>'] = paam['> 1000 eTDS']
                paam['PAN'] = paam['No Of PAN Resident'] + paam['No Of PAN NRI']
                paam['TAN'] = paam['No Of App TAN']

                paam['Month'] = paam['Date'].dt.strftime('%b')
                paam['Year'] = paam['Date'].dt.strftime('%y')
                paam['Trx_Month'] = paam['Month'] + '-' + paam['Year']
                PAAM = paam[["Trx_Month", "Branch Code", "TDS 0-<100", "TDS 100-1000", "TDS 1000>", "PAN", "TAN"]]
                PAAM = pd.melt(PAAM, id_vars=['Trx_Month', 'Branch Code'], var_name='Operator', value_name='Trx_Count')

                PAAM = PAAM[PAAM['Trx_Count'] != 0]

                paam = pd.merge(PAAM, COMM, how='left', on='Operator')

                paam['Revenue'] = paam['Trx_Count'] * paam['Per unit Revenue']
                paam['Payout'] = paam['Trx_Count'] * paam['Per unit Payout']
                paam_pivot = pd.pivot_table(paam, index=['Trx_Month', 'Branch Code', 'Segments'], values=['Trx_Count', 'Revenue', 'Payout'], aggfunc=sum).reset_index()

                # Save the processed data to an attribute for later export
                self.processed_data = paam_pivot

                # Enable the export button after processing
                self.export_button.config(state=tk.NORMAL)
                self.upload_button.config(state=tk.NORMAL)
                messagebox.showinfo("Success", "Data processed successfully!")

            except Exception as e:
                messagebox.showerror("Error", f"Failed to process data: {e}")

    def export_data(self):
        if hasattr(self, 'processed_data') and self.processed_data is not None:
            file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel Files", "*.xlsx")])
            if file_path:
                try:
                    self.processed_data.to_excel(file_path, index=False)
                    messagebox.showinfo("Success", "Data exported successfully!")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to export data: {e}")
        else:
            messagebox.showerror("Error", "No data to export.")

    def upload_to_mysql(self):
        if self.processed_data is not None and self.db_connection is not None:
            try:
                cursor = self.db_connection.cursor()
                
                # Create table if not exists
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS paam_data (
                    Trx_Month VARCHAR(255),
                    Branch_Code VARCHAR(255),
                    Segments VARCHAR(255),
                    Trx_Count INT,
                    Revenue DECIMAL(10, 2),
                    Payout DECIMAL(10, 2)
                )
                """)
                
                # Insert data into the table
                insert_query = """
                INSERT INTO paam_data (Trx_Month, Branch_Code, Segments, Trx_Count, Revenue, Payout)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                for index, row in self.processed_data.iterrows():
                    cursor.execute(insert_query, (
                        row['Trx_Month'],
                        row['Branch Code'],
                        row['Segments'],
                        row['Trx_Count'],
                        row['Revenue'],
                        row['Payout']
                    ))

                self.db_connection.commit()
                messagebox.showinfo("Success", "Data uploaded to MySQL successfully!")
            except mysql.connector.Error as err:
                messagebox.showerror("Error", f"Failed to upload data: {err}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to upload data: {e}")
        else:
            messagebox.showerror("Error", "No data to upload or not connected to MySQL.")

class FinancialInclusionApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Financial Inclusion Data Processor")

        # Initialize file paths
        self.file_paths = {"AEPS": "", "DMT": ""}
        self.data = None
        self.db_connection = None  # Add a variable for DB connection
        
        # Set the window icon (ensure the path to 'data-transformation.ico' is correct)
        self.root.iconbitmap('data-transformation.ico')
        
        # Create and grid widgets
        self.create_widgets()
    
    def create_widgets(self):
        # AEPS File Path Label and Entry
        self.aeps_path_label = tk.Label(self.root, text="AEPS File Path:")
        self.aeps_path_label.grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.aeps_path_entry = tk.Entry(self.root, width=50)
        self.aeps_path_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        # AEPS Import Button
        self.import_aeps_button = tk.Button(self.root, text="Import AEPS File", command=lambda: self.import_file("AEPS"))
        self.import_aeps_button.grid(row=0, column=2, padx=5, pady=5)

        # DMT File Path Label and Entry
        self.dmt_path_label = tk.Label(self.root, text="DMT File Path:")
        self.dmt_path_label.grid(row=1, column=0, padx=5, pady=5, sticky="e")
        self.dmt_path_entry = tk.Entry(self.root, width=50)
        self.dmt_path_entry.grid(row=1, column=1, padx=5, pady=5, sticky="w")

        # DMT Import Button
        self.import_dmt_button = tk.Button(self.root, text="Import DMT File", command=lambda: self.import_file("DMT"))
        self.import_dmt_button.grid(row=1, column=2, padx=5, pady=5)

        # Process Data Button
        self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
        self.process_button.grid(row=2, column=0, columnspan=3, pady=10)

        # Export Data Button
        self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
        self.export_button.grid(row=3, column=0, columnspan=3, pady=10)

        # Upload Data to MySQL Button
        self.upload_button = tk.Button(self.root, text="Upload Data to MySQL", command=self.upload_to_mysql, state=tk.DISABLED)
        self.upload_button.grid(row=4, column=0, columnspan=3, pady=10)

        # Connect to MySQL Button
        self.connect_button = tk.Button(self.root, text="Connect to MySQL", command=self.connect_mysql)
        self.connect_button.grid(row=5, column=0, columnspan=3, pady=10)

        # MySQL Connection Status Indicator
        self.status_indicator = tk.Label(self.root, text="", width=2, height=1, bg="red", relief=tk.RAISED)
        self.status_indicator.grid(row=5, column=3, padx=10, pady=10)

        # Footer with copyright notice
        self.footer_label = tk.Label(self.root, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
                                    relief=tk.SUNKEN, anchor='w')
        self.footer_label.grid(row=6, column=0, columnspan=4, sticky="w", pady=5)

       

    def connect_mysql(self):
        try:
            self.processed_data['Date'] = pd.to_datetime(self.processed_data['Date']).dt.date
            
            # Connection parameters (customize as needed)
            self.db_connection = mysql.connector.connect(
                host='localhost',
                database='naresh_db',
                user='root',
                password='Naresh@123'
            )
            messagebox.showinfo("Success", "Connected to MySQL database!")
        except mysql.connector.Error as err:
            messagebox.showerror("Error", f"Failed to connect to MySQL: {err}")
            
    def import_file(self, file_type):
        """Prompt user to select a file and update the file paths dictionary and Entry widget."""
        file_path = filedialog.askopenfilename(title=f"Select {file_type} File", filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            self.file_paths[file_type] = file_path
            if file_type == "AEPS":
                self.aeps_path_entry.delete(0, tk.END)
                self.aeps_path_entry.insert(0, file_path)
            elif file_type == "DMT":
                self.dmt_path_entry.delete(0, tk.END)
                self.dmt_path_entry.insert(0, file_path)
                
            messagebox.showinfo("Info", f"{file_type} file imported successfully!")
            if all(self.file_paths.values()):  # Enable processing if both files are selected
                self.process_button.config(state=tk.NORMAL)

    def process_data(self):
        try:
            AEPS = pd.read_excel(self.file_paths["AEPS"])
            DMT = pd.read_excel(self.file_paths["DMT"])
            
            digi = pd.concat([AEPS, DMT], axis=0)
            Digipay = digi[[ "Id", "OrderID", "Number", "RPCode", "Parent", "Operator", "Amount", "API", "UserId", "APIName",
                             "Status", "RechargeMode", "RechargeDate", "EditDate", "Username", "Balance", "Cost", "ChargePer",
                             "ServiceId", "RevertTran", "OperatorId", "OptID", "WhiteLabelID", "AD_CommAmt", "AD_ChargeAmt",
                             "MD_CommAmt", "MD_ChargeAmt", "ZBP_CommAmt", "ZBP_ChargeAmt", "CommAmt", "AD_UserId", "MD_UserId",
                             "ZBP_UserId", "Param2", "IfscCode", "ProfitAmount", "GSTAllow", "TDSAllow", "BeneName", "BankName",
                             "SenderName", "PayType", "SchemeName" ]]
            Digipay['Id'].fillna(0, inplace=True)
            self.data = Digipay[Digipay['Status'] == "Success"]

            messagebox.showinfo("Info", "Data processed successfully!")
            self.export_button.config(state=tk.NORMAL)
            self.upload_button.config(state=tk.NORMAL)
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during processing: {e}")

    def export_data(self):
        if self.data is not None:
            export_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")],
                title="Save Processed Data"
            )
            if export_path:
                try:
                    self.data.to_excel(export_path, index=False)
                    messagebox.showinfo("Info", "Data exported successfully!")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to export data: {e}")
        else:
            messagebox.showwarning("Warning", "No data to export. Please process data first.")
    def upload_to_mysql(self):
        if self.data is not None and self.db_connection is not None:
            try:
                cursor = self.db_connection.cursor()
                
                # Create table if not exists
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS financial_data (
                    Id INT,
                    OrderID VARCHAR(25),
                    Number VARCHAR(25),
                    RPCode VARCHAR(25),
                    Parent VARCHAR(25),
                    Operator VARCHAR(25),
                    Amount DECIMAL(10, 2),
                    API VARCHAR(25),
                    UserId VARCHAR(25),
                    APIName VARCHAR(25),
                    Status VARCHAR(25),
                    RechargeMode VARCHAR(25),
                    RechargeDate DATETIME,
                    EditDate DATETIME,
                    Username VARCHAR(25),
                    Balance DECIMAL(10, 2),
                    Cost DECIMAL(10, 2),
                    ChargePer DECIMAL(10, 2),
                    ServiceId VARCHAR(25),
                    RevertTran VARCHAR(25),
                    OperatorId VARCHAR(25),
                    OptID VARCHAR(25),
                    WhiteLabelID VARCHAR(25),
                    AD_CommAmt DECIMAL(10, 2),
                    AD_ChargeAmt DECIMAL(10, 2),
                    MD_CommAmt DECIMAL(10, 2),
                    MD_ChargeAmt DECIMAL(10, 2),
                    ZBP_CommAmt DECIMAL(10, 2),
                    ZBP_ChargeAmt DECIMAL(10, 2),
                    CommAmt DECIMAL(10, 2),
                    AD_UserId VARCHAR(25),
                    MD_UserId VARCHAR(25),
                    ZBP_UserId VARCHAR(25),
                    Param2 VARCHAR(25),
                    IfscCode VARCHAR(25),
                    ProfitAmount DECIMAL(10, 2),
                    GSTAllow DECIMAL(10, 2),
                    TDSAllow DECIMAL(10, 2),
                    BeneName VARCHAR(25),
                    BankName VARCHAR(25),
                    SenderName VARCHAR(25),
                    PayType VARCHAR(25),
                    SchemeName VARCHAR(25)
                )
                """)
                
                # Insert data into the table
                for i, row in self.data.iterrows():
                    cursor.execute("""
                    INSERT INTO financial_data (
                        Id, OrderID, Number, RPCode, Parent, Operator, Amount, API, UserId, APIName, Status, RechargeMode, 
                        RechargeDate, EditDate, Username, Balance, Cost, ChargePer, ServiceId, RevertTran, OperatorId, OptID, 
                        WhiteLabelID, AD_CommAmt, AD_ChargeAmt, MD_CommAmt, MD_ChargeAmt, ZBP_CommAmt, ZBP_ChargeAmt, CommAmt, 
                        AD_UserId, MD_UserId, ZBP_UserId, Param2, IfscCode, ProfitAmount, GSTAllow, TDSAllow, BeneName, BankName, 
                        SenderName, PayType, SchemeName
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, tuple(row))
                
                self.db_connection.commit()
                messagebox.showinfo("Info", "Data uploaded to MySQL database successfully!")
            except mysql.connector.Error as err:
                messagebox.showerror("Error", f"Failed to upload data to MySQL: {err}")
            finally:
                cursor.close()
        else:
            messagebox.showwarning("Warning", "No data to upload or not connected to MySQL. Please process data and connect to MySQL first.")

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
# class TransactionStatusApp:
#     def __init__(self, root):
#         self.root = root
#         self.root.title("Transaction Status Processor")

#         # Initialize file paths and data
#         self.file_paths = {"AEPS": "", "DMT": "", "BC": ""}
#         self.data = None
        
#         # Set the window icon (ensure the path to 'data-transformation.ico' is correct)
#         self.root.iconbitmap('data-transformation.ico')
        
#         # Create and place widgets
#         self.create_widgets()

#     def create_widgets(self):
#         # AEPS File Path Label and Entry
#         tk.Label(self.root, text="AEPS File Path:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
#         self.aeps_entry = tk.Entry(self.root, width=50)
#         self.aeps_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")
#         tk.Button(self.root, text="Import AEPS File", command=lambda: self.import_file("AEPS")).grid(row=0, column=2, padx=5, pady=5)

#         # DMT File Path Label and Entry
#         tk.Label(self.root, text="DMT File Path:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
#         self.dmt_entry = tk.Entry(self.root, width=50)
#         self.dmt_entry.grid(row=1, column=1, padx=5, pady=5, sticky="w")
#         tk.Button(self.root, text="Import DMT File", command=lambda: self.import_file("DMT")).grid(row=1, column=2, padx=5, pady=5)

#         # BC File Path Label and Entry
#         tk.Label(self.root, text="BC File Path:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
#         self.bc_entry = tk.Entry(self.root, width=50)
#         self.bc_entry.grid(row=2, column=1, padx=5, pady=5, sticky="w")
#         tk.Button(self.root, text="Import BC File", command=lambda: self.import_file("BC")).grid(row=2, column=2, padx=5, pady=5)

#         # Process Data Button
#         self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
#         self.process_button.grid(row=3, column=0, columnspan=3, pady=10)

#         # Export Data Button
#         self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
#         self.export_button.grid(row=4, column=0, columnspan=3, pady=10)

#         # Footer with copyright notice
#         self.footer_label = tk.Label(self.root, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
#                                     relief=tk.SUNKEN, anchor='w')
#         self.footer_label.grid(row=5, column=0, columnspan=3, sticky="w", pady=5)

#     def import_file(self, file_type):
#         """Prompt user to select a file and update the file paths dictionary and Entry widget."""
#         file_path = filedialog.askopenfilename(title=f"Select {file_type} File", filetypes=[("Excel Files", "*.xlsx")])
#         if file_path:
#             self.file_paths[file_type] = file_path
#             entry_widget = getattr(self, f"{file_type.lower()}_entry")
#             entry_widget.delete(0, tk.END)
#             entry_widget.insert(0, file_path)
            
#             # Enable process button if all files are set
#             if all(self.file_paths.values()):
#                 self.process_button.config(state=tk.NORMAL)

#     def process_data(self):
#         try:
#             AEPS = pd.read_excel(self.file_paths["AEPS"])
#             DMT = pd.read_excel(self.file_paths["DMT"])
#             BC = pd.read_excel(self.file_paths["BC"])

#             # Combine AEPS and DMT data
#             digi = pd.concat([AEPS, DMT], axis=0)
#             Digipay = digi[["Id", "OrderID", "Number", "RPCode", "Parent", "Operator", "Amount", "API", "UserId", "APIName",
#                              "Status", "RechargeMode", "RechargeDate", "EditDate", "Username", "OperatorId"]]
#             Digipay['Id'].fillna(0, inplace=True)

#             # Merge with BC data
#             Digipay = pd.merge(Digipay, BC, on='Operator', how='left')
#             Digipayfinal = Digipay[Digipay['Segments'] != "No Revenue"]
#             Digipayfinal["RechargeDate"] = pd.to_datetime(Digipayfinal["RechargeDate"])
#             Digipayfinal['Date'] = Digipayfinal["RechargeDate"].dt.date

#             self.data = Digipayfinal
#             self.export_button.config(state=tk.NORMAL)
#             messagebox.showinfo("Info", "Data processed successfully!")
#         except Exception as e:
#             messagebox.showerror("Error", f"An error occurred during processing: {e}")

#     def export_data(self):
#         if self.data is not None:
#             export_path = filedialog.asksaveasfilename(
#                 defaultextension=".xlsx",
#                 filetypes=[("Excel Files", "*.xlsx")],
#                 title="Save Processed Data"
#             )
#             if export_path:
#                 try:
#                     self.data.to_excel(export_path, index=False)
#                     messagebox.showinfo("Info", "Data exported successfully!")
#                 except Exception as e:
#                     messagebox.showerror("Error", f"Failed to export data: {e}")
#         else:
#             messagebox.showwarning("Warning", "No data to export. Please process data first.")
class TransactionStatusApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Transaction Status Processor")

        # Initialize file paths and data
        self.file_paths = {"AEPS": "", "DMT": "", "BC": ""}
        self.data = None
        
        
        # Set the window icon
        self.root.iconbitmap('data-transformation.ico')
        # Create and place widgets
        self.create_widgets()

    def create_widgets(self):
        # AEPS File Path Label and Entry
        tk.Label(self.root, text="AEPS File Path:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.aeps_entry = tk.Entry(self.root, width=50)
        self.aeps_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        tk.Button(self.root, text="Import AEPS File", command=lambda: self.import_file("AEPS")).grid(row=0, column=2, padx=5, pady=5)

        # DMT File Path Label and Entry
        tk.Label(self.root, text="DMT File Path:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        self.dmt_entry = tk.Entry(self.root, width=50)
        self.dmt_entry.grid(row=1, column=1, padx=5, pady=5, sticky="w")
        tk.Button(self.root, text="Import DMT File", command=lambda: self.import_file("DMT")).grid(row=1, column=2, padx=5, pady=5)

        # BC File Path Label and Entry
        tk.Label(self.root, text="BC File Path:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        self.bc_entry = tk.Entry(self.root, width=50)
        self.bc_entry.grid(row=2, column=1, padx=5, pady=5, sticky="w")
        tk.Button(self.root, text="Import BC File", command=lambda: self.import_file("BC")).grid(row=2, column=2, padx=5, pady=5)

        # Process Data Button
        self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
        self.process_button.grid(row=3, column=0, columnspan=3, pady=10)

        # Export Data Button
        self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
        self.export_button.grid(row=4, column=0, columnspan=3, pady=10)

        # Send to MySQL Button
        self.mysql_button = tk.Button(self.root, text="Send to MySQL", command=self.send_to_mysql, state=tk.DISABLED)
        self.mysql_button.grid(row=5, column=0, columnspan=3, pady=10)

        # Footer with copyright notice
        self.footer_label = tk.Label(self.root, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
                                    relief=tk.SUNKEN, anchor='w')
        self.footer_label.grid(row=6, column=0, columnspan=3, sticky="w", pady=5)

    def import_file(self, file_type):
        """Prompt user to select a file and update the file paths dictionary and Entry widget."""
        file_path = filedialog.askopenfilename(title=f"Select {file_type} File", filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            self.file_paths[file_type] = file_path
            entry_widget = getattr(self, f"{file_type.lower()}_entry")
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, file_path)
            
            # Enable process button if all files are set
            if all(self.file_paths.values()):
                self.process_button.config(state=tk.NORMAL)

    def process_data(self):
        try:
            AEPS = pd.read_excel(self.file_paths["AEPS"])
            DMT = pd.read_excel(self.file_paths["DMT"])
            BC = pd.read_excel(self.file_paths["BC"])

            # Combine AEPS and DMT data
            digi = pd.concat([AEPS, DMT], axis=0)
            Digipay = digi[["Id", "OrderID", "Number", "RPCode", "Parent", "Operator", "Amount", "API", "UserId", "APIName",
                             "Status", "RechargeMode", "RechargeDate", "EditDate", "Username", "OperatorId"]]
            Digipay['Id'].fillna(0, inplace=True)

            # Merge with BC data
            Digipay = pd.merge(Digipay, BC, on='Operator', how='left')
            Digipayfinal = Digipay[Digipay['Segments'] != "No Revenue"]
            Digipayfinal["RechargeDate"] = pd.to_datetime(Digipayfinal["RechargeDate"])
            Digipayfinal['Date'] = Digipayfinal["RechargeDate"].dt.date

            self.data = Digipayfinal
            self.export_button.config(state=tk.NORMAL)
            self.mysql_button.config(state=tk.NORMAL)  # Enable MySQL button
            messagebox.showinfo("Info", "Data processed successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during processing: {e}")

    def export_data(self):
        if self.data is not None:
            export_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")],
                title="Save Processed Data"
            )
            if export_path:
                try:
                    self.data.to_excel(export_path, index=False)
                    messagebox.showinfo("Info", "Data exported successfully!")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to export data: {e}")
        else:
            messagebox.showwarning("Warning", "No data to export. Please process data first.")

    def send_to_mysql(self):
        """Send processed data to MySQL database."""
        try:
            self.processed_data['Date'] = pd.to_datetime(self.processed_data['Date']).dt.date
            
            # Connect to MySQL
            connection = mysql.connector.connect(
                host='localhost',  # Your host, e.g. localhost
                database='naresh_db',  # Your database name
                user='root',  # Your MySQL username
                password='Naresh@123'  # Your MySQL password
            )

            if connection.is_connected():
                cursor = connection.cursor()
                # Assuming your MySQL table has the same column names as the DataFrame
                for i, row in self.data.iterrows():
                    sql = """INSERT INTO your_table_name (Id, OrderID, Number, RPCode, Parent, Operator, Amount, API, UserId,
                                                            APIName, Status, RechargeMode, RechargeDate, EditDate, Username, OperatorId)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    cursor.execute(sql, tuple(row))

                connection.commit()  # Commit the transaction
                messagebox.showinfo("Info", "Data sent to MySQL successfully!")
        except Error as e:
            messagebox.showerror("Error", f"Error connecting to MySQL: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

class WelcomeScreen(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Welcome")
        self.geometry("500x500")
        self.resizable(False, False)

        # Set the window icon
        try:
            self.iconbitmap('data-transformation.ico')
        except tk.TclError as e:
            print(f"Error setting icon: {e}")

        self.create_widgets()
    
    def create_widgets(self):
        # Center the content
        frame = tk.Frame(self)
        frame.pack(expand=True, padx=20, pady=20)
        
        tk.Label(frame, text="Welcome to the Data Transformation Software", font=("Arial", 14)).pack(pady=20)
        tk.Button(frame, text="Click Here to Start", command=self.start_application).pack(pady=10)
            
class DigipayAllTransactionsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Digipay All Transactions")

        # Initialize file paths and data
        self.file_paths = {
            "CommissionRates": "",
            "DMT": "",
            "AEPS": ""
        }
        self.processed_data = None

        # Set the window icon (ensure the path is correct)
        try:
            self.root.iconbitmap('data-transformation.ico')
        except tk.TclError as e:
            print(f"Error setting icon: {e}")

        # Create and place widgets
        self.create_widgets()

    def create_widgets(self):
        # Commission Rates
        tk.Label(self.root, text="Commission Rates File:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.commission_path_entry = tk.Entry(self.root, width=50, state='readonly')
        self.commission_path_entry.grid(row=0, column=1, padx=5, pady=5)
        tk.Button(self.root, text="Import Commission Rates", command=lambda: self.import_file("CommissionRates")).grid(row=0, column=2, padx=5, pady=5)

        # DMT File
        tk.Label(self.root, text="DMT File:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        self.dmt_path_entry = tk.Entry(self.root, width=50, state='readonly')
        self.dmt_path_entry.grid(row=1, column=1, padx=5, pady=5)
        tk.Button(self.root, text="Import DMT File", command=lambda: self.import_file("DMT")).grid(row=1, column=2, padx=5, pady=5)

        # AEPS File
        tk.Label(self.root, text="AEPS File:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        self.aeps_path_entry = tk.Entry(self.root, width=50, state='readonly')
        self.aeps_path_entry.grid(row=2, column=1, padx=5, pady=5)
        tk.Button(self.root, text="Import AEPS File", command=lambda: self.import_file("AEPS")).grid(row=2, column=2, padx=5, pady=5)

        # Process Data Button
        self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
        self.process_button.grid(row=3, column=0, columnspan=3, pady=10)

        # Export Data Button
        self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
        self.export_button.grid(row=4, column=0, columnspan=3, pady=10)

        # Send to MySQL Button
        self.mysql_button = tk.Button(self.root, text="Send to MySQL", command=self.send_to_mysql, state=tk.DISABLED)
        self.mysql_button.grid(row=5, column=0, columnspan=3, pady=10)

        # Footer with copyright notice
        self.footer_label = tk.Label(self.root, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
                                    relief=tk.SUNKEN, anchor='w')
        self.footer_label.grid(row=20, column=0, columnspan=3, sticky="w", pady=20)

    def import_file(self, file_type):
        """Prompt user to select a file and update the file paths dictionary."""
        file_path = filedialog.askopenfilename(title=f"Select {file_type} File", filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            self.file_paths[file_type] = file_path
            
            # Update the corresponding entry to show the imported file path
            if file_type == "CommissionRates":
                self.update_entry(self.commission_path_entry, file_path)
            elif file_type == "DMT":
                self.update_entry(self.dmt_path_entry, file_path)
            elif file_type == "AEPS":
                self.update_entry(self.aeps_path_entry, file_path)

            # Enable process button if all files are set
            if all(self.file_paths.values()):
                self.process_button.config(state=tk.NORMAL)
                messagebox.showinfo("Info", "All files imported successfully!")

    def update_entry(self, entry_widget, file_path):
        entry_widget.config(state='normal')
        entry_widget.delete(0, tk.END)
        entry_widget.insert(0, file_path)
        entry_widget.config(state='readonly')

    def process_data(self):
        try:
            # Load files
            commission_rates_dmt = pd.read_excel(self.file_paths["CommissionRates"])
            dmt = pd.read_excel(self.file_paths["DMT"])
            aeps = pd.read_excel(self.file_paths["AEPS"])

            # Process DMT
            dmt = pd.merge(dmt, commission_rates_dmt, on='Operator')
            dmt = dmt[["Operator", "Amount", "Status", "RechargeDate", "Username", "Cost", 
                        "AD_CommAmt", "AD_ChargeAmt", "MD_CommAmt", "MD_ChargeAmt", 
                        "ZBP_CommAmt", "ZBP_ChargeAmt", "CommAmt", "Revenue %", "Value", "Segments"]]
            DMT = dmt[dmt['Status'] == "Success"]
            DMT['Revenue'] = DMT.apply(
                lambda row: (row['Amount'] / 1.18) * (row['Revenue %']) / 100 if row['Segments'] == "Recharge" 
                else (row['Amount'] * (row['Revenue %']) / 100) + row['Value'], axis=1
            )
            DMT['Payout'] = DMT[['AD_CommAmt', 'MD_CommAmt', 'CommAmt']].sum(axis=1)

            # Process AEPS
            aeps = pd.merge(aeps, commission_rates_dmt, on='Operator')
            aeps['CommAmt'] = aeps['Amount'] - aeps['Cost']
            AEPS = aeps[aeps['Status'] == 'Success']
            AEPS['Revenue'] = AEPS['Value'] + (AEPS['Amount'] * (AEPS['Revenue %'] / 100))
            AEPS['Payout'] = AEPS[['AD_CommAmt', 'MD_CommAmt', 'CommAmt']].sum(axis=1)

            # Combine DMT and AEPS
            DIGIPAY = pd.concat([DMT, AEPS], axis=0)
            DIGIPAY = DIGIPAY[["Username", "RechargeDate", "Segments", "Revenue", "Payout"]]
            DIGIPAY = DIGIPAY[DIGIPAY['Segments'] != 'No Revenue']
            DIGIPAY['Trxcount'] = 1
            DIGIPAY['Month'] = DIGIPAY['RechargeDate'].dt.strftime('%b')
            DIGIPAY['Year'] = DIGIPAY['RechargeDate'].dt.strftime('%y')
            DIGIPAY['Trx_Month'] = DIGIPAY['Month'] + '-' + DIGIPAY['Year']
            DIGI_Pivot = pd.pivot_table(
                DIGIPAY, index=['Trx_Month', 'Username', 'Segments'],
                values=['Trxcount', 'Revenue', 'Payout'], aggfunc='sum'
            ).reset_index()
            DIGI_Pivot = DIGI_Pivot[["Trx_Month", "Username", "Segments", "Revenue", "Payout", "Trxcount"]]

            # Save processed data to an attribute for export
            self.processed_data = DIGI_Pivot
            self.export_button.config(state=tk.NORMAL)
            self.mysql_button.config(state=tk.NORMAL)
            messagebox.showinfo("Info", "Data processed successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during processing: {e}")

    def export_data(self):
        if self.processed_data is not None:
            try:
                export_path = filedialog.asksaveasfilename(
                    defaultextension=".xlsx",
                    filetypes=[("Excel Files", "*.xlsx")],
                    title="Save Processed Data"
                )
                if export_path:
                    self.processed_data.to_excel(export_path, index=False)
                    messagebox.showinfo("Info", "Data exported successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred during export: {e}")
        else:
            messagebox.showwarning("Warning", "No data to export. Please process data first.")

    def send_to_mysql(self):
        if self.processed_data is not None:
            try:
                connection = mysql.connector.connect(
                    host='localhost',
                    database='naresh_db',
                    user='root',
                    password='Naresh@123'  # Update this with secure practices
                )
                if connection.is_connected():
                    cursor = connection.cursor()
                    
                    # Create table if not exists
                    create_table_query = """
                    CREATE TABLE IF NOT EXISTS digipay_transactions (
                        Trx_Month VARCHAR(10),
                        Username VARCHAR(255),
                        Segments VARCHAR(50),
                        Revenue DOUBLE,
                        Payout DOUBLE,
                        Trxcount INT
                    );
                    """
                    cursor.execute(create_table_query)

                    # Insert data into table
                    insert_query = """
                    INSERT INTO digipay_transactions (Trx_Month, Username, Segments, Revenue, Payout, Trxcount)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """
                    
                    for index, row in self.processed_data.iterrows():
                        cursor.execute(insert_query, tuple(row))
                    
                    connection.commit()
                    cursor.close()
                    connection.close()

                    messagebox.showinfo("Info", "Data sent to MySQL successfully!")
                else:
                    messagebox.showerror("Error", "Failed to connect to MySQL.")
            except Error as e:
                messagebox.showerror("Error", f"An error occurred while sending data to MySQL: {e}")
            except Exception as e:
                messagebox.showerror("Error", f"An unexpected error occurred: {e}")
        else:
            messagebox.showwarning("Warning", "No data to send. Please process data first.")





class DSCDataTransformationApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DSC Data Transformation")

        # Initialize file path and processed data
        self.file_path = ""
        self.processed_data = None
        
        # Set the window icon (ensure the path to 'data-transformation.ico' is correct)
        self.root.iconbitmap('data-transformation.ico')
        
        # Create and place widgets
        self.create_widgets()

    def create_widgets(self):
        # File Path Label and Entry
        tk.Label(self.root, text="Data File Path:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        self.file_entry = tk.Entry(self.root, width=50)
        self.file_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        tk.Button(self.root, text="Import File", command=self.import_file).grid(row=0, column=2, padx=5, pady=5)

        # Process Data Button
        self.process_button = tk.Button(self.root, text="Process Data", command=self.process_data, state=tk.DISABLED)
        self.process_button.grid(row=1, column=0, columnspan=3, pady=10)

        # Export Data Button
        self.export_button = tk.Button(self.root, text="Export Data", command=self.export_data, state=tk.DISABLED)
        self.export_button.grid(row=2, column=0, columnspan=3, pady=10)

        # Send to MySQL Button
        self.mysql_button = tk.Button(self.root, text="Send to MySQL", command=self.send_to_mysql, state=tk.DISABLED)
        self.mysql_button.grid(row=3, column=0, columnspan=3, pady=10)

        # Footer with copyright notice
        self.footer_label = tk.Label(self.root, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
                                    relief=tk.SUNKEN, anchor='w')
        self.footer_label.grid(row=4, column=0, columnspan=3, sticky="w", pady=5)

    def import_file(self):
        try:
            self.file_path = filedialog.askopenfilename(title="Select Data File", filetypes=[("Excel Files", "*.xlsx")])
            if self.file_path:
                self.file_entry.delete(0, tk.END)
                self.file_entry.insert(0, self.file_path)
                messagebox.showinfo("Info", "File imported successfully!")
                self.process_button.config(state=tk.NORMAL)
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during file import: {e}")

    def process_data(self):
        try:
            if not self.file_path:
                raise ValueError("No file selected for processing.")

            # Load data
            df = pd.read_excel(self.file_path)
            df['Token Category'] = df['Token Category'].str.upper()

            # Process DSC
            DSC = df.dropna(subset=['Target of DSC'])
            DSC = DSC[["Date", "Employee Name", "Employee Code", "Target of DSC", "DSC Quantity", "DSC Price", "DSC without GST", "DSC per unit cost", "DSC total cost", "BP Code"]]
            DSC['Revenue'] = DSC['DSC without GST']
            DSC['Payout'] = DSC['DSC per unit cost'] * DSC['DSC Quantity']
            DSC['Segments'] = "DSC"

            # Process USB
            USB = df.dropna(subset=['Token Category'])
            USB1 = USB[USB['Token Category'] == 'EPASS']
            USB2 = USB[USB['Token Category'] == 'PROXKEY']
            USB = pd.concat([USB1, USB2], ignore_index=True)
            USB = USB[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            USB['Revenue'] = USB['Token without GST']
            USB['Payout'] = USB['Token per unit cost'] * USB['Token Quantity']
            USB['Segments'] = "USB"

            # Process Thermal Printer
            Thermal_Printer = df.dropna(subset=['Token Category'])
            Thermal_Printer1 = Thermal_Printer[Thermal_Printer['Token Category'] == 'MENTATION PRINTER']
            Thermal_Printer2 = Thermal_Printer[Thermal_Printer['Token Category'] == 'BLUEPRINT']
            Thermal_Printer3 = Thermal_Printer[Thermal_Printer['Token Category'] == 'THERMAL PRINTER']
            Thermal_Printer = pd.concat([Thermal_Printer1, Thermal_Printer2, Thermal_Printer3], ignore_index=True)
            Thermal_Printer = Thermal_Printer[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            Thermal_Printer['Revenue'] = Thermal_Printer['Token without GST']
            Thermal_Printer['Payout'] = Thermal_Printer['Token per unit cost'] * Thermal_Printer['Token Quantity']
            Thermal_Printer['Segments'] = "Thermal Printer"

            # Process MANTRA
            MANTRA = df.dropna(subset=['Token Category'])
            MANTRA = MANTRA[MANTRA['Token Category'] == 'MFS110']
            MANTRA = MANTRA[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            MANTRA['Revenue'] = MANTRA['Token without GST']
            MANTRA['Payout'] = MANTRA['Token per unit cost'] * MANTRA['Token Quantity']
            MANTRA['Segments'] = "MANTRA"

            # Process MATM
            MATM = df.dropna(subset=['Token Category'])
            MATM = MATM[MATM['Token Category'] == 'MATM']
            MATM = MATM[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            MATM['Revenue'] = MATM['Token without GST']
            MATM['Payout'] = MATM['Token per unit cost'] * MATM['Token Quantity']
            MATM['Segments'] = "MATM"

            # Process IRIS SCANNER
            IRIS_SCANNER = df.dropna(subset=['Token Category'])
            IRIS_SCANNER = IRIS_SCANNER[IRIS_SCANNER['Token Category'] == 'IRIS SCANNER']
            IRIS_SCANNER = IRIS_SCANNER[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            IRIS_SCANNER['Revenue'] = IRIS_SCANNER['Token without GST']
            IRIS_SCANNER['Payout'] = IRIS_SCANNER['Token per unit cost'] * IRIS_SCANNER['Token Quantity']
            IRIS_SCANNER['Segments'] = "Iris Scanner"

            # Process Startek
            Startek = df.dropna(subset=['Token Category'])
            Startek = Startek[Startek['Token Category'] == 'STARTEK']
            Startek = Startek[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            Startek['Revenue'] = Startek['Token without GST']
            Startek['Payout'] = Startek['Token per unit cost'] * Startek['Token Quantity']
            Startek['Segments'] = "Startek"

            # Process Morpho
            Morpho = df.dropna(subset=['Token Category'])
            Morpho = Morpho[Morpho['Token Category'] == 'MORPHO']
            Morpho = Morpho[["Date", "Employee Name", "Employee Code", "Token Category", "Token Quantity", "Token Price", "Token without GST", "Token per unit cost", "Token Total cost", "BP Code"]]
            Morpho['Revenue'] = Morpho['Token without GST']
            Morpho['Payout'] = Morpho['Token per unit cost'] * Morpho['Token Quantity']
            Morpho['Segments'] = "Morpho"

            # Combine all data
            DSC_USB_DEVICES = pd.concat([DSC, USB, Thermal_Printer, MANTRA, MATM, IRIS_SCANNER, Startek, Morpho], ignore_index=True)
            DSC_USB_DEVICES = DSC_USB_DEVICES[["Date", "Employee Name", "Employee Code", "BP Code", "Revenue", "Payout", "Segments"]]

            # Add additional columns
            DSC_USB_DEVICES['SM NAME'] = DSC_USB_DEVICES.apply(lambda row: row['Employee Name'] if row['BP Code'] == 2034 else '-', axis=1)
            DSC_USB_DEVICES['SM ID'] = DSC_USB_DEVICES.apply(lambda row: row['Employee Code'] if row['BP Code'] == 2034 else '-', axis=1)

            # Save processed data to an attribute for export and enable buttons
            self.processed_data = DSC_USB_DEVICES
            self.export_button.config(state=tk.NORMAL)
            self.mysql_button.config(state=tk.NORMAL)
            messagebox.showinfo("Info", "Data processed successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during processing: {e}")

    def export_data(self):
        if self.processed_data is not None:
            try:
                export_path = filedialog.asksaveasfilename(
                    defaultextension=".xlsx",
                    filetypes=[("Excel Files", "*.xlsx")],
                    title="Save Processed Data"
                )
                if export_path:
                    self.processed_data.to_excel(export_path, index=False)
                    messagebox.showinfo("Info", "Data exported successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred during export: {e}")
        else:
            messagebox.showwarning("Warning", "No data to export. Please process data first.")

    def send_to_mysql(self):
        if self.processed_data is not None:
            try:
                self.processed_data['Date'] = pd.to_datetime(self.processed_data['Date']).dt.date
            
                connection = mysql.connector.connect(
                    host='localhost',
                    database='naresh_db',
                    user='root',
                    password='Naresh@123'
                )
                if connection.is_connected():
                    cursor = connection.cursor()
                    
                    # Create table if not exists
                    create_table_query = """
                    CREATE TABLE IF NOT EXISTS dsc_data (
                        Date DATE,
                        Employee_Name VARCHAR(255),
                        Employee_Code VARCHAR(50),
                        BP_Code INT,
                        Revenue DOUBLE,
                        Payout DOUBLE,
                        Segments VARCHAR(50),
                        SM_Name VARCHAR(255),
                        SM_ID VARCHAR(50)
                    );
                    """
                    cursor.execute(create_table_query)

                    # Insert data into table
                    insert_query = """
                    INSERT INTO dsc_data (Date, Employee_Name, Employee_Code, BP_Code, Revenue, Payout, Segments, SM_Name, SM_ID)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    
                    for index, row in self.processed_data.iterrows():
                        cursor.execute(insert_query, tuple(row))
                    
                    connection.commit()
                    cursor.close()
                    connection.close()

                    messagebox.showinfo("Info", "Data sent to MySQL successfully!")
                else:
                    messagebox.showerror("Error", "Failed to connect to MySQL.")
            except Error as e:
                messagebox.showerror("Error", f"An error occurred while sending data to MySQL: {e}")
            except Exception as e:
                messagebox.showerror("Error", f"An unexpected error occurred: {e}")
        else:
            messagebox.showwarning("Warning", "No data to send. Please process data first.")
                    
class WelcomeScreen(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Welcome")
        self.geometry("500x500")
        self.resizable(False, False)

        # Set the window icon (ensure the path to 'data-transformation.ico' is correct)
        try:
            self.iconbitmap('data-transformation.ico')
        except tk.TclError as e:
            print(f"Error setting icon: {e}")

        self.create_widgets()
    
    def create_widgets(self):
        # Center the content
        frame = tk.Frame(self)
        frame.pack(expand=True, padx=20, pady=20)
        
        tk.Label(frame, text="Welcome to the Data Transformation Software", font=("Arial", 14)).pack(pady=20)
        tk.Button(frame, text="Click Here to Start", command=self.start_application).pack(pady=10)

    def start_application(self):
        self.destroy()  # Close the welcome screen
        app = Application()  # Open the main application window
        app.mainloop()
        
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Main Menu")
        self.geometry("500x500")
        
        
        # Set the window icon (ensure the path to 'data-transformation.ico' is correct)
        try:
            self.iconbitmap('data-transformation.ico')
        except tk.TclError as e:
            print(f"Error setting icon: {e}")

        # Create a frame to hold the notification and button
        frame = tk.Frame(self)
        frame.pack(pady=20)
        
        
        # Notification Label
        tk.Label(frame, text="Note: This is Only for Data Transformation use", font=("Arial", 12), fg="blue").pack(pady=5)
        
        # Define buttons and their commands
        buttons = [
            
            ("Financial Inclusion", self.open_financial_inclusion),
            ("Transaction Status", self.open_transaction_status),
            ("Agent Activation", self.open_agent_activation),
            ("Digipay All Transactions", self.open_digipay_all_transactions),
            ("DSC Data Transformation", self.open_dsc_data_transformation),
            ("PAAM All Transactions", self.open_paam_all_transactions),
            
        ]
        # Create the red logout button
        

        
         # Footer with copyright notice
        self.footer_label = tk.Label(self, text="Copyright (c) 2024 Abhishek and Naresh - All Rights Reserved.", 
                                    relief=tk.SUNKEN, anchor='w')
        self.footer_label.pack(side=tk.BOTTOM, fill=tk.X, pady=5)
       
        # Create and pack buttons
        for text, command in buttons:
            button = ttk.Button(self, text=text, command=command)
            button.pack(pady=10)
        
        # Create the red logout button
        self.logout_button = tk.Button(self, text="Logout", bg="red", fg="white", command=self.logout)
        self.logout_button.pack(pady=10)   
            
    def open_financial_inclusion(self):
        financial_inclusion_window = tk.Toplevel(self)
        FinancialInclusionApp(financial_inclusion_window)

    def open_transaction_status(self):
        transaction_status_window = tk.Toplevel(self)
        TransactionStatusApp(transaction_status_window)

    def open_agent_activation(self):
        agent_activation_window = tk.Toplevel(self)
        AgentActivationApp(agent_activation_window)
        
    def open_digipay_all_transactions(self):
        digipay_all_transactions = tk.Toplevel(self)
        DigipayAllTransactionsApp(digipay_all_transactions)
    
  
        
    def open_dsc_data_transformation(self):
        dsc_data_transformation = tk.Toplevel(self)
        DSCDataTransformationApp(dsc_data_transformation)

        
    def open_paam_all_transactions(self):
        paam_window = tk.Toplevel(self)
        PAAMDataProcessorApp(paam_window)

    def logout(self):
        self.destroy()  # Close the main application window
        os.system("python mainlogin.py")  # Adjust to your actual login script
        
if __name__ == "__main__":
   
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    WelcomeScreen(root)
    root.mainloop()
    app = Application()
    app.mainloop()
   