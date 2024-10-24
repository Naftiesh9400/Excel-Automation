import tkinter as tk
from tkinter import messagebox
import mysql.connector
import hashlib
import os

# Function to connect to MySQL
def connect_db():
    try:
        return mysql.connector.connect(
            host='localhost',
            database='naresh_db',
            user='root',
            password='Naresh@123'
        )
    except mysql.connector.Error as e:
        messagebox.showerror("Database Error", f"Error: {e}")
        return None

# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Login / Signup")
        self.root.geometry("1920x1080")

        # Create a frame for the white box
        self.white_box = tk.Frame(self.root, bg='white', padx=20, pady=20)
        self.white_box.pack(expand=True, fill='both', padx=100, pady=80)

        # Create a header frame
        self.header_frame = tk.Frame(self.white_box, bg='white')
        self.header_frame.pack(pady=10)

        tk.Label(self.header_frame, text="Login / Signup", font=("Arial", 24, "bold")).pack()

        # Create a form frame
        self.form_frame = tk.Frame(self.white_box, bg='white')
        self.form_frame.pack(pady=10)

        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.first_name_var = tk.StringVar()
        self.last_name_var = tk.StringVar()
        self.new_password_var = tk.StringVar()

        self.show_login()

    def show_login(self):
        for widget in self.form_frame.winfo_children():
            widget.destroy()

        tk.Label(self.form_frame, text="Login", font=("Arial", 16)).pack(pady=10)

        login_frame = tk.Frame(self.form_frame, bg='white')
        login_frame.pack(pady=10)

        tk.Label(login_frame, text="Username:", font=("Arial", 14)).grid(row=0, column=0, padx=5, pady=5)
        tk.Entry(login_frame, textvariable=self.username_var, font=("Arial", 14)).grid(row=0, column=1, padx=5, pady=5)

        tk.Label(login_frame, text="Password:", font=("Arial", 14)).grid(row=1, column=0, padx=5, pady=5)
        tk.Entry(login_frame, textvariable=self.password_var, show="*", font=("Arial", 14)).grid(row=1, column=1, padx=5, pady=5)

        tk.Button(self.form_frame, text="Login", command=self.login, font=("Arial", 14)).pack(pady=10)
        tk.Button(self.form_frame, text="Signup", command=self.show_signup, font=("Arial", 14)).pack(pady=5)
        tk.Button(self.form_frame, text="Forgot Password?", command=self.show_forgot_password, font=("Arial", 14)).pack(pady=5)

    def show_signup(self):
        for widget in self.form_frame.winfo_children():
            widget.destroy()

        tk.Label(self.form_frame, text="Signup", font=("Arial", 16)).pack(pady=10)

        signup_frame = tk.Frame(self.form_frame, bg='white')
        signup_frame.pack(pady=10)

        tk.Label(signup_frame, text="First Name:", font=("Arial", 14)).grid(row=0, column=0, padx=5, pady=5)
        tk.Entry(signup_frame, textvariable=self.first_name_var, font=("Arial", 14)).grid(row=0, column=1, padx=5, pady=5)

        tk.Label(signup_frame, text="Last Name:", font=("Arial", 14)).grid(row=1, column=0, padx=5, pady=5)
        tk.Entry(signup_frame, textvariable=self.last_name_var, font=("Arial", 14)).grid(row=1, column=1, padx=5, pady=5)

        tk.Label(signup_frame, text="Username:", font=("Arial", 14)).grid(row=2, column=0, padx=5, pady=5)
        tk.Entry(signup_frame, textvariable=self.username_var, font=("Arial", 14)).grid(row=2, column=1, padx=5, pady=5)

        tk.Label(signup_frame, text="Password:", font=("Arial", 14)).grid(row=3, column=0, padx=5, pady=5)
        tk.Entry(signup_frame, textvariable=self.password_var, show="*", font=("Arial", 14)).grid(row=3, column=1, padx=5, pady=5)

        tk.Button(self.form_frame, text="Create Account", command=self.signup, font=("Arial", 14)).pack(pady=10)
        tk.Button(self.form_frame, text="Back to Login", command=self.show_login, font=("Arial", 14)).pack(pady=5)

    def show_forgot_password(self):
        for widget in self.form_frame.winfo_children():
            widget.destroy()

        tk.Label(self.form_frame, text="Reset Password", font=("Arial", 16)).pack(pady=10)

        tk.Label(self.form_frame, text="Username:", font=("Arial", 14)).pack(pady=5)
        tk.Entry(self.form_frame, textvariable=self.username_var, font=("Arial", 14)).pack(pady=5)

        tk.Label(self.form_frame, text="New Password:", font=("Arial", 14)).pack(pady=5)
        tk.Entry(self.form_frame, textvariable=self.new_password_var, show="*", font=("Arial", 14)).pack(pady=5)

        tk.Button(self.form_frame, text="Change Password", command=self.change_password, font=("Arial", 14)).pack(pady=10)
        tk.Button(self.form_frame, text="Back to Login", command=self.show_login, font=("Arial", 14)).pack(pady=5)

    def change_password(self):
        username = self.username_var.get()
        new_password = self.new_password_var.get()
        hashed_password = hash_password(new_password)

        db = connect_db()
        if db:
            cursor = db.cursor()
            try:
                cursor.execute("UPDATE users SET password=%s WHERE username=%s", (hashed_password, username))
                if cursor.rowcount > 0:
                    db.commit()
                    messagebox.showinfo("Success", "Password changed successfully!")
                    self.show_login()
                else:
                    messagebox.showerror("Error", "Username not found.")
            except Exception as e:
                messagebox.showerror("Error", str(e))
            finally:
                db.close()

    def emp(self, first_name, last_name):
        self.main()
        os.system(f"python abhiresh.py {first_name} {last_name}")
        self.root.deiconify()

    def main(self):
        self.root.withdraw()

    def login(self):
        username = self.username_var.get()
        password = self.password_var.get()
        hashed_password = hash_password(password)

        db = connect_db()
        if db:
            cursor = db.cursor()
            cursor.execute("SELECT first_name, last_name FROM users WHERE username=%s AND password=%s", (username, hashed_password))
            user = cursor.fetchone()
            db.close()

            if user:
                first_name, last_name = user
                messagebox.showinfo("Login", "Login Successful!")
                self.emp(first_name, last_name)
            else:
                messagebox.showerror("Login", "Invalid username or password.")

    def signup(self):
        first_name = self.first_name_var.get()
        last_name = self.last_name_var.get()
        username = self.username_var.get()
        password = self.password_var.get()
        hashed_password = hash_password(password)

        # Check if the username starts with "RBL0000"
        if not username.startswith("RBL0000"):
            messagebox.showerror("Signup", "Username must start with 'RBL0000'.")
            return

        db = connect_db()
        if db:
            cursor = db.cursor()
            try:
                cursor.execute("INSERT INTO users (first_name, last_name, username, password) VALUES (%s , %s, %s, %s)", 
                               (first_name, last_name, username, hashed_password))
                db.commit()
                messagebox.showinfo("Signup", "Account created successfully!")
                self.show_login()
            except mysql.connector.IntegrityError:
                messagebox.showerror("Signup", "Username already exists.")
            except Exception as e:
                messagebox.showerror("Error", str(e))
            finally:
                db.close()

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()