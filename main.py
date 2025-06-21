import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import shutil
import datetime
import json
import threading
import sys
from dateutil.relativedelta import relativedelta
import time
# Removed PIL imports as logo image is no longer used
# Removed subprocess import as Robocopy is no longer used

# --- Constants ---
CONFIG_FILE = "move_config.json"
LOG_FILE = "app_log.txt"          # ไฟล์ Log หลักของแอปพลิเคชัน (ครอบคลุมเฉพาะ Log หลักของแอปพลิเคชัน)
ACTION_LOG_FILE = "action_log.txt" # ไฟล์ Log สำหรับบันทึกการกระทำกับไฟล์ (ย้าย, คัดลอก, ลบ) และขั้นตอนย่อย
LAST_RUN_FILE = "last_run.json"   # ไฟล์สำหรับบันทึกวันที่รัน Task ล่าสุด

class FileManagerApp:
    def __init__(self, master):
        self.master = master
        master.title("Auto Data Transfer IT") # Simplified title for the window bar
        master.resizable(True, True) # อนุญาตให้ปรับขนาดหน้าต่างได้
        # Set a minimum size for the window to maintain layout aesthetics
        # Adjusted minsize to accommodate side-by-side sections
        self.master.minsize(700, 550) # Increased width to fit two frames side-by-side

        # --- Variables for status and GUI (ตัวแปรสำหรับสถานะและ GUI) ---
        self.operation_cancelled = False # สถานะการยกเลิกการทำงานของไฟล์
        self.start_time = time.time()  # เวลาเริ่มต้นของการทำงาน
        self.total_bytes_processed = 0 # จำนวนไบต์ที่ถูกประมวลผล (สำหรับคำนวณความเร็ว)
        self.is_task_running = False # เพิ่มแฟล็กเพื่อควบคุมการทำงานซ้อนกัน

        # Initialize ttk.Style for GUI customization (กำหนดสไตล์สำหรับ GUI)
        self.style = ttk.Style()
        self._configure_styles() # Call method to set up custom styles

        # Removed self.logo_photo as image is no longer used

        # StringVar/BooleanVar variables for binding with GUI Widgets (ตัวแปรสำหรับผูกกับ Widgets ใน GUI)
        self.filter_old_files_var = tk.BooleanVar(value=False)
        self.months_old_var = tk.StringVar(value="3")
        self.source_var = tk.StringVar()
        self.dest_var = tk.StringVar()
        self.file_type_var = tk.StringVar(value="All")
        self.auto_day_var = tk.StringVar(value="1")
        self.auto_interval_var = tk.StringVar(value="1")
        self.auto_operation_var = tk.StringVar(value="move")
        self.auto_time_var = tk.StringVar(value="00:01")
        self.min_free_space_var = tk.StringVar(value="5.0")
        # Removed self.use_robocopy_var

        # --- GUI Setup (การตั้งค่า GUI) ---
        try:
            self._create_widgets()         # สร้างส่วนประกอบของ GUI
        except Exception as e:
            print(f"ERROR: Exception during _create_widgets: {e}")
            raise # Re-raise to see full traceback in terminal

        # Check and create last_run.json immediately if it doesn't exist
        # ตรวจสอบและสร้าง last_run.json ทันทีหากยังไม่มี
        if not os.path.exists(LAST_RUN_FILE):
            self._log(f"ℹ️ {LAST_RUN_FILE} not found. Initializing with current date.", to_app_log=True, to_gui_log=True)
            self._set_last_run_date(datetime.datetime.now().strftime("%Y-%m-%d"))

        try:
            self._load_settings_gui()      # โหลดการตั้งค่าที่บันทึกไว้มาแสดงใน GUI
        except Exception as e:
            print(f"ERROR: Exception during _load_settings_gui: {e}")
            raise

        try:
            self._update_next_run_label()  # อัปเดตข้อความแสดงเวลาทำงานรอบถัดไป
        except Exception as e:
            print(f"ERROR: Exception during _update_next_run_label: {e}")
            raise

        self._start_scheduler_thread() # เริ่มต้น Thread สำหรับการตรวจสอบ Task อัตโนมัติ

    def _configure_styles(self):
        """Configure and apply custom ttk styles for GUI elements for a modern, comfortable light mode."""
        # Set a modern theme as a base
        self.style.theme_use('clam') 

        # Define a consistent font for the application for readability
        app_font = ('Segoe UI', 9) 
        bold_app_font = ('Segoe UI', 9, 'bold')

        # General background color for the main window and frames (light grey, almost white)
        self.master.configure(bg='#F5F5F5') 
        self.style.configure('TFrame', background='#F5F5F5')
        
        # LabelFrame styling for clear section separation with a subtle border
        self.style.configure('TLabelframe', 
                             background='#F5F5F5', 
                             bordercolor='#D3D3D3', # Subtle grey border
                             relief='groove', # Gives a slight indented effect
                             borderwidth=1)
        self.style.configure('TLabelframe.Label', 
                             background='#F5F5F5', 
                             foreground='#4A4A4A', # Darker text for titles for good contrast
                             font=bold_app_font) 

        # Style for standard Labels
        self.style.configure('TLabel', 
                             background='#F5F5F5', 
                             foreground='#4A4A4A', # Dark text for readability
                             font=app_font) 
        
        # Style for Entry and Combobox fields for a clean, professional look
        self.style.configure('TEntry', 
                             fieldbackground='#FFFFFF', # White input field
                             foreground='#4A4A4A', 
                             borderwidth=1, 
                             relief="solid", # Solid border
                             font=app_font, 
                             padding=3) # Reduced internal padding for compactness
        self.style.configure('TCombobox', 
                             fieldbackground='#FFFFFF', 
                             foreground='#4A4A4A', 
                             borderwidth=1, 
                             relief="solid", 
                             font=app_font, 
                             padding=3) # Reduced internal padding for compactness
        
        # Style for Checkbuttons
        self.style.configure('TCheckbutton', 
                             background='#F5F5F5', 
                             foreground='#4A4A4A', 
                             font=app_font)

        # Custom button styles for visual distinction and responsiveness (maintaining existing colors)
        # Save Button (Vibrant Green for positive action)
        self.style.configure('Green.TButton',
                             background='#4CAF50', # Material Design Green
                             foreground='white',
                             font=bold_app_font,
                             padding=[10, 5], # Reduced padding for compactness
                             relief='flat',
                             focuscolor='#4CAF50', 
                             borderwidth=0)
        self.style.map('Green.TButton',
                       background=[('active', '#43A047'), ('pressed', '#388E3C'), ('disabled', '#6A8B6A')], # Added disabled state
                       foreground=[('active', 'white'), ('disabled', '#cccccc')]) # Dimmer text for disabled
        
        # Move/Copy Buttons (Standard Blue for primary actions)
        self.style.configure('Blue.TButton',
                             background='#2196F3', # Material Design Blue
                             foreground='white',
                             font=bold_app_font,
                             padding=[10, 5], # Reduced padding for compactness
                             relief='flat',
                             focuscolor='#2196F3',
                             borderwidth=0)
        self.style.map('Blue.TButton',
                       background=[('active', '#1976D2'), ('pressed', '#1565C0'), ('disabled', '#5B8FB3')], # Added disabled state
                       foreground=[('active', 'white'), ('disabled', '#cccccc')]) # Dimmer text for disabled

        # Delete/Cancel Buttons (Standard Red for destructive/stop actions)
        self.style.configure('Red.TButton',
                             background='#F44336', # Material Design Red
                             foreground='white',
                             font=bold_app_font,
                             padding=[10, 5], # Reduced padding for compactness
                             relief='flat',
                             focuscolor='#F44336',
                             borderwidth=0)
        self.style.map('Red.TButton',
                       background=[('active', '#D32F2F'), ('pressed', '#C62828'), ('disabled', '#8A6D6B')], # Added disabled state
                       foreground=[('active', 'white'), ('disabled', '#cccccc')]) # Dimmer text for disabled

        # Progress Bar Style for clear progress indication
        self.style.configure('Custom.Horizontal.TProgressbar',
                             background='#4CAF50', # Green fill
                             troughcolor='#E0E0E0', # Light grey trough
                             bordercolor='#E0E0E0', # Border matches trough
                             lightcolor='#4CAF50', 
                             darkcolor='#4CAF50')

    def _create_widgets(self):
        """สร้างและจัดวางส่วนประกอบทั้งหมดของ GUI รวมถึงส่วนหัวพร้อมโลโก้"""
        # เฟรมหลักสำหรับจัดวาง Widgets
        frame = ttk.Frame(self.master, padding=(15, 15)) # Increased overall padding
        frame.grid(row=0, column=0, sticky="nsew") 
        self.master.grid_rowconfigure(0, weight=1)    
        self.master.grid_columnconfigure(0, weight=1) 
        
        # Configure the row where the log box resides to expand vertically
        frame.grid_rowconfigure(4, weight=1) # Log box is now at row 4
        frame.grid_columnconfigure(0, weight=1) # Allow column 0 to expand (for left side content)
        frame.grid_columnconfigure(1, weight=1) # Allow column 1 to expand (for right side content)
        
        row_idx = 0 # Starting row index for placing widgets

        # --- Header Frame for Title and Logo (ส่วนหัวสำหรับชื่อแอปและโลโก้) ---
        header_frame = ttk.Frame(frame, padding=(0, 5)) # Added padding to separate from top/bottom
        # Spanning 2 columns (0 and 1) as there is no separate logo column anymore
        header_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=(0, 10)) 
        header_frame.grid_columnconfigure(0, weight=1) # Make single column expandable

        # App Title Label with larger emoji
        # Increased font size from 10 to 20 to make the emoji and text larger
        ttk.Label(header_frame, text="📦 Auto Data Transfer IT", font=('Segoe UI', 20, 'bold'), 
                  foreground='#333333').grid(row=0, column=0, sticky='ew', padx=10)
        
        row_idx += 1 # Increment row index after header frame

        # --- Folder Paths Section (การตั้งค่าเส้นทาง) ---
        path_frame = ttk.LabelFrame(frame, text="Folder Paths", padding=(10, 10)) # Increased padding
        path_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=10, padx=10) # Span 2 columns to take full width
        path_frame.grid_columnconfigure(1, weight=1) # Make Entry fields expandable within this frame

        ttk.Label(path_frame, text="📂 Source Folder:").grid(row=0, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji, Increased pady, padx
        ttk.Entry(path_frame, textvariable=self.source_var, width=50).grid(row=0, column=1, sticky="ew", pady=5) # Increased pady
        ttk.Button(path_frame, text="Browse", command=lambda: self._browse_folder(self.source_var)).grid(row=0, column=2, sticky="w", padx=(8,0), pady=5) # Increased padx, pady

        ttk.Label(path_frame, text="📁 Destination Folder:").grid(row=1, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji, Increased pady, padx
        ttk.Entry(path_frame, textvariable=self.dest_var, width=50).grid(row=1, column=1, sticky="ew", pady=5) # Increased pady
        ttk.Button(path_frame, text="Browse", command=lambda: self._browse_folder(self.dest_var)).grid(row=1, column=2, sticky="w", padx=(8,0), pady=5) # Increased padx, pady
        row_idx += 1 

        # --- File Filtering & Conditions Section (ประเภทไฟล์และเงื่อนไข) ---
        filter_group_frame = ttk.LabelFrame(frame, text="File Filtering & Conditions", padding=(10, 10)) # Increased padding
        # Placed in column 0
        filter_group_frame.grid(row=row_idx, column=0, sticky="nsew", pady=10, padx=(10, 5)) # Adjusted padx for side-by-side
        filter_group_frame.grid_columnconfigure(1, weight=1) # Allow entry fields in this frame to expand

        ttk.Label(filter_group_frame, text="🗃️ File Type:").grid(row=0, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji, Increased pady, padx
        ttk.Combobox(filter_group_frame, textvariable=self.file_type_var, values=["All", "Excel"], width=10, state="readonly").grid(row=0, column=1, sticky="ew", pady=5) # Increased pady

        # Frame for "Only files older than" options for tidy inline arrangement
        filter_age_frame = ttk.Frame(filter_group_frame) 
        filter_age_frame.grid(row=1, column=0, columnspan=2, sticky="w", pady=5, padx=(0, 8)) # Increased pady, padx
        ttk.Checkbutton(filter_age_frame, text="✅ Only files older than (months):", variable=self.filter_old_files_var).pack(side="left")
        ttk.Entry(filter_age_frame, textvariable=self.months_old_var, width=5).pack(side="left", padx=(5,0)) # Increased padx

        # --- Automated Task Settings Section (การตั้งค่าการทำงานอัตโนมัติ) ---
        auto_settings_frame = ttk.LabelFrame(frame, text="Automated Task Settings", padding=(10, 10)) # Increased padding
        # Placed in column 1, next to filter_group_frame
        auto_settings_frame.grid(row=row_idx, column=1, sticky="nsew", pady=10, padx=(5, 10)) # Adjusted padx for side-by-side
        auto_settings_frame.grid_columnconfigure(1, weight=1) # Allow entry fields in this frame to expand

        ttk.Label(auto_settings_frame, text="🗓️ Auto Day of Month:").grid(row=0, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji
        ttk.Entry(auto_settings_frame, textvariable=self.auto_day_var, width=10).grid(row=0, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="🔄 Repeat Every N Months:").grid(row=1, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji
        ttk.Entry(auto_settings_frame, textvariable=self.auto_interval_var, width=10).grid(row=1, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="Auto Operation:").grid(row=2, column=0, sticky="w", pady=5, padx=(0, 8))
        ttk.Combobox(auto_settings_frame, textvariable=self.auto_operation_var, values=["move", "copy", "delete"], width=10, state="readonly").grid(row=2, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="⏰ Auto Run Time (HH:MM):").grid(row=3, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji
        ttk.Entry(auto_settings_frame, textvariable=self.auto_time_var, width=10).grid(row=3, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="💾 Min Free Space (GB):").grid(row=4, column=0, sticky="w", pady=5, padx=(0, 8)) # Added emoji
        ttk.Entry(auto_settings_frame, textvariable=self.min_free_space_var, width=10).grid(row=4, column=1, sticky="ew", pady=5)

        # Removed the Robocopy Checkbutton section
            
        row_idx += 1 # This row_idx now corresponds to the row AFTER the side-by-side frames


        # --- Command Buttons (ปุ่มคำสั่ง) ---
        button_frame = ttk.Frame(frame)
        # Modified columnspan to 5 and added grid_columnconfigure for 5 columns
        # Adjusted to 5 columns instead of 6 as Stop App button is removed.
        button_frame.grid(row=row_idx, column=0, columnspan=2, pady=(15, 10), sticky="ew", padx=10)
        button_frame.grid_columnconfigure((0,1,2,3,4), weight=1) # Adjusted to 5 columns

        # Applying custom button styles
        ttk.Button(button_frame, text="💾 Save Settings", command=self._save_settings, style='Green.TButton').grid(row=0, column=0, sticky="ew", padx=5) # Increased padx
        self.move_button = ttk.Button(button_frame, text="🚚 Move Now", command=lambda: self._run_in_thread("move"), style='Blue.TButton')
        self.move_button.grid(row=0, column=1, sticky="ew", padx=5) # Increased padx

        self.copy_button = ttk.Button(button_frame, text="📄 Copy Now", command=lambda: self._run_in_thread("copy"), style='Blue.TButton')
        self.copy_button.grid(row=0, column=2, sticky="ew", padx=5) # Increased padx

        self.delete_button = ttk.Button(button_frame, text="🗑️ Delete Now", command=lambda: self._run_in_thread("delete"), style='Red.TButton')
        self.delete_button.grid(row=0, column=3, sticky="ew", padx=5) # Increased padx
        ttk.Button(button_frame, text="⛔ Cancel Operation", command=self._cancel_operation, style='Red.TButton').grid(row=0, column=4, sticky="ew", padx=5) # Increased padx
        
        row_idx += 1

        # --- Log Box (กล่อง Log) ---
        self.log_box = tk.Text(frame, height=15, width=100, state='normal', 
                               bg='#FFFFFF', fg='#4A4A4A', font=('Consolas', 9), 
                               padx=10, pady=10, relief='flat', borderwidth=1, # Increased padx, pady
                               highlightbackground='#D3D3D3', highlightcolor='#A0A0A0', highlightthickness=1) 
        self.log_box.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=10, sticky="nsew") # Span 2 columns
        # เพิ่ม Scrollbar ให้กับ log_box
        log_scrollbar = ttk.Scrollbar(frame, command=self.log_box.yview)
        log_scrollbar.grid(row=row_idx, column=2, sticky='ns', padx=(0, 10)) # Adjusted column for scrollbar
        self.log_box['yscrollcommand'] = log_scrollbar.set
        # ตั้งค่าเป็น 'disabled' หลังจากสร้างเพื่อให้ผู้ใช้แก้ไขไม่ได้
        self.log_box.config(state='disabled')
        row_idx += 1


        # --- Progress Bar & Status (แถบความคืบหน้าและสถานะ) ---
        self.progress_bar = ttk.Progressbar(frame, length=400, mode='determinate', style='Custom.Horizontal.TProgressbar')
        self.progress_bar.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=(5, 5), sticky="ew") # Span 2 columns
        row_idx += 1

        self.progress_label = ttk.Label(frame, text="")
        self.progress_label.grid(row=row_idx, column=0, columnspan=2, sticky="ew", padx=10, pady=(0,5)) # Span 2 columns
        row_idx += 1

        self.next_run_label = ttk.Label(frame, text="📅 Calculating next run time...")
        self.next_run_label.grid(row=row_idx, column=0, columnspan=2, pady=(5, 10), sticky="ew", padx=10) # Span 2 columns


    # --- Logging Functions (ฟังก์ชันการบันทึก Log) ---
    def _log(self, message, to_app_log=False, to_gui_log=True):
        """บันทึกข้อความ Log ไปยัง Console, ไฟล์ และ Log Box ใน GUI พร้อมแสดง Popup แจ้งเตือนข้อผิดพลาด"""
        full_msg = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}"
        print(full_msg) # แสดงใน Console เสมอ

        # เขียน Log ลงไฟล์ app_log.txt
        if to_app_log:
            try:
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(full_msg + "\n")
            except IOError as e:
                # ข้อผิดพลาดในการเขียน app_log เป็นข้อผิดพลาดสำคัญ ควรแสดงใน GUI
                # หลีกเลี่ยงการเรียก messagebox.showerror ซ้ำซ้อนที่นี่ เพื่อไม่ให้เกิดลูปการแจ้งเตือน
                # เพราะ _log จะถูกเรียกอีกครั้งด้วย to_gui_log=True ในบรรทัดถัดไป
                self._log(f"Error writing to app log file {LOG_FILE}: {e}", to_app_log=False, to_gui_log=True) 

        # แสดงใน log_box ของ GUI (ถ้ามีและยังไม่ถูกทำลาย)
        if to_gui_log and hasattr(self, 'log_box') and self.log_box.winfo_exists():
            self.log_box.config(state='normal')
            self.log_box.insert(tk.END, full_msg + "\n")
            self.log_box.see(tk.END) # เลื่อนไปที่บรรทัดสุดท้าย
            self.log_box.config(state='disabled') # ปิดการใช้งานไม่ให้ผู้ใช้แก้ไข

            # --- MODIFICATION: Show error messagebox for messages starting with "❌ Error" ---
            if message.strip().startswith("❌ Error"):
                # Use master.after to ensure messagebox is called on the main thread
                # This prevents potential deadlocks if called directly from a background thread
                self.master.after(0, lambda: messagebox.showerror("Error Occurred", message))


    def _log_action(self, file_name, action_type, status, src=None, dst=None, current_skipped_count=None, total_initial_files=None):
        """
        บันทึกการกระทำกับไฟล์ลงในไฟล์ Action Log โดยเฉพาะ 
        ข้อความเหล่านี้จะถูกบันทึกใน action_log.txt และ app_log.txt แต่จะไม่แสดงใน GUI Log Box
        """
        time_str = datetime.datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')
        base_file_name = os.path.basename(file_name) # ใช้เฉพาะชื่อไฟล์
        msg = f"{time_str} {action_type.upper()} | {base_file_name} | {status}"

        # เพิ่มข้อมูลเส้นทางสำหรับ Action ต่างๆ
        if action_type.upper() == "DELETE" and src:
            src_dir = os.path.dirname(src)
            msg += f" | FROM: {src_dir}"
        elif src and dst:
            src_dir = os.path.dirname(src)
            dst_dir = os.path.dirname(dst)
            msg += f" | FROM: {src_dir} TO: {dst_dir}"
        # สำหรับ SKIP, src คือไฟล์ที่ถูกข้าม
        elif action_type.upper() == "SKIP" and src:
            src_dir = os.path.dirname(src)
            msg += f" | FILE_PATH: {src_dir}"
            if current_skipped_count is not None and total_initial_files is not None:
                msg += f" | SKIPPED {current_skipped_count:,}/{total_initial_files:,} files"

        # บันทึกเข้าไฟล์ ACTION_LOG_FILE เสมอ
        try:
            with open(ACTION_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except IOError as e:
            # ข้อผิดพลาดในการเขียน action_log เป็นข้อผิดพลาดสำคัญ ควรแสดงใน GUI
            self._log(f"❌ Error writing to action log file {ACTION_LOG_FILE}: {e}", to_app_log=True, to_gui_log=True) 

        # บันทึกเข้า app_log.txt (ไม่แสดงใน GUI Log Box)
        # CHANGED: to_app_log=False to prevent duplication in app_log.txt, as it's now exclusively for ACTION_LOG_FILE.
        self._log(msg, to_app_log=False, to_gui_log=False) 

    def _log_process_step(self, message):
        """
        บันทึกข้อความขั้นตอนการประมวลผล (Process Step)
        ข้อความเหล่านี้จะถูกบันทึกใน action_log.txt และ app_log.txt แต่จะไม่แสดงใน GUI Log Box
        """
        full_msg = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - [PROCESS_STEP] - {message}"
        
        # บันทึกเข้า action_log.txt
        try:
            with open(ACTION_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(full_msg + "\n")
        except IOError as e:
            self._log(f"Error writing process step to action log file {ACTION_LOG_FILE}: {e}", to_app_log=True, to_gui_log=True)

        # บันทึกเข้า app_log.txt (ไม่แสดงใน GUI Log Box)
        # CHANGED: to_app_log=False to prevent duplication in app_log.txt, as it's now exclusively for ACTION_LOG_FILE.
        self._log(message, to_app_log=False, to_gui_log=False)


    # --- Settings Management Functions (ฟังก์ชันจัดการการตั้งค่า) ---
    def _load_settings(self):
        """โหลดการตั้งค่าจากไฟล์ JSON"""
        # --- FIX: Changed to load from CONFIG_FILE instead of LAST_RUN_FILE ---
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                    # Removed setdefault for "use_robocopy"
                    return settings
            except json.JSONDecodeError as e:
                self._log(f"❌ Error decoding config file {CONFIG_FILE}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                return {}                                                                                    
            except IOError as e:
                self._log(f"❌ Error reading config file {CONFIG_FILE}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                return {}                                                                                    
        return {}

    def _save_settings(self):
        """บันทึกการตั้งค่าปัจจุบันลงในไฟล์ JSON"""
        config = {
            "source": self.source_var.get(),
            "dest": self.dest_var.get(),
            "file_type": self.file_type_var.get(),
            "auto_day": self.auto_day_var.get(),
            "auto_interval": self.auto_interval_var.get(),
            "auto_operation": self.auto_operation_var.get(),
            "auto_time": self.auto_time_var.get(),
            "min_free_space_gb": self.min_free_space_var.get(),
            "filter_old": self.filter_old_files_var.get(),
            "months_old": self.months_old_var.get()
            # Removed "use_robocopy" from config
        }
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4) # ใช้ indent=4 เพื่อให้อ่านง่าย
            self._log("✅ Configuration saved successfully", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                                                                                             
        except IOError as e:
            self._log(f"❌ Error saving config file {CONFIG_FILE}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI

    def _load_settings_gui(self):
        """โหลดการตั้งค่าที่บันทึกไว้มาแสดงใน Widgets ของ GUI"""
        config = self._load_settings()
        self.source_var.set(config.get("source", ""))
        self.dest_var.set(config.get("dest", ""))
        self.file_type_var.set(config.get("file_type", "All"))
        self.auto_day_var.set(config.get("auto_day", "1"))
        self.auto_interval_var.set(config.get("auto_interval", "1"))
        self.auto_operation_var.set(config.get("auto_operation", "move"))
        self.auto_time_var.set(config.get("auto_time", "00:01"))
        self.min_free_space_var.set(config.get("min_free_space_gb", "5.0"))
        self.filter_old_files_var.set(config.get("filter_old", False))
        self.months_old_var.set(config.get("months_old", "3"))
        # Removed loading of "use_robocopy"


    # --- File/Directory Operations Functions (ฟังก์ชันเกี่ยวกับการทำงานกับไฟล์/โฟลเดอร์) ---
    def _browse_folder(self, var):
        """เปิดหน้าต่างให้ผู้ใช้เลือกโฟลเดอร์และตั้งค่าลงในตัวแปรที่ระบุ"""
        folder = filedialog.askdirectory()
        if folder:
            var.set(folder)

    def _check_free_space_gb(self, path):
        """ตรวจสอบพื้นที่ว่างของดิสก์ในหน่วย GB"""
        try:
            total, used, free = shutil.disk_usage(path)
            free_gb = free / (1024 ** 3)
            total_gb = total / (1024 ** 3)
            return free_gb, total_gb
        except Exception as e:
            self._log(f"❌ Error checking disk space for {path}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
            return 0.0, 0.0 # คืนค่า 0 หากเกิดข้อผิดพลาด

    def _cancel_operation(self):
        """ตั้งค่าสถานะการยกเลิกการทำงาน"""
        self.operation_cancelled = True
        self._log("❌ Operation Cancelled By User", to_app_log=True, to_gui_log=True) # แสดงใน GUI
        self._set_buttons_state("normal") # เปิดใช้งานปุ่มทันทีเมื่อยกเลิก
        # When cancelled, immediately reset is_task_running to allow new runs
        self.is_task_running = False 
        self.master.after(0, self._update_next_run_label) # Update label after canceling

    def _set_buttons_state(self, state):
        """ตั้งค่าสถานะของปุ่ม Move, Copy, Delete"""
        self.move_button.config(state=state)
        self.copy_button.config(state=state)
        self.delete_button.config(state=state)

    def _run_in_thread(self, op):
        """รันการทำงาน (Move/Copy/Delete) ใน Thread แยกต่างหาก เพื่อไม่ให้ GUI ค้าง"""
        if self.is_task_running: # ตรวจสอบว่ามี Task กำลังรันอยู่หรือไม่
            self._log("⚠️ Task is already running. Ignoring new request to run.", to_app_log=True, to_gui_log=True)
            return

        emoji_map = {
            "move": "🔀",
            "copy": "📄",
            "delete": "🗑️"
        }
        emoji = emoji_map.get(op, "ℹ️") # Get emoji based on operation, default to info emoji
        
        self._log(f" 🔁 Starting {op} operation...", to_app_log=True, to_gui_log=True) # แสดงใน GUI
        self.operation_cancelled = False # รีเซ็ตสถานะการยกเลิก
        self._set_buttons_state("disabled") # ปิดการใช้งานปุ่ม
        self.progress_bar["value"] = 0 # รีเซ็ตแถบความคืบหน้า
        self.progress_label.config(text="") # รีเซ็ตข้อความสถานะ
        
        self.is_task_running = True # ตั้งค่าแฟล็กว่ามีงานกำลังรัน
        
        # Update last run date immediately when task starts (NEW LOCATION)
        self._set_last_run_date(datetime.datetime.now().strftime("%Y-%m-%d"))
        # Immediately update the label to show the new "Last Run" date
        self.master.after(0, self._update_next_run_label) 

        # เริ่ม Thread ใหม่สำหรับฟังก์ชัน _safe_run
        threading.Thread(target=lambda: self._safe_run(op), daemon=True).start()

    def _safe_run(self, op):
        """เรียกใช้ฟังก์ชันการทำงานหลักและจัดการกับข้อผิดพลาด/สถานะการทำงาน"""
        try:
            self._move_or_copy_files(op)
        except Exception as e:
            self._log(f"❌ Error: An unexpected error occurred during operation: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
            # Removed direct messagebox.showerror here as it's now handled by _log
        finally:
            # Removed _set_last_run_date and _update_next_run_label from here
            # They are now called when the task starts or when cancelled
            self._set_buttons_state("normal") # Always re-enable buttons regardless of outcome
            self._log(f"Operation '{op}' finished execution.", to_app_log=True, to_gui_log=True) # แสดงใน GUI
            self.is_task_running = False # รีเซ็ตแฟล็กเมื่อ Task สิ้นสุด
            # Ensure label is updated after completion to reflect potential next run if it just finished.
            self.master.after(0, self._update_next_run_label)


    def _move_or_copy_files(self, operation="move"):
        """
        ดำเนินการย้าย, คัดลอก, หรือลบไฟล์ตามการตั้งค่า
        ตอนนี้ใช้ shutil เท่านั้น
        """
        self.operation_cancelled = False # รีเซ็ตสถานะการยกเลิกสำหรับ Task ใหม่
        config = self._load_settings()
        src = config.get("source", "")
        dst = config.get("dest", "")
        file_type = config.get("file_type", "All")
        min_free_space = float(config.get("min_free_space_gb", 5.0))
        filter_old = bool(config.get("filter_old", False))
        months_old = int(config.get("months_old", 3))
        # Removed use_robocopy variable as it's no longer used

        self._log(f"Beginning file operation logic for {operation.capitalize()} from '{src}' to '{dst}' (File type: {file_type}).", to_app_log=True, to_gui_log=True) # แสดงใน GUI

        # Initialize total_files_in_src_initial_count at the very beginning
        all_files_in_src = []
        if os.path.exists(src): # ตรวจสอบว่าโฟลเดอร์ต้นทางมีอยู่ก่อนที่จะพยายามอ่าน
            all_files_in_src = [f for f in os.listdir(src) if os.path.isfile(os.path.join(src, f))]
        total_files_in_src_initial_count = len(all_files_in_src)


        # ตรวจสอบว่าโฟลเดอร์ต้นทาง/ปลายทางมีอยู่จริงหรือไม่
        if not os.path.exists(src):
            msg = f"❌ Error: Source folder not found: {src}"
            self._log(msg, to_app_log=True, to_gui_log=True) # แสดงใน GUI
            return
        if operation != "delete" and not os.path.exists(dst): # โฟลเดอร์ปลายทางจำเป็นเฉพาะ Move/Copy
            msg = f"❌ Error: Destination folder not found: {dst}"
            self._log(msg, to_app_log=True, to_gui_log=True) # แสดงใน GUI
            return

        # ตรวจสอบพื้นที่ว่างของดิสก์ปลายทางสำหรับ Move/Copy
        if operation != "delete":
            free_space, total_space = self._check_free_space_gb(dst)
            if free_space < min_free_space:
                warning = f"❌ Error: Free space on destination ({free_space:.2f} GB) is below minimum required ({min_free_space} GB). Operation halted."
                self._log(warning, to_app_log=True, to_gui_log=True) # แสดงใน GUI
                return

        # Initialize cutoff_time to None 
        cutoff_time = None
        if filter_old:
            try:
                cutoff_time = datetime.datetime.now() - relativedelta(months=months_old)
                self._log(f"📅 Transferring files older than {months_old} months. Cutoff date: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
            except Exception as e:
                self._log(f"❌ Error calculating cutoff time: {e}. Age filter disabled for this operation.", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                filter_old = False # ปิดการกรองอายุหากคำนวณผิดพลาด

        # --- Using shutil for all operations ---
        eligible_files = []
        skipped_initial_shutil = 0
        
        for f in all_files_in_src: # Use the original all_files_in_src which is now guaranteed to be defined
            file_path = os.path.join(src, f)

            # Filter by file type
            if file_type == "Excel" and not f.lower().endswith((".xls", ".xlsx", ".xlsm", ".csv")):
                skipped_initial_shutil += 1
                self._log_action(f, "skip", "WRONG_FILE_TYPE", src=file_path) # Log skip reason for shutil path too
                continue
            
            # Filter by age
            if filter_old:
                try:
                    modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    # Use cutoff_time here, which is now guaranteed to be defined (either None or a datetime object)
                    if cutoff_time is not None and modified_time > cutoff_time: 
                        skipped_initial_shutil += 1
                        self._log_action(f, "skip", f"NOT_OLD_ENOUGH|Modified:{modified_time.strftime('%Y-%m-%d %H:%M:%S')}", src=file_path) # Log skip reason
                        continue
                except Exception as e:
                    self._log(f"❌ Error checking modification time for {f} (shutil path): {e}. Skipping age filter for this file.", to_app_log=True, to_gui_log=True)
                    self._log_action(f, "skip", f"ERROR_MOD_TIME:{e}", src=file_path)
                    pass # Continue processing, but skip age filter for this specific file

            eligible_files.append(f)

        total_files_to_process = len(eligible_files)
        if total_files_to_process == 0 and skipped_initial_shutil == total_files_in_src_initial_count:
            self._log(f"ℹ️ All {total_files_in_src_initial_count:,} files were skipped by filters. No eligible files found to {operation}.", to_app_log=True, to_gui_log=True) 
            self.progress_bar["value"] = 100
            self.progress_label.config(text=f"✅ Completed. All files skipped.")
            return 
        elif total_files_in_src_initial_count == 0:
            self._log(f"ℹ️ No files found in source. Operation finished.", to_app_log=True, to_gui_log=True)
            self.progress_bar["value"] = 100
            self.progress_label.config(text=f"✅ Completed. No files found.")
            return

        # --- Sort eligible files by modification time (oldest first) ---
        # เรียงลำดับไฟล์ที่ผ่านการกรองตามเวลาแก้ไข (modification time) จากเก่าสุดไปใหม่สุด
        # Ensure that only actual file paths are passed to getmtime
        eligible_files_with_mod_time = []
        for f in eligible_files:
            file_path = os.path.join(src, f)
            try:
                eligible_files_with_mod_time.append((os.path.getmtime(file_path), f))
            except Exception as e:
                self._log(f"❌ Error: Could not get modification time for eligible file '{f}': {e}. Skipping from processing.", to_app_log=True, to_gui_log=True)
                # If we cannot get mod time for an eligible file, we should not try to process it.
                total_files_to_process -= 1 # Adjust total eligible count
                skipped_initial_shutil += 1 # Count it as an implicit skip due to error
                continue
        
        eligible_files_with_mod_time.sort(key=lambda x: x[0])
        eligible_files = [f_name for mod_time, f_name in eligible_files_with_mod_time] # Rebuild eligible_files list


        # Calculate total size of eligible files
        total_size_to_process_bytes = 0
        for f_name in eligible_files:
            file_path = os.path.join(src, f_name)
            try:
                total_size_to_process_bytes += os.path.getsize(file_path)
            except Exception as e:
                self._log(f"❌ Error: Could not get size for eligible file '{f_name}': {e}. Skipping from total size calculation.", to_app_log=True, to_gui_log=True)
                # This file's size won't be included in the total, but processing will attempt it later.
                pass # Continue to get size for other files

        self._log(f"Processing {total_files_to_process:,} eligible files with total size {total_size_to_process_bytes / (1024*1024):.2f} MB using shutil...", to_app_log=True, to_gui_log=True) # แสดงใน GUI
        processed_count = 0
        self.total_bytes_processed = 0
        self.start_time = time.time()

        # --- File Processing Loop (for shutil) ---
        for idx, f in enumerate(eligible_files, start=1):
            if self.operation_cancelled:
                self._log("⚠️ Operation cancelled by user. Stopping file processing.", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                break # ออกจากลูปทันที

            source_path = os.path.join(src, f)
            target_path = os.path.join(dst, f) if operation != "delete" else None
            success = False
            file_size = 0 

            # Check if source file actually exists before attempting to process
            if not os.path.isfile(source_path):
                self._log(f"❌ Error: File '{f}' disappeared from source during operation. Skipping.", to_app_log=True, to_gui_log=True)
                # Increment skipped_initial_shutil for files that disappeared mid-operation
                skipped_initial_shutil += 1 
                # This file won't be processed, but we still update GUI to reflect overall iteration progress
                self._update_progress_gui(idx, total_files_to_process, operation, 0, time.time() - self.start_time,
                                          0, processed_count, skipped_initial_shutil, total_files_in_src_initial_count, total_size_to_process_bytes)
                continue # Skip to next file in eligible_files

            try:
                file_start_time = time.time()
                file_size = os.path.getsize(source_path)

                if operation in ("move", "copy") and os.path.exists(target_path):
                    base, ext = os.path.splitext(f)
                    count = 1
                    while os.path.exists(target_path):
                        target_path = os.path.join(dst, f"{base}_copy{count}{ext}")
                        count += 1
                    self._log_process_step(f"File '{f}' already exists in destination. Renaming to '{os.path.basename(target_path)}'.")

                if operation == "move":
                    self._log_process_step(f"[Move Step 1/2] Attempting to copy '{f}' to '{target_path}'.")
                    shutil.copy2(source_path, target_path)

                    if os.path.exists(target_path) and os.path.getsize(source_path) == os.path.getsize(target_path):
                        self._log_process_step(f"[Move Step 1/2] Copy of '{f}' successful. Verifying integrity.")
                        if not self.operation_cancelled:
                            try:
                                self._log_process_step(f"[Move Step 2/2] Attempting to delete original file '{source_path}'.")
                                os.remove(source_path)
                                self._log_action(f, "delete", "SUCCESS", src=source_path)
                                success = True
                                self._log_action(f, "move", "SUCCESS", src=source_path, dst=target_path)
                                self._log_process_step(f"[Move Completed] '{f}' moved successfully.")
                            except Exception as delete_e:
                                self._log(f"❌ Error: [Move Step 2/2 Failed] Error deleting original file '{source_path}': {delete_e}", to_app_log=True, to_gui_log=True) 
                                self._log_action(f, "delete", f"ERROR: {delete_e}", src=source_path)
                                self._log_action(f, "move", "PARTIAL_SUCCESS_DELETE_FAILED", src=source_path, dst=target_path)
                                success = False
                                self._log(f"⚠️ [Move Partial] '{f}' copied, but original not deleted due to error. Check '{source_path}'.", to_app_log=True, to_gui_log=True)
                        else:
                            self._log_action(f, "move", "CANCELLED_AFTER_COPY", src=source_path, dst=target_path)
                            self._log(f"⚠️ [Move Cancelled] Copy successful but deletion skipped due to cancellation: {source_path}", to_app_log=True, to_gui_log=True)
                            success = False
                    else:
                        self._log_action(f, "move", "FAILED_SIZE_MISMATCH", src=source_path, dst=target_path)
                        self._log(f"❌ Error: [Move Failed] File size mismatch or target not found after copy. Skipped deleting: {source_path}", to_app_log=True, to_gui_log=True)
                        success = False

                elif operation == "copy":
                    shutil.copy2(source_path, target_path)
                    self._log_action(f, "copy", "SUCCESS", src=source_path, dst=target_path)
                    success = True

                elif operation == "delete":
                    os.remove(source_path)
                    self._log_action(f, "delete", "SUCCESS", src=source_path)
                    success = True

                if success:
                    processed_count += 1
                    self.total_bytes_processed += file_size

                file_end_time = time.time()
                elapsed_file = file_end_time - file_start_time      
                total_elapsed_time = file_end_time - self.start_time 

                # --- MODIFICATION: Pass total_initial_files_in_src and total_size_to_process_bytes to _update_progress_gui ---
                self._update_progress_gui(idx, total_files_to_process, operation, elapsed_file, total_elapsed_time,
                                          file_size, processed_count, skipped_initial_shutil, total_files_in_src_initial_count, total_size_to_process_bytes)

            except (IOError, OSError) as e:
                # This block specifically handles disk-related errors (e.g., drive disconnected)
                msg = f"❌ Error: Drive disconnected or file system error encountered while processing {source_path}: {e}. Stopping operation."
                self._log(msg, to_app_log=True, to_gui_log=True)
                self._log_action(f, operation, f"DRIVE_ERROR: {e}", src=source_path, dst=target_path)
                self.operation_cancelled = True # Set flag to stop the main loop
                break # Exit the file processing loop immediately
            except Exception as e:
                # This block handles any other unexpected errors during file processing
                msg = f"❌ Error processing {source_path}: {e}"
                self._log(msg, to_app_log=True, to_gui_log=True) 
                self._log_action(f, operation, f"ERROR: {e}", src=source_path, dst=target_path) 

        # --- เพิ่มการนับไฟล์ที่เหลือในโฟลเดอร์ต้นทาง (applies to both robocopy and shutil paths) ---
        # Only count remaining files if the operation was not cancelled due to drive error
        if not self.operation_cancelled:
            remaining_files_in_source_folder = len([f for f in os.listdir(src) if os.path.isfile(os.path.join(src, f))])
        else:
            # If cancelled due to drive error, remaining count is not reliably determinable
            remaining_files_in_source_folder = "N/A (Operation stopped due to drive error)"


        # สิ้นสุดการทำงาน
        # --- MODIFICATION: Include skipped count in the final message ---
        final_msg = (f"✅ Operation finished. Processed {processed_count:,} files. " 
                     f"Skipped {skipped_initial_shutil:,} files. "
                     f"Remaining in source: {remaining_files_in_source_folder} files.")
        self._log(final_msg, to_app_log=True, to_gui_log=True) # แสดงใน GUI
        self.progress_bar["value"] = 100 # ตั้งค่าแถบความคืบหน้าเป็น 100%
        self.progress_label.config(text=f"✅ Completed.")
        

    def _update_progress_gui(self, current_idx, total_eligible_files, operation, elapsed_file, total_elapsed_time,
                             current_file_size, processed_count, skipped_total_count, total_initial_files_in_src, total_size_to_process_bytes):
        """อัปเดตแถบความคืบหน้าและข้อความสถานะใน GUI"""
        if total_eligible_files == 0:
            progress = 100
        else:
            progress = int((current_idx / total_eligible_files) * 100)

        # คำนวณความเร็วและเวลาที่เหลือ
        speed_mbps = (current_file_size / 1024 / 1024) / elapsed_file if elapsed_file > 0 else 0
        avg_speed = (self.total_bytes_processed / 1024 / 1024) / total_elapsed_time if total_elapsed_time > 0 else 0
        files_per_minute = (processed_count / total_elapsed_time * 60) if total_elapsed_time > 0 else 0
        remaining_eligible_files = total_eligible_files - processed_count
        est_time_left = remaining_eligible_files * (total_elapsed_time / processed_count) if processed_count else 0
        hours, rem = divmod(est_time_left, 3600)
        minutes, seconds = divmod(rem, 60)

        # Convert bytes to a more readable format (MB, GB)
        def format_bytes(bytes_val):
            if bytes_val < 1024:
                return f"{bytes_val} B"
            elif bytes_val < (1024 ** 2):
                return f"{bytes_val / 1024:.2f} KB"
            elif bytes_val < (1024 ** 3):
                return f"{bytes_val / (1024 ** 2):.2f} MB"
            else:
                return f"{bytes_val / (1024 ** 3):.2f} GB"

        processed_size_formatted = format_bytes(self.total_bytes_processed)
        total_size_formatted = format_bytes(total_size_to_process_bytes)

        # สร้างข้อความแสดงความคืบหน้า
        # ส่วนนี้แสดงเฉพาะสถานะของการดำเนินการ (Move/Copy/Delete) ของไฟล์ที่ "มีสิทธิ์"
        operation_portion = f"📦 {operation.upper()} {processed_count:,}/{total_eligible_files:,} files ({processed_size_formatted}/{total_size_formatted}) @ {speed_mbps:.2f} MB/s"
        eta_portion = f"⏳ ETA: {int(hours)}h {int(minutes)}m {int(seconds)}s | Avg: {avg_speed:.2f} MB/s | FPM: {files_per_minute:,.2f}"
        
        # --- MODIFICATION: Display skipped files out of total initial files ---
        skipped_portion = f"Skipped: {skipped_total_count:,}/{total_initial_files_in_src:,} files"

        # รวมข้อความ
        combined_msg = f"{skipped_portion} | {operation_portion} | {eta_portion}"

        self.progress_bar["value"] = progress
        self.progress_label.config(text=combined_msg)
        self.master.update_idletasks() # บังคับให้ GUI อัปเดตทันที

    # --- Scheduling Functions (ฟังก์ชันการตั้งเวลา) ---
    def _get_last_run_date(self):
        """ดึงวันที่รัน Task ล่าสุดจากไฟล์ JSON"""
        if os.path.exists(LAST_RUN_FILE):
            try:
                with open(LAST_RUN_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("last_run", "")
            except json.JSONDecodeError as e:
                self._log(f"❌ Error decoding last run file {LAST_RUN_FILE}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                return ""
            except IOError as e:
                self._log(f"❌ Error reading last run file {LAST_RUN_FILE}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                return ""
        return ""

    def _set_last_run_date(self, date_str):
        """บันทึกวันที่รัน Task ปัจจุบันลงในไฟล์ JSON"""
        try:
            with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
                json.dump({"last_run": date_str}, f, indent=4)
        except IOError as e:
            self._log(f"❌ Error writing last run file {LAST_RUN_FILE}: {e}", to_app_log=True, to_gui_log=True) # แสดงใน GUI

    def _get_valid_datetime(self, year, month, day, time_obj):
        """
        Helper to create a datetime object, handling day exceeding month's days.
        จะถูกเรียกใช้ภายใน _should_schedule_run เพื่อจัดการกับวันที่ที่ไม่ถูกต้อง
        เช่น วันที่ 31 ในเดือนกุมภาพันธ์ จะถูกปรับเป็นวันที่ 28 หรือ 29
        """
        # คำนวณวันสุดท้ายของเดือนนั้นๆ
        last_day_of_month = (datetime.date(year, month, 1) + relativedelta(months=1) - datetime.timedelta(days=1)).day
        # ใช้วันที่ที่เล็กกว่าระหว่าง auto_day และวันสุดท้ายของเดือน
        valid_day = min(day, last_day_of_month)
        return datetime.datetime(year, month, valid_day, time_obj.hour, time_obj.minute, 0, 0)

    def _should_schedule_run(self):
        """
        คำนวณว่าถึงเวลาที่ควรจะรัน Task อัตโนมัติแล้วหรือยัง
        และคำนวณเวลาที่ควรจะรันครั้งถัดไป
        """
        now = datetime.datetime.now()
        # self._log(f"Scheduler Debug: Current time (now): {now.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log

        # --- FIX: Load config from CONFIG_FILE here as well ---
        config = self._load_settings() # This will now call the corrected _load_settings method
        try:
            auto_day = int(config.get("auto_day", 1))
            auto_time_str = config.get("auto_time", "00:01")
            
            auto_interval = int(config.get("auto_interval", "1"))
            if auto_interval < 0:
                self._log(f"⚠️ 'Repeat Every N Months' is set to {auto_interval}, which is invalid. Using 1 month for calculation.", to_app_log=True, to_gui_log=True) # แสดงใน GUI
                auto_interval = 1 

            target_hour, target_minute = map(int, auto_time_str.split(":"))
            configured_auto_time_obj = datetime.time(target_hour, target_minute)
            # self._log(f"Scheduler Debug: Configured auto_day={auto_day}, auto_time={auto_time_str}, auto_interval={auto_interval}", to_app_log=True, to_gui_log=True) # Removed debug log

        except ValueError as e:
            self._log(f"❌ Error: Invalid auto-scheduling configuration (Day/Time/Interval): {e}. Please check settings.", to_app_log=True, to_gui_log=True) # แสดงใน GUI
            return False, datetime.datetime.now(), "❌ Error: Invalid schedule config."

        last_run_str = self._get_last_run_date()
        # self._log(f"Scheduler Debug: Raw last_run from file: '{last_run_str}'", to_app_log=True, to_gui_log=True) # Removed debug log
        
        last_run_dt_from_file = None 
        if last_run_str:
            try:
                # Ensure last_run_dt_from_file also uses the configured time for consistent comparison
                last_run_dt_from_file = datetime.datetime.strptime(last_run_str, "%Y-%m-%d").replace(
                    hour=configured_auto_time_obj.hour, minute=configured_auto_time_obj.minute, second=0, microsecond=0
                )
            except ValueError:
                self._log(f"❌ Error: Invalid last run date format in {LAST_RUN_FILE}: {last_run_str}. Ignoring last run date for calculation.", to_app_log=True, to_gui_log=True) # แสดงใน GUI
        
        # Determine the effective last run datetime. If no last run, use a very old date.
        # This makes the logic cleaner for the first run.
        effective_last_run_dt = last_run_dt_from_file if last_run_dt_from_file else datetime.datetime(1900, 1, 1, 0, 0)
        # self._log(f"Scheduler Debug: Effective last_run_dt: {effective_last_run_dt.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log

        run_now = False
        next_scheduled_run_display = None
        
        # --- Special handling for auto_interval = 0 (Immediate Run for Testing/One-off Daily) ---
        if auto_interval == 0:
            configured_time_today = now.replace(hour=configured_auto_time_obj.hour, minute=configured_auto_time_obj.minute, second=0, microsecond=0)
            # self._log(f"Scheduler Debug: Interval 0. Configured time today: {configured_time_today.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log
            
            # Should run now if:
            # 1. It hasn't run today yet (or ever), AND the configured time for today has passed.
            if (effective_last_run_dt.date() < now.date() or effective_last_run_dt.year == 1900) and now >= configured_time_today:
                run_now = True
                # Next display time: tomorrow at configured time
                next_scheduled_run_display = configured_time_today + datetime.timedelta(days=1)
                
            # Not yet time to run today
            elif now < configured_time_today:
                run_now = False
                next_scheduled_run_display = configured_time_today # Display time is today's configured time
            # Already ran today
            else: # effective_last_run_dt.date() == now.date() and now >= configured_time_today
                run_now = False
                next_scheduled_run_display = configured_time_today + datetime.timedelta(days=1) # Display time is tomorrow
            
            # Construct message with last_run_str
            last_run_info = f"Last Run: {last_run_str if last_run_str else 'Never'}"
            full_next_run_msg = f"{last_run_info} | Next Scheduled: {next_scheduled_run_display.strftime('%Y-%m-%d %H:%M')}"
            
            return run_now, next_scheduled_run_display, full_next_run_msg
        
        # --- Monthly Scheduling (auto_interval > 0) ---
        
        # Calculate the next scheduled date that is strictly AFTER the effective_last_run_dt
        # 'current_candidate_dt' will serve as the 'target_dt_after_last_run'
        
        # Start candidate from current month's calculated auto_day and time
        # This gives us the target for the current month if it's after last run.
        current_candidate_dt = self._get_valid_datetime(now.year, now.month, auto_day, configured_auto_time_obj)
        # self._log(f"Scheduler Debug: Initial current_candidate_dt (based on now.month): {current_candidate_dt.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log

        # Loop to find the FIRST scheduled time (auto_day, auto_time) that is
        # strictly AFTER the effective_last_run_dt.
        # This handles cases where the app was off for a long time and missed multiple scheduled runs.
        # We need to ensure we find the *first* upcoming run that hasn't been recorded.
        while current_candidate_dt <= effective_last_run_dt:
            # self._log(f"Scheduler Debug: current_candidate_dt ({current_candidate_dt.strftime('%Y-%m-%d %H:%M:%S')}) <= effective_last_run_dt ({effective_last_run_dt.strftime('%Y-%m-%d %H:%M:%S')}). Advancing by {auto_interval} month(s).", to_app_log=True, to_gui_log=True) # Removed debug log
            current_candidate_dt += relativedelta(months=auto_interval)
            current_candidate_dt = self._get_valid_datetime(
                current_candidate_dt.year,
                current_candidate_dt.month,
                auto_day,
                configured_auto_time_obj
            )
            # self._log(f"Scheduler Debug: Advanced current_candidate_dt (after last_run check): {current_candidate_dt.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log
        
        # At this point, current_candidate_dt is the *first scheduled run* that is after `effective_last_run_dt`.
        # This is our actual target for this cycle.
        
        # Now, determine if 'now' is at or past this calculated 'current_candidate_dt'.
        run_now = (now >= current_candidate_dt)
        # self._log(f"Scheduler Debug: Final decision: run_now = {run_now} (now={now.strftime('%Y-%m-%d %H:%M:%S')}, candidate={current_candidate_dt.strftime('%Y-%m-%d %H:%M:%S')})", to_app_log=True, to_gui_log=True) # Removed debug log
        
        if run_now:
            # If we are running now, the next time to display in the GUI is the *next* scheduled interval after this one.
            # This is important so the label always points to a future run after the current one triggers.
            next_scheduled_run_display = current_candidate_dt + relativedelta(months=auto_interval)
            next_scheduled_run_display = self._get_valid_datetime(
                next_scheduled_run_display.year,
                next_scheduled_run_display.month,
                auto_day,
                configured_auto_time_obj
            )
            # self._log(f"Scheduler Debug: Displaying next run (after current run): {next_scheduled_run_display.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log
        else:
            # If not running now, the `current_candidate_dt` *is* the next time to display.
            # It's already in the future relative to 'now' because 'run_now' is False.
            next_scheduled_run_display = current_candidate_dt
            # self._log(f"Scheduler Debug: Displaying next run (waiting for it): {next_scheduled_run_display.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True) # Removed debug log


        delta = relativedelta(next_scheduled_run_display, now)
        months_remaining = delta.years * 12 + delta.months
        days_remaining = delta.days
        hours_remaining = delta.hours
        minutes_remaining = delta.minutes

        remaining_time_parts = []
        if months_remaining > 0:
            remaining_time_parts.append(f"{months_remaining} month{'s' if months_remaining > 1 else ''}")
        if days_remaining > 0:
            remaining_time_parts.append(f"{days_remaining} day{'s' if days_remaining > 1 else ''}")
        if hours_remaining > 0:
            remaining_time_parts.append(f"{hours_remaining} hour{'s' if hours_remaining > 1 else ''}")
        if minutes_remaining > 0:
            remaining_time_parts.append(f"{minutes_remaining} minute{'s' if minutes_remaining > 1 else ''}")
        
        remaining_time_str = ""
        if remaining_time_parts:
            remaining_time_str = "in " + ", ".join(remaining_time_parts)


        # Construct message with last_run_str
        last_run_info = f"Last Run: {last_run_str if last_run_str else 'Never'}"
        full_next_run_msg = f"{last_run_info} | Next Scheduled: {next_scheduled_run_display.strftime('%Y-%m-%d %H:%M')}{' | ' + remaining_time_str if remaining_time_str else ''}"
        
        return run_now, next_scheduled_run_display, full_next_run_msg

    def _scheduled_job(self):
        """
        ฟังก์ชันที่ถูกเรียกโดย Thread Background ทุกนาที
        เพื่อตรวจสอบว่าถึงเวลาที่จะรัน Task อัตโนมัติแล้วหรือยัง
        """
        self._log("Scheduler: Checking for scheduled run...", to_app_log=True, to_gui_log=False) # Log to app_log only, not GUI clutter
        
        # เพิ่มการตรวจสอบแฟล็ก is_task_running ก่อนพิจารณาการรัน
        if self.is_task_running:
            self._log("Scheduler: Task is currently running. Skipping scheduled check to prevent overlap.", to_app_log=True, to_gui_log=False)
            # Update label to reflect that a task is in progress, even if it's the current one
            self.master.after(0, lambda: self.next_run_label.config(text=f"⏳ A task is in progress..."))
            return # ไม่ต้องทำต่อถ้ามีงานกำลังรันอยู่

        run_now, next_scheduled_run_display, next_run_info_msg = self._should_schedule_run()
        # Update GUI label immediately when scheduler checks
        self.master.after(0, lambda: self.next_run_label.config(text=f"⏳ {next_run_info_msg}")) 
        
        if run_now:
            config = self._load_settings()
            auto_operation = config.get("auto_operation", "move")
            
            self._log(f"✅ Scheduled time reached - Starting data transfer ({auto_operation.upper()}).", to_app_log=True, to_gui_log=True) # แสดงใน GUI
            self._run_in_thread(auto_operation) # เริ่ม Task ใน Thread แยก
        else:
            # No need to log "pending" messages to GUI here as it's already updated in next_run_label
            self._log(f"Scheduler: Not yet time to run. Next run: {next_run_info_msg}", to_app_log=True, to_gui_log=False) # Log to app_log only


    def _start_scheduler_thread(self):
        """เริ่มต้น Thread สำหรับการตรวจสอบ Task อัตโนมัติอย่างต่อเนื่อง"""
        def run_schedule_loop():
            # Initial sleep to allow GUI to set up before first check
            time.sleep(5) 
            while True:
                try:
                    self._scheduled_job()
                except Exception as e:
                    self._log(f"❌ Error: Scheduler Thread Error: {e}", to_app_log=True, to_gui_log=True) # เพิ่ม '❌ Error'
                time.sleep(60) # ตรวจสอบทุก 60 วินาที

        # เริ่ม Thread แบบ daemon เพื่อให้มันหยุดทำงานเมื่อโปรแกรมหลักปิด
        threading.Thread(target=run_schedule_loop, daemon=True).start()
        self._log("Scheduler: Background scheduler thread started.", to_app_log=True, to_gui_log=True)


    def _update_next_run_label(self):
        """อัปเดตข้อความแสดงเวลาการทำงานรอบถัดไปใน GUI"""
        try:
            # Re-evaluate and update the label. This ensures it's always fresh.
            # No need for deep debug logging here as _should_schedule_run will handle it.
            # Check if a task is already running before updating with next scheduled time
            if self.is_task_running:
                self.next_run_label.config(text=f"⏳ A task is in progress...")
            else:
                _, _, full_next_run_msg = self._should_schedule_run()
                self.next_run_label.config(text=f"⏳ {full_next_run_msg}")

        except Exception as e:
            print(f"ERROR: Exception within _update_next_run_label main logic: {e}")
            self._log(f"❌ Error: Failed to update next scheduled time label: {e}", to_app_log=True, to_gui_log=True) # เพิ่ม '❌ Error'
            # Removed direct label update here as _log will handle it if to_gui_log is True
            # self.next_run_label.config(text="❌ Error calculating next run time.")

        # ตั้งเวลาให้ฟังก์ชันนี้ถูกเรียกใหม่ทุก 60 วินาทีเพื่ออัปเดตสถานะ
        self.master.after(60000, self._update_next_run_label)

    # Removed _stop_application function as requested

if __name__ == "__main__":
    root = tk.Tk()
    try:
        app = FileManagerApp(root)
    except Exception as e:
        print(f"ERROR: Exception during FileManagerApp instantiation: {e}")
        # Show a final error message if the app cannot be instantiated
        messagebox.showerror("Application Startup Error", f"Failed to start the application: {e}")
        sys.exit(1) # Exit if app cannot be created, to prevent hanging process

    root.mainloop()
