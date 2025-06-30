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

# --- Constants ---
CONFIG_FILE = "move_config.json"
# ไฟล์ Log หลักของแอปพลิเคชัน (ครอบคลุมเฉพาะ Log หลักของแอปพลิเคชัน)
LOG_FILE = "app_log.txt"
# ไฟล์ Log สำหรับบันทึกการกระทำกับไฟล์ (ย้าย, คัดลอก, ลบ) และขั้นตอนย่อย
ACTION_LOG_FILE = "action_log.txt"
# ไฟล์สำหรับบันทึกวันที่รัน Task ล่าสุด
LAST_RUN_FILE = "last_run.json"
# เพิ่มไฟล์ Log สำหรับเก็บข้อผิดพลาดโดยเฉพาะ
ERROR_LOG_FILE = "error_log.txt"

# --- Custom Exception for Critical Operations ---
class OperationCriticalError(Exception):
    """ข้อยกเว้นสำหรับข้อผิดพลาดที่ควรกระทบกับการทำงานทั้งหมด"""
    pass

class FileManagerApp:
    def __init__(self, master):
        self.master = master
        # เปลี่ยนชื่อแอปพลิเคชันเป็น "Auto Data Transfer"
        master.title("Auto Data Transfer") 
        master.resizable(True, True) # อนุญาตให้ปรับขนาดหน้าต่างได้
        # กำหนดขนาดขั้นต่ำของหน้าต่างเพื่อรักษาสุนทรียภาพของเลย์เอาต์
        # ปรับขนาดขั้นต่ำเพื่อรองรับส่วนข้างเคียงกัน
        self.master.minsize(700, 550) # เพิ่มความกว้างให้พอดีกับสองเฟรมที่วางข้างกัน

        # --- Variables for status and GUI (ตัวแปรสำหรับสถานะและ GUI) ---
        self.operation_cancelled = False # สถานะการยกเลิกการทำงานของไฟล์
        self.start_time = time.time()  # เวลาเริ่มต้นของการทำงาน
        self.total_bytes_processed = 0 # จำนวนไบต์ที่ถูกประมวลผล (สำหรับคำนวณความเร็ว)
        self.is_task_running = False # เพิ่มแฟล็กเพื่อควบคุมการทำงานซ้อนกัน
        self.consecutive_skip_errors = 0 # เพิ่มตัวนับสำหรับการข้ามไฟล์ติดต่อกัน
        # กำหนดจำนวนสูงสุดของการข้ามไฟล์ติดต่อกันก่อนจะถือว่าเป็นข้อผิดพลาดวิกฤติ
        self.MAX_CONSECUTIVE_SKIP_ERRORS = 10 

        # ตัวแปรเฉพาะ Animation สำหรับป้ายข้อความ "กำลังดำเนินการ..."
        self.loading_dots_count = 0  # เพื่อวนรอบจำนวนจุด
        # ชุดอิโมจิสำหรับ Animation การโหลด
        self.loading_animation_emojis = ["☐→          📁",
                                         "◰ →         📁",
                                         "◱  →        �",
                                         "◲   →       📁",
                                         "◳    →      📁",
                                         "◧     →     📁",
                                         "◨      →    📁",
                                         "◩       →   📁",
                                         "◪        →  📁",
                                         "⊞         →📁",
                                         "▩           💾"]
        # เพื่อเก็บ ID ของการเรียก after ที่กำหนดเวลาไว้สำหรับการอัปเดตป้าย
        self.after_id_update_label = None

        # Initialize ttk.Style for GUI customization (กำหนดสไตล์สำหรับ GUI)
        self.style = ttk.Style()
        self._configure_styles() # เรียกเมธอดเพื่อตั้งค่าสไตล์ที่กำหนดเอง

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

        # --- GUI Setup (การตั้งค่า GUI) ---
        try:
            self._create_widgets()         # สร้างส่วนประกอบของ GUI
        except Exception as e:
            print(f"ERROR: Exception during _create_widgets: {e}")
            self._log(f"❌ ข้อผิดพลาดร้ายแรงในการสร้าง Widgets GUI: {e}", to_app_log=True, to_gui_log=False, show_popup=True)
            raise # Re-raise เพื่อดู stack traceback แบบเต็มใน terminal

        # ตรวจสอบและสร้าง last_run.json ทันทีหากไม่มีอยู่
        if not os.path.exists(LAST_RUN_FILE):
            # ใช้ LAST_RUN_FILE ตามที่กำหนดใน constants
            self._log(f"ℹ️ ไม่พบไฟล์ {LAST_RUN_FILE} เริ่มต้นด้วยวันที่ปัจจุบัน", to_app_log=True, to_gui_log=True)
            self._set_last_run_date(datetime.datetime.now().strftime("%Y-%m-%d"))

        try:
            self._load_settings_gui()      # โหลดการตั้งค่าที่บันทึกไว้มาแสดงใน GUI
        except Exception as e:
            print(f"ERROR: Exception during _load_settings_gui: {e}")
            self._log(f"❌ ข้อผิดพลาดร้ายแรงในการโหลดการตั้งค่า GUI: {e}", to_app_log=True, to_gui_log=False, show_popup=True)
            raise

        # เรียกครั้งแรกเพื่ออัปเดตป้ายบอกเวลารันครั้งถัดไป (จะเริ่มการอัปเดตต่อเนื่องด้วย)
        self._update_next_run_label()  

        self._start_scheduler_thread() # เริ่มต้น Thread สำหรับการตรวจสอบ Task อัตโนมัติ

        # --- คำสั่งเพิ่มเติมเพื่อช่วยให้หน้าต่าง GUI แสดงผลอย่างชัดเจน ---
        # ประมวลผลเหตุการณ์ที่รอดำเนินการเพื่อให้แน่ใจว่าหน้าต่างพร้อม
        self.master.update_idletasks() 
        # ตรวจสอบให้แน่ใจว่าหน้าต่างไม่ถูกย่อขนาด
        self.master.deiconify() 
        # นำหน้าต่างขึ้นมาด้านบนสุด
        self.master.lift()      
        # บังคับให้โฟกัสไปที่หน้าต่าง
        self.master.focus_force() 
        # (สำหรับ Windows: อาจช่วยให้หน้าต่างปรากฏ)
        self.master.attributes('-topmost', True) 
        self.master.after_idle(self.master.attributes, '-topmost', False) # เอา topmost ออกหลังจากที่แสดงผลแล้ว


    def _configure_styles(self):
        """กำหนดค่าและใช้สไตล์ ttk ที่กำหนดเองสำหรับองค์ประกอบ GUI สำหรับโหมดแสงที่ทันสมัยและสบายตา"""
        # ตั้งค่าธีมที่ทันสมัยเป็นฐาน
        self.style.theme_use('clam') 

        # กำหนดฟอนต์ที่สอดคล้องกันสำหรับแอปพลิเคชันเพื่อให้อ่านง่าย
        app_font = ('Segoe UI', 9) 
        bold_app_font = ('Segoe UI', 9, 'bold')

        # สีพื้นหลังทั่วไปสำหรับหน้าต่างหลักและเฟรม (สีเทาอ่อนเกือบขาว)
        self.master.configure(bg='#F5F5F5') 
        self.style.configure('TFrame', background='#F5F5F5')
        
        # สไตล์ LabelFrame สำหรับการแยกส่วนที่ชัดเจนด้วยเส้นขอบที่ละเอียดอ่อน
        self.style.configure('TLabelframe', 
                             background='#F5F5F5', 
                             bordercolor='#D3D3D3', # เส้นขอบสีเทาอ่อน
                             relief='groove', # ให้ผลกระทบที่ดูเหมือนยุบเล็กน้อย
                             borderwidth=1)
        self.style.configure('TLabelframe.Label', 
                             background='#F5F5F5', 
                             foreground='#4A4A4A', # ข้อความที่เข้มขึ้นสำหรับหัวข้อเพื่อให้คอนทราสต์ดี
                             font=bold_app_font) 

        # สไตล์สำหรับ Label มาตรฐาน
        self.style.configure('TLabel', 
                             background='#F5F5F5', 
                             foreground='#4A4A4A', # ข้อความสีเข้มเพื่อให้อ่านง่าย
                             font=app_font) 
        
        # สไตล์สำหรับช่อง Entry และ Combobox เพื่อรูปลักษณ์ที่สะอาดและเป็นมืออาชีพ
        self.style.configure('TEntry', 
                             fieldbackground='#FFFFFF', # ช่องป้อนข้อมูลสีขาว
                             foreground='#4A4A4A', 
                             borderwidth=1, 
                             relief="solid", # เส้นขอบทึบ
                             font=app_font, 
                             padding=3) # ลดระยะห่างภายในเพื่อความกะทัดรัด
        self.style.configure('TCombobox', 
                             fieldbackground='#FFFFFF', 
                             foreground='#4A4A4A', 
                             borderwidth=1, 
                             relief="solid", 
                             font=app_font, 
                             padding=3) # ลดระยะห่างภายในเพื่อความกะทัดรัด
        
        # สไตล์สำหรับ Checkbuttons
        self.style.configure('TCheckbutton', 
                             background='#F5F5F5', 
                             foreground='#4A4A4A', 
                             font=app_font)

        # สไตล์ปุ่มที่กำหนดเองเพื่อความแตกต่างทางสายตาและการตอบสนอง (รักษาโทนสีเดิม)
        # ปุ่มบันทึก (สีเขียวสดใสสำหรับการดำเนินการเชิงบวก)
        self.style.configure('Green.TButton',
                             background='#4CAF50', # Material Design Green
                             foreground='white',
                             font=bold_app_font,
                             padding=[10, 5], # ลดระยะห่างเพื่อความกะทัดรัด
                             relief='flat',
                             focuscolor='#4CAF50', 
                             borderwidth=0)
        self.style.map('Green.TButton',
                       background=[('active', '#43A047'), ('pressed', '#388E3C'), ('disabled', '#6A8B6A')], # เพิ่มสถานะถูกปิดใช้งาน
                       foreground=[('active', 'white'), ('disabled', '#cccccc')]) # ข้อความที่จางลงเมื่อถูกปิดใช้งาน
        
        # ปุ่มย้าย/คัดลอก (สีน้ำเงินมาตรฐานสำหรับการดำเนินการหลัก)
        self.style.configure('Blue.TButton',
                             background='#2196F3', # Material Design Blue
                             foreground='white',
                             font=bold_app_font,
                             padding=[10, 5], # ลดระยะห่างเพื่อความกะทัดรัด
                             relief='flat',
                             focuscolor='#2196F3',
                             borderwidth=0)
        self.style.map('Blue.TButton',
                       background=[('active', '#1976D2'), ('pressed', '#1565C0'), ('disabled', '#5B8FB3')], # เพิ่มสถานะถูกปิดใช้งาน
                       foreground=[('active', 'white'), ('disabled', '#cccccc')]) # ข้อความที่จางลงเมื่อถูกปิดใช้งาน

        # ปุ่มลบ/ยกเลิก (สีแดงมาตรฐานสำหรับการดำเนินการทำลาย/หยุด)
        self.style.configure('Red.TButton',
                             background='#F44336', # Material Design Red
                             foreground='white',
                             font=bold_app_font,
                             padding=[10, 5], # ลดระยะห่างเพื่อความกะทัดรัด
                             relief='flat',
                             focuscolor='#F44336',
                             borderwidth=0)
        self.style.map('Red.TButton',
                       background=[('active', '#D32F2F'), ('pressed', '#C62828'), ('disabled', '#8A6D6B')], # เพิ่มสถานะถูกปิดใช้งาน
                       foreground=[('active', 'white'), ('disabled', '#cccccc')]) # ข้อความที่จางลงเมื่อถูกปิดใช้งาน

        # สไตล์ Progress Bar สำหรับการบ่งชี้ความคืบหน้าที่ชัดเจน
        self.style.configure('Custom.Horizontal.TProgressbar',
                             background='#4CAF50', # เติมสีเขียว
                             troughcolor='#E0E0E0', # รางสีเทาอ่อน
                             bordercolor='#E0E0E0', # เส้นขอบตรงกับราง
                             lightcolor='#4CAF50', 
                             darkcolor='#4CAF50')

    def _create_widgets(self):
        """สร้างและจัดวางส่วนประกอบทั้งหมดของ GUI รวมถึงส่วนหัวพร้อมโลโก้"""
        # เฟรมหลักสำหรับจัดวาง Widgets
        frame = ttk.Frame(self.master, padding=(15, 15)) # เพิ่มระยะห่างโดยรวม
        frame.grid(row=0, column=0, sticky="nsew") 
        self.master.grid_rowconfigure(0, weight=1)    
        self.master.grid_columnconfigure(0, weight=1) 
        
        # กำหนดค่าแถวที่กล่อง log อยู่ให้ขยายในแนวตั้ง
        frame.grid_rowconfigure(4, weight=1) # กล่อง Log อยู่ที่แถว 4
        frame.grid_columnconfigure(0, weight=1) # อนุญาตให้คอลัมน์ 0 ขยาย (สำหรับเนื้อหาด้านซ้าย)
        frame.grid_columnconfigure(1, weight=1) # อนุญาตให้คอลัมน์ 1 ขยาย (สำหรับเนื้อหาด้านขวา)
        
        row_idx = 0 # ดัชนีแถวเริ่มต้นสำหรับการวาง Widgets

        # --- Header Frame for Title and Logo (ส่วนหัวสำหรับชื่อแอปและโลโก้) ---
        header_frame = ttk.Frame(frame, padding=(0, 5)) # เพิ่มระยะห่างเพื่อแยกจากด้านบน/ล่าง
        # ครอบคลุม 2 คอลัมน์ (0 และ 1) เนื่องจากไม่มีคอลัมน์โลโก้แยกต่างหากอีกแล้ว
        header_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=(0, 10)) 
        header_frame.grid_columnconfigure(0, weight=1) # ทำให้คอลัมน์เดียวขยายได้

        # ป้ายชื่อแอปพร้อมอีโมจิขนาดใหญ่ - เปลี่ยนข้อความ
        # เพิ่มขนาดฟอนต์จาก 10 เป็น 20 เพื่อให้อีโมจิและข้อความใหญ่ขึ้น
        ttk.Label(header_frame, text="📦 Auto Data Transfer", font=('Segoe UI', 20, 'bold'), 
                  foreground='#333333').grid(row=0, column=0, sticky='ew', padx=10)
        
        row_idx += 1 # เพิ่มดัชนีแถวหลังจากเฟรมหัวข้อ

        # --- Folder Paths Section (การตั้งค่าเส้นทาง) ---
        path_frame = ttk.LabelFrame(frame, text="เส้นทางโฟลเดอร์", padding=(10, 10)) # เพิ่มระยะห่าง
        path_frame.grid(row=row_idx, column=0, columnspan=2, sticky="ew", pady=10, padx=10) # ครอบคลุม 2 คอลัมน์เพื่อใช้ความกว้างเต็ม
        path_frame.grid_columnconfigure(1, weight=1) # ทำให้ช่อง Entry ขยายได้ภายในเฟรมนี้

        ttk.Label(path_frame, text="📂 โฟลเดอร์ต้นทาง:").grid(row=0, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ, เพิ่ม pady, padx
        ttk.Entry(path_frame, textvariable=self.source_var, width=50).grid(row=0, column=1, sticky="ew", pady=5) # เพิ่ม pady
        ttk.Button(path_frame, text="เรียกดู", command=lambda: self._browse_folder(self.source_var)).grid(row=0, column=2, sticky="w", padx=(8,0), pady=5) # เพิ่ม padx, pady

        ttk.Label(path_frame, text="📁 โฟลเดอร์ปลายทาง:").grid(row=1, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ, เพิ่ม pady, padx
        ttk.Entry(path_frame, textvariable=self.dest_var, width=50).grid(row=1, column=1, sticky="ew", pady=5) # เพิ่ม pady
        ttk.Button(path_frame, text="เรียกดู", command=lambda: self._browse_folder(self.dest_var)).grid(row=1, column=2, sticky="w", padx=(8,0), pady=5) # เพิ่ม padx, pady
        row_idx += 1 

        # --- File Filtering & Conditions Section (ประเภทไฟล์และเงื่อนไข) ---
        filter_group_frame = ttk.LabelFrame(frame, text="การกรองไฟล์และเงื่อนไข", padding=(10, 10)) # เพิ่มระยะห่าง
        # วางในคอลัมน์ 0
        filter_group_frame.grid(row=row_idx, column=0, sticky="nsew", pady=10, padx=(10, 5)) # ปรับ padx สำหรับวางข้างกัน
        filter_group_frame.grid_columnconfigure(1, weight=1) # อนุญาตให้ช่องป้อนข้อมูลในเฟรมนี้ขยายได้

        ttk.Label(filter_group_frame, text="🗃️ ประเภทไฟล์:").grid(row=0, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ, เพิ่ม pady, padx
        ttk.Combobox(filter_group_frame, textvariable=self.file_type_var, values=["ทั้งหมด", "Excel"], width=10, state="readonly").grid(row=0, column=1, sticky="ew", pady=5) # เพิ่ม pady

        # เฟรมสำหรับตัวเลือก "เฉพาะไฟล์ที่เก่ากว่า" สำหรับการจัดวางที่เรียบร้อย
        filter_age_frame = ttk.Frame(filter_group_frame) 
        filter_age_frame.grid(row=1, column=0, columnspan=2, sticky="w", pady=5, padx=(0, 8)) # เพิ่ม pady, padx
        ttk.Checkbutton(filter_age_frame, text="✅ เฉพาะไฟล์ที่เก่ากว่า (เดือน):", variable=self.filter_old_files_var).pack(side="left")
        ttk.Entry(filter_age_frame, textvariable=self.months_old_var, width=5).pack(side="left", padx=(5,0)) # เพิ่ม padx

        # --- Automated Task Settings Section (การตั้งค่าการทำงานอัตโนมัติ) ---
        auto_settings_frame = ttk.LabelFrame(frame, text="การตั้งค่าการทำงานอัตโนมัติ", padding=(10, 10)) # เพิ่มระยะห่าง
        # วางในคอลัมน์ 1, ถัดจาก filter_group_frame
        auto_settings_frame.grid(row=row_idx, column=1, sticky="nsew", pady=10, padx=(5, 10)) # ปรับ padx สำหรับวางข้างกัน
        auto_settings_frame.grid_columnconfigure(1, weight=1) # อนุญาตให้ช่องป้อนข้อมูลในเฟรมนี้ขยายได้

        ttk.Label(auto_settings_frame, text="🗓️ วันที่ทำงานอัตโนมัติของเดือน:").grid(row=0, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ
        ttk.Entry(auto_settings_frame, textvariable=self.auto_day_var, width=10).grid(row=0, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="🔄 ทำซ้ำทุกๆ N เดือน:").grid(row=1, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ
        ttk.Entry(auto_settings_frame, textvariable=self.auto_interval_var, width=10).grid(row=1, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="การทำงานอัตโนมัติ:").grid(row=2, column=0, sticky="w", pady=5, padx=(0, 8))
        ttk.Combobox(auto_settings_frame, textvariable=self.auto_operation_var, values=["move", "copy", "delete"], width=10, state="readonly").grid(row=2, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="⏰ เวลาทำงานอัตโนมัติ (HH:MM):").grid(row=3, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ
        ttk.Entry(auto_settings_frame, textvariable=self.auto_time_var, width=10).grid(row=3, column=1, sticky="ew", pady=5)

        ttk.Label(auto_settings_frame, text="💾 พื้นที่ว่างขั้นต่ำ (GB):").grid(row=4, column=0, sticky="w", pady=5, padx=(0, 8)) # เพิ่มอีโมจิ
        ttk.Entry(auto_settings_frame, textvariable=self.min_free_space_var, width=10).grid(row=4, column=1, sticky="ew", pady=5)
            
        row_idx += 1 # ดัชนีแถวนี้สอดคล้องกับแถวถัดจากเฟรมที่วางข้างกัน


        # --- Command Buttons (ปุ่มคำสั่ง) ---
        button_frame = ttk.Frame(frame)
        button_frame.grid(row=row_idx, column=0, columnspan=2, pady=(15, 10), sticky="ew", padx=10)
        button_frame.grid_columnconfigure((0,1,2,3,4), weight=1) # ปรับเป็น 5 คอลัมน์

        # ใช้สไตล์ปุ่มที่กำหนดเอง
        ttk.Button(button_frame, text="💾 บันทึกการตั้งค่า", command=self._save_settings, style='Green.TButton').grid(row=0, column=0, sticky="ew", padx=5) # เพิ่ม padx
        self.move_button = ttk.Button(button_frame, text="🚚 ย้ายเดี๋ยวนี้", command=lambda: self._run_in_thread("move"), style='Blue.TButton')
        self.move_button.grid(row=0, column=1, sticky="ew", padx=5) # เพิ่ม padx

        self.copy_button = ttk.Button(button_frame, text="📄 คัดลอกเดี๋ยวนี้", command=lambda: self._run_in_thread("copy"), style='Blue.TButton')
        self.copy_button.grid(row=0, column=2, sticky="ew", padx=5) # เพิ่ม padx

        self.delete_button = ttk.Button(button_frame, text="🗑️ ลบเดี๋ยวนี้", command=lambda: self._run_in_thread("delete"), style='Red.TButton')
        self.delete_button.grid(row=0, column=3, sticky="ew", padx=5) # เพิ่ม padx
        ttk.Button(button_frame, text="⛔ ยกเลิกการทำงาน", command=self._cancel_operation, style='Red.TButton').grid(row=0, column=4, sticky="ew", padx=5) # เพิ่ม padx
        
        row_idx += 1

        # --- Log Box (กล่อง Log) ---
        self.log_box = tk.Text(frame, height=15, width=100, state='normal', 
                               bg='#FFFFFF', fg='#4A4A4A', font=('Consolas', 9), 
                               padx=10, pady=10, relief='flat', borderwidth=1, # เพิ่ม padx, pady
                               highlightbackground='#D3D3D3', highlightcolor='#A0A0A0', highlightthickness=1) 
        self.log_box.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=10, sticky="nsew") # ครอบคลุม 2 คอลัมน์
        # เพิ่ม Scrollbar ให้กับ log_box
        log_scrollbar = ttk.Scrollbar(frame, command=self.log_box.yview)
        log_scrollbar.grid(row=row_idx, column=2, sticky='ns', padx=(0, 10)) # ปรับคอลัมน์สำหรับ scrollbar
        self.log_box['yscrollcommand'] = log_scrollbar.set
        # ตั้งค่าเป็น 'disabled' หลังจากสร้างเพื่อให้ผู้ใช้แก้ไขไม่ได้
        self.log_box.config(state='disabled')
        row_idx += 1


        # --- Progress Bar & Status (แถบความคืบหน้าและสถานะ) ---
        self.progress_bar = ttk.Progressbar(frame, length=400, mode='determinate', style='Custom.Horizontal.TProgressbar')
        self.progress_bar.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=(5, 5), sticky="ew") # ครอบคลุม 2 คอลัมน์
        row_idx += 1

        self.progress_label = ttk.Label(frame, text="")
        self.progress_label.grid(row=row_idx, column=0, columnspan=2, sticky="ew", padx=10, pady=(0,5)) # ครอบคลุม 2 คอลัมน์
        row_idx += 1

        self.next_run_label = ttk.Label(frame, text="📅 กำลังคำนวณเวลารันครั้งถัดไป...")
        self.next_run_label.grid(row=row_idx, column=0, columnspan=2, pady=(5, 10), sticky="ew", padx=10) # ครอบคลุม 2 คอลัมน์


    # --- Logging Functions (ฟังก์ชันการบันทึก Log) ---
    def _log(self, message, to_app_log=False, to_gui_log=True, show_popup=False):
        """บันทึกข้อความ Log ไปยัง Console, ไฟล์ และ Log Box ใน GUI พร้อมแสดง Popup แจ้งเตือนข้อผิดพลาด (เงื่อนไขใหม่)"""
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
                self._log(f"❌ ข้อผิดพลาดในการเขียนไฟล์ Log หลัก {LOG_FILE}: {e}", to_app_log=False, to_gui_log=True, show_popup=True) 

        # เขียน Log ลงไฟล์ error_log.txt หากเป็นข้อความ Error
        if show_popup or "❌" in message: # ตรวจสอบคำขอ popup อย่างชัดเจนหรือ emoji ข้อผิดพลาด
            try:
                with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(full_msg + "\n")
            except IOError as e:
                # ข้อผิดพลาดในการเขียน error_log เป็นข้อผิดพลาดสำคัญ ควรแสดงใน GUI
                self._log(f"❌ ข้อผิดพลาดในการเขียนไฟล์ Log ข้อผิดพลาด {ERROR_LOG_FILE}: {e}", to_app_log=False, to_gui_log=True, show_popup=False) # ไม่ต้องแสดง popup อีกครั้ง

        # แสดงใน log_box ของ GUI (ถ้ามีและยังไม่ถูกทำลาย)
        if to_gui_log and hasattr(self, 'log_box') and self.log_box.winfo_exists():
            self.log_box.config(state='normal')
            self.log_box.insert(tk.END, full_msg + "\n")
            self.log_box.see(tk.END) # เลื่อนไปที่บรรทัดสุดท้าย
            self.log_box.config(state='disabled') # ปิดการใช้งานไม่ให้ผู้ใช้แก้ไข

            # --- MODIFICATION: Show error messagebox based on show_popup parameter ---
            if show_popup: # แสดง popup เฉพาะเมื่อถูกร้องขออย่างชัดเจนเท่านั้น
                self.master.after(0, lambda: messagebox.showerror("ข้อผิดพลาด", message))


    def _log_action(self, file_name, action_type, status, src=None, dst=None, current_skipped_count=None, total_initial_files=None):
        """
        บันทึกการกระทำกับไฟล์ลงในไฟล์ Action Log โดยเฉพาะ 
        ข้อความเหล่านี้จะถูกบันทึกใน action_log.txt และ app_log.txt แต่จะไม่แสดงใน GUI Log Box
        """
        time_str = datetime.datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')
        base_file_name = os.path.basename(file_name) # ใช้เฉพาะชื่อไฟล์

        # แมป action_type ภาษาอังกฤษเป็นภาษาไทยสำหรับข้อความ Log
        action_type_thai_map = {
            "MOVE": "ย้าย",
            "COPY": "คัดลอก",
            "DELETE": "ลบ",
            "SKIP": "ข้าม"
        }
        action_type_display = action_type_thai_map.get(action_type.upper(), action_type.upper()) # แปลงเป็นไทย

        msg = f"{time_str} {action_type_display} | {base_file_name} | {status}"

        # เพิ่มข้อมูลเส้นทางสำหรับ Action ต่างๆ
        if action_type.upper() == "DELETE" and src:
            src_dir = os.path.dirname(src)
            msg += f" | จาก: {src_dir}"
        elif src and dst:
            src_dir = os.path.dirname(src)
            dst_dir = os.path.dirname(dst)
            msg += f" | จาก: {src_dir} ไปยัง: {dst_dir}"
        # สำหรับ SKIP, src คือไฟล์ที่ถูกข้าม
        elif action_type.upper() == "SKIP" and src:
            src_dir = os.path.dirname(src)
            msg += f" | เส้นทางไฟล์: {src_dir}"
            if current_skipped_count is not None and total_initial_files is not None:
                msg += f" | ข้ามไป {current_skipped_count:,}/{total_initial_files:,} ไฟล์"

        # บันทึกเข้าไฟล์ ACTION_LOG_FILE (ซึ่งตอนนี้เป็น .txt) เสมอ
        try:
            with open(ACTION_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except IOError as e:
            # ข้อผิดพลาดในการเขียน action_log เป็นข้อผิดพลาดสำคัญ ควรแสดงใน GUI
            self._log(f"❌ ข้อผิดพลาดในการเขียนไฟล์ Log การทำงาน {ACTION_LOG_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 

        # บันทึกเข้า app_log.txt (ไม่แสดงใน GUI Log Box)
        # CHANGED: to_app_log=False เพื่อป้องกันการซ้ำกันใน app_log.txt เนื่องจากตอนนี้มีไว้สำหรับ ACTION_LOG_FILE โดยเฉพาะ
        # ตรวจสอบให้แน่ใจว่า show_popup=False สำหรับ action logs เว้นแต่จำเป็นอย่างชัดเจน
        self._log(msg, to_app_log=False, to_gui_log=False, show_popup=False) 

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
            self._log(f"ข้อผิดพลาดในการเขียนขั้นตอนการประมวลผลไปยังไฟล์ Log การทำงาน {ACTION_LOG_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True)

        # บันทึกเข้า app_log.txt (ไม่แสดงใน GUI Log Box)
        # ตรวจสอบให้แน่ใจว่า show_popup=False สำหรับ process step logs
        self._log(message, to_app_log=False, to_gui_log=False, show_popup=False)


    # --- Settings Management Functions (ฟังก์ชันจัดการการตั้งค่า) ---
    def _load_settings(self):
        """โหลดการตั้งค่าจากไฟล์ JSON"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                    return settings
            except json.JSONDecodeError as e:
                self._log(f"❌ ข้อผิดพลาดในการอ่านไฟล์ตั้งค่า {CONFIG_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 
                return {}                                                                                    
            except IOError as e:
                self._log(f"❌ ข้อผิดพลาดในการอ่านไฟล์ตั้งค่า {CONFIG_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 
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
        }
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4) # ใช้ indent=4 เพื่อให้อ่านง่าย
            self._log("✅ บันทึกการตั้งค่าสำเร็จ", to_app_log=True, to_gui_log=True, show_popup=False) 
                                                                                             
        except IOError as e:
            self._log(f"❌ ข้อผิดพลาดในการบันทึกไฟล์ตั้งค่า {CONFIG_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 

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
        except (IOError, OSError) as e:
            # Re-raise เป็นข้อผิดพลาดวิกฤติเพื่อหยุดการทำงานทั้งหมด
            raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์เมื่อตรวจสอบพื้นที่ว่างสำหรับ {path}: {e}")
        except Exception as e:
            # ดักจับข้อผิดพลาดที่ไม่คาดคิดอื่น ๆ
            raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดเมื่อตรวจสอบพื้นที่ว่างสำหรับ {path}: {e}")

    def _cancel_operation(self):
        """ตั้งค่าสถานะการยกเลิกการทำงาน"""
        self.operation_cancelled = True
        # บันทึกข้อความเฉพาะใน GUI เมื่อถูกยกเลิก
        self._log("⛔ ผู้ใช้ยกเลิกการทำงาน รีเซ็ตสถานะแล้ว", to_app_log=True, to_gui_log=True, show_popup=False) 
        self._set_buttons_state("normal") # เปิดใช้งานปุ่มทันทีเมื่อยกเลิก
        # เมื่อถูกยกเลิก ให้รีเซ็ต is_task_running ทันทีเพื่ออนุญาตให้มีการรันใหม่
        self.is_task_running = False 
        self._update_next_run_label() # อัปเดตป้ายหลังจากยกเลิกเพื่อแสดงข้อความคงที่

    def _set_buttons_state(self, state):
        """ตั้งค่าสถานะของปุ่ม Move, Copy, Delete"""
        self.move_button.config(state=state)
        self.copy_button.config(state=state)
        self.delete_button.config(state=state)

    def _run_in_thread(self, op):
        """รันการทำงาน (Move/Copy/Delete) ใน Thread แยกต่างหาก เพื่อไม่ให้ GUI ค้าง"""
        if self.is_task_running: # ตรวจสอบว่ามี Task กำลังรันอยู่หรือไม่
            self._log("⚠️ Task กำลังทำงานอยู่ ไม่รับคำขอใหม่", to_app_log=True, to_gui_log=True, show_popup=False)
            return

        emoji_map = {
            "move": "🔀",
            "copy": "📄",
            "delete": "🗑️"
        }
        emoji = emoji_map.get(op, "ℹ️") # รับอีโมจิตามการทำงาน, ค่าเริ่มต้นเป็นอีโมจิข้อมูล
        
        self._log(f" 🔁 กำลังเริ่มการทำงาน {op}...", to_app_log=True, to_gui_log=True, show_popup=False) 
        self.operation_cancelled = False # รีเซ็ตสถานะการยกเลิก
        self._set_buttons_state("disabled") # ปิดการใช้งานปุ่ม
        self.progress_bar["value"] = 0 # รีเซ็ตแถบความคืบหน้า
        self.progress_label.config(text="") # รีเซ็ตข้อความสถานะ
        
        self.is_task_running = True # ตั้งค่าแฟล็กว่ามีงานกำลังรัน
        self.loading_dots_count = 0 # รีเซ็ตจำนวนจุดเมื่อเริ่มงานใหม่
        self._update_next_run_label() # เริ่ม Animation ทันที

        # อัปเดตวันที่รันล่าสุดทันทีที่งานเริ่ม (ตำแหน่งใหม่)
        self._set_last_run_date(datetime.datetime.now().strftime("%Y-%m-%d"))
        
        # เริ่ม Thread ใหม่สำหรับฟังก์ชัน _safe_run
        threading.Thread(target=lambda: self._safe_run(op), daemon=True).start()

    def _fail_operation_ui_update(self, error_msg="การทำงานล้มเหลวอย่างไม่คาดคิด"):
        """อัปเดต GUI เพื่อแสดงการทำงานที่ล้มเหลวและรีเซ็ตสถานะ"""
        self.progress_bar["value"] = 0
        self.progress_label.config(text=f"❌ ข้อผิดพลาด: {error_msg}")
        self._set_buttons_state("normal")
        self.is_task_running = False
        # รีเซ็ตข้อผิดพลาดการข้ามไฟล์ติดต่อกันเมื่อการทำงานล้มเหลวหรือถูกยกเลิก
        self.consecutive_skip_errors = 0 
        self._update_next_run_label() # อัปเดตป้ายบอกเวลารันครั้งถัดไป, แสดงสถานะ "idle" ตอนนี้

    def _safe_run(self, op):
        """เรียกใช้ฟังก์ชันการทำงานหลักและจัดการกับข้อผิดพลาด/สถานะการทำงาน"""
        try:
            self._move_or_copy_files(op)
            # หาก _move_or_copy_files เสร็จสิ้นโดยไม่เกิด OperationCriticalError
            # และ operation_cancelled ไม่ได้ถูกตั้งค่า (เช่น โดยผู้ใช้ยกเลิก)
            # ข้อความแสดงความสำเร็จโดยละเอียดจะถูกบันทึกภายใน _move_or_copy_files
            pass # ไม่มี Log เพิ่มเติมที่นี่สำหรับกรณีสำเร็จ
        except OperationCriticalError as e:
            # สิ่งนี้ดักจับข้อผิดพลาดวิกฤติที่เกิดจาก _move_or_copy_files หรือ _check_free_space_gb
            error_msg = str(e) # รับข้อความจากข้อยกเว้นที่กำหนดเอง
            self._log(f"❌ ข้อผิดพลาด: {error_msg}", to_app_log=True, to_gui_log=True, show_popup=True) # แสดง popup สำหรับข้อผิดพลาดวิกฤติ
            self._fail_operation_ui_update(error_msg)
        except Exception as e:
            # ดักจับข้อผิดพลาดอื่น ๆ ที่ไม่ได้จัดการ
            error_msg = f"ข้อผิดพลาดที่ไม่คาดคิดเกิดขึ้นระหว่างการทำงาน: {e}"
            self._log(f"❌ ข้อผิดพลาดในการประมวลผล: {error_msg}", to_app_log=True, to_gui_log=True, show_popup=True) # แสดง popup สำหรับข้อผิดพลาดที่ไม่คาดคิด
            self._fail_operation_ui_update(error_msg)
        finally:
            if not self.operation_cancelled:
                self._set_buttons_state("normal")
                self.is_task_running = False
                self.consecutive_skip_errors = 0 # ตรวจสอบให้แน่ใจว่ามีการรีเซ็ตเมื่อเสร็จสมบูรณ์ตามปกติ
            self._update_next_run_label() # อัปเดตป้ายเสมอเมื่อสิ้นสุดงาน, แสดงสถานะ idle

    def _move_or_copy_files(self, operation="move"):
        """
        ดำเนินการย้าย, คัดลอก, หรือลบไฟล์ตามการตั้งค่า
        ตอนนี้ใช้ shutil เท่านั้น
        """
        self.operation_cancelled = False # รีเซ็ตสถานะการยกเลิกสำหรับ Task ใหม่
        self.consecutive_skip_errors = 0 # รีเซ็ตตัวนับการข้ามเมื่อเริ่มการทำงานใหม่
        config = self._load_settings()
        src = config.get("source", "")
        dst = config.get("dest", "")
        file_type = config.get("file_type", "All")
        min_free_space = float(config.get("min_free_space_gb", 5.0))
        filter_old = bool(config.get("filter_old", False))
        months_old = int(config.get("months_old", 3))

        self._log(f"กำลังเริ่มการทำงานไฟล์ {operation.capitalize()} จาก '{src}' ไปยัง '{dst}' (ประเภทไฟล์: {file_type})", to_app_log=True, to_gui_log=True, show_popup=False) 

        # บันทึกตัวกรองตามอายุและวันที่ตัดยอดหากเปิดใช้งาน
        if filter_old:
            cutoff_time = datetime.datetime.now() - relativedelta(months=months_old)
            self._log(f"📅 กำลังโอนย้ายไฟล์ที่เก่ากว่า {months_old} เดือน วันที่ตัดยอด: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}", to_app_log=True, to_gui_log=True, show_popup=False)

        # --- Initial path validation and file gathering (จุดสำคัญสำหรับการตัดการเชื่อมต่อ SSD) ---
        all_files_in_src = []
        total_files_in_src_initial_count = 0
        total_size_to_process_bytes = 0

        try:
            if not os.path.exists(src):
                raise OperationCriticalError(f"ไม่พบโฟลเดอร์ต้นทาง: {src}")

            if operation != "delete" and not os.path.exists(dst):
                raise OperationCriticalError(f"ไม่พบโฟลเดอร์ปลายทาง: {dst}")

            # ตรวจสอบพื้นที่ว่างบนปลายทางสำหรับการย้าย/คัดลอก
            if operation != "delete":
                free_space, total_space = self._check_free_space_gb(dst) # สิ่งนี้อาจทำให้เกิด OperationCriticalError
                if free_space < min_free_space:
                    raise OperationCriticalError(f"พื้นที่ว่างบนปลายทาง ({free_space:.2f} GB) ต่ำกว่าที่กำหนดขั้นต่ำ ({min_free_space} GB) หยุดการทำงาน")
            
            # เติม all_files_in_src - ครอบคลุมด้วย try-except สำหรับข้อผิดพลาดของดิสก์
            try:
                # นี่ควรเป็นการตรวจสอบการเข้าถึงแหล่งที่มาที่แข็งแกร่งเป็นอันดับแรก
                all_files_in_src = [f for f in os.listdir(src) if os.path.isfile(os.path.join(src, f))]
            except (IOError, OSError) as e:
                # สิ่งนี้ดักจับข้อผิดพลาดการเข้าถึงดิสก์หลักเมื่อแสดงรายการไฟล์ครั้งแรก
                raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์เมื่อแสดงรายการไฟล์ในโฟลเดอร์ต้นทาง '{src}': {e} หยุดการทำงาน")

            total_files_in_src_initial_count = len(all_files_in_src)

            if total_files_in_src_initial_count == 0:
                self._log(f"ℹ️ ไม่พบไฟล์ในโฟลเดอร์ต้นทาง สิ้นสุดการทำงานแล้ว", to_app_log=True, to_gui_log=True, show_popup=False)
                self.progress_bar["value"] = 100
                self.progress_label.config(text=f"✅ เสร็จสิ้น ไม่พบไฟล์")
                return # ออกจากลูปก่อนหากไม่มีไฟล์ให้ประมวลผล

            eligible_files = []
            # นี่จะนับไฟล์ที่ถูกข้ามโดยตัวกรองหรือข้อผิดพลาดเริ่มต้นระหว่าง *การสแกนเริ่มต้น*
            skipped_initial_shutil = 0

            # ลูปนี้ใช้สำหรับการกรองเริ่มต้นและการเติม 'eligible_files'
            for f in all_files_in_src:
                file_path = os.path.join(src, f)
                
                # ตรวจสอบการเข้าถึงสำหรับไฟล์แต่ละไฟล์ระหว่างการสแกนคุณสมบัติเริ่มต้น
                try:
                    # หาก os.path.isfile ล้มเหลวที่นี่ แสดงว่าเป็นข้อผิดพลาดของดิสก์ที่สำคัญ
                    if not os.path.isfile(file_path):
                        # MODIFIED: Raise critical error immediately if file is missing during initial scan
                        # หากคืนค่าเป็น False โดยไม่มีข้อผิดพลาด หมายความว่าไฟล์ถูกลบจริง ๆ
                        # Increment skipped_initial_shutil ไม่จำเป็นต้องมีตัวนับการข้ามไฟล์ติดต่อกันอีกต่อไปในกรณีนี้
                        raise OperationCriticalError(f"ไฟล์ '{f}' หายไปจากต้นทางระหว่างการสแกนเริ่มต้น หยุดการทำงาน")
                except (IOError, OSError) as e:
                    # หาก os.path.isfile *ส่ง* ข้อยกเว้น แสดงว่าเป็นปัญหาการเข้าถึงดิสก์
                    raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์เมื่อตรวจสอบการมีอยู่ของ '{f}' ระหว่างการสแกนเริ่มต้น: {e} หยุดการทำงาน")
                except Exception as e:
                    raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดเมื่อตรวจสอบการมีอยู่ของ '{f}' ระหว่างการสแกนเริ่มต้น: {e} หยุดการทำงาน")

                # กรองตามประเภทไฟล์
                if file_type == "Excel" and not f.lower().endswith((".xls", ".xlsx", ".xlsm", ".csv")):
                    skipped_initial_shutil += 1
                    self._log_action(f, "skip", "ประเภทไฟล์ไม่ถูกต้อง", src=file_path) # สถานะแปลแล้ว
                    continue
                
                # กรองตามอายุและรับขนาด ตรวจสอบให้แน่ใจว่าข้อผิดพลาดของดิสก์ที่นี่ก็ทำให้เกิดข้อผิดพลาดที่สำคัญด้วย
                try:
                    # การดำเนินการเหล่านี้เข้าถึงดิสก์ ดังนั้นจึงต้องจัดการอย่างแข็งขัน
                    file_modified_time = os.path.getmtime(file_path) # ตรวจสอบเวลาแก้ไข
                    current_file_size = os.path.getsize(file_path)   # รับขนาด
                except (IOError, OSError) as e:
                    raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์ (getmtime/getsize) สำหรับ '{f}' ระหว่างการสแกนเริ่มต้น: {e} หยุดการทำงาน")
                except Exception as e:
                    raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิด (getmtime/getsize) สำหรับ '{f}': {e} หยุดการทำงาน")

                if filter_old:
                    cutoff_time = datetime.datetime.now() - relativedelta(months=months_old)
                    modified_time = datetime.datetime.fromtimestamp(file_modified_time)
                    if modified_time > cutoff_time: 
                        skipped_initial_shutil += 1
                        self._log_action(f, "skip", f"ยังไม่เก่าพอ|แก้ไขเมื่อ:{modified_time.strftime('%Y-%m-%d %H:%M:%S')}", src=file_path) # สถานะแปลแล้ว
                        continue
                
                # เพิ่มลงในไฟล์ที่มีสิทธิ์และขนาดรวม
                eligible_files.append(f)
                total_size_to_process_bytes += current_file_size # ใช้ขนาดที่ดึงมาข้างต้น

            total_files_to_process = len(eligible_files)
            if total_files_to_process == 0 and skipped_initial_shutil == total_files_in_src_initial_count:
                self._log(f"ℹ️ ไฟล์ทั้งหมด {total_files_in_src_initial_count:,} ไฟล์ถูกข้ามด้วยตัวกรอง หรือพบข้อผิดพลาดระหว่างการสแกนเริ่มต้น ไม่พบไฟล์ที่เข้าเกณฑ์สำหรับการ {operation}", to_app_log=True, to_gui_log=True, show_popup=False) 
                self.progress_bar["value"] = 100
                self.progress_label.config(text=f"✅ เสร็จสิ้น ไฟล์ที่เข้าเกณฑ์ทั้งหมดถูกข้าม")
                return 
            elif total_files_to_process == 0: # กรณีนี้ครอบคลุมเมื่อไฟล์ทั้งหมดถูกข้าม แต่ skipped_initial_shutil อาจเป็น 0 ด้วย (เช่น ไม่มีไฟล์ในโฟลเดอร์)
                 self._log(f"ℹ️ ไม่พบไฟล์ที่เข้าเกณฑ์หลังจากใช้ตัวกรอง สิ้นสุดการทำงานแล้ว", to_app_log=True, to_gui_log=True, show_popup=False)
                 self.progress_bar["value"] = 100
                 self.progress_label.config(text=f"✅ เสร็จสิ้น ไม่มีไฟล์ให้ประมวลผล")
                 return

            # จัดเรียงไฟล์ที่มีสิทธิ์ตามเวลาการแก้ไข (เก่าที่สุดก่อน)
            eligible_files_with_mod_time = []
            for f_name in eligible_files:
                file_path = os.path.join(src, f_name)
                try:
                    eligible_files_with_mod_time.append((os.path.getmtime(file_path), f_name))
                except (IOError, OSError) as e:
                    # หากเกิดเหตุการณ์นี้ขึ้นที่นี่ ถือเป็นข้อผิดพลาดที่สำคัญเนื่องจากไฟล์เคยถูกพิจารณาว่ามีสิทธิ์
                    raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์เมื่อดึงวันที่แก้ไขสำหรับไฟล์ที่เข้าเกณฑ์ '{f_name}': {e} หยุดการทำงาน")
                except Exception as e:
                    raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดเมื่อดึงวันที่แก้ไขสำหรับไฟล์ที่เข้าเกณฑ์ '{f_name}': {e} หยุดการทำงาน")
            
            eligible_files_with_mod_time.sort(key=lambda x: x[0])
            eligible_files = [f_name for mod_time, f_name in eligible_files_with_mod_time] 

            # บันทึกสรุปไฟล์ที่มีสิทธิ์และรายละเอียดของไฟล์แรกที่มีสิทธิ์
            self._log(f"📄 พบ {total_files_to_process:,} ไฟล์ที่เข้าเกณฑ์สำหรับการประมวลผลหลังจากใช้ตัวกรอง", to_app_log=True, to_gui_log=True, show_popup=False)
            if filter_old and eligible_files:
                first_eligible_file_name = eligible_files[0]
                first_eligible_file_path = os.path.join(src, first_eligible_file_name)
                try:
                    first_file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(first_eligible_file_path))
                    self._log(f"เริ่มย้ายจากไฟล์: {first_eligible_file_name} (แก้ไขล่าสุด: {first_file_mod_time.strftime('%Y-%m-%d %H:%M:%S')})", to_app_log=True, to_gui_log=True, show_popup=False)
                except (IOError, OSError) as e:
                    self._log(f"⚠️ คำเตือน: ไม่สามารถดึงวันที่แก้ไขสำหรับไฟล์แรกที่เข้าเกณฑ์ '{first_eligible_file_name}': {e}", to_app_log=True, to_gui_log=True, show_popup=False)


        except OperationCriticalError as e:
            # Re-raise เพื่อให้ _safe_run ดักจับและหยุดทุกอย่าง
            raise e
        except Exception as e:
            # ดักจับข้อผิดพลาดอื่น ๆ ที่ไม่คาดคิดระหว่างการตั้งค่าเริ่มต้น
            raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดเกิดขึ้นระหว่างการตั้งค่าการประมวลผลไฟล์เริ่มต้น: {e}")

        # --- สิ้นสุดการตรวจสอบเส้นทางเริ่มต้นและการรวบรวมไฟล์ ---

        # เปลี่ยน: total_size_to_process_bytes / (1024**3) และ "GB"
        self._log(f"กำลังประมวลผล {total_files_to_process:,} ไฟล์ที่เข้าเกณฑ์ ขนาดรวม {total_size_to_process_bytes / (1024**3):.2f} GB โดยใช้ shutil...", to_app_log=True, to_gui_log=True, show_popup=False) 
        processed_count = 0
        self.total_bytes_processed = 0
        self.start_time = time.time()

        # --- ลูปการประมวลผลไฟล์ (สำหรับ shutil) ---
        for idx, f in enumerate(eligible_files, start=1):
            if self.operation_cancelled:
                self._log("⚠️ ผู้ใช้ยกเลิกการทำงาน กำลังหยุดการประมวลผลไฟล์", to_app_log=True, to_gui_log=True, show_popup=False) 
                break # ออกจากลูปทันที

            source_path = os.path.join(src, f)
            target_path = os.path.join(dst, f) if operation != "delete" else None
            success = False
            file_size = 0 

            # ตรวจสอบการเข้าถึงไฟล์อย่างชัดเจนก่อนที่จะประมวลผล
            try:
                # หาก os.path.isfile คืนค่า False โดยไม่มีข้อยกเว้น แสดงว่าไฟล์หายไปแล้ว
                # หากเกิดข้อยกเว้น แสดงว่าเป็นปัญหาการเข้าถึงดิสก์ที่สำคัญ
                if not os.path.isfile(source_path):
                    # MODIFIED: Raise critical error immediately if file is missing during processing loop
                    raise OperationCriticalError(f"ไฟล์ '{f}' หายไปจากต้นทางระหว่างการทำงาน หยุดการทำงาน")
                else:
                    # หากพบไฟล์ (ไม่หายไป) ให้รีเซ็ตตัวนับข้อผิดพลาดต่อเนื่อง
                    self.consecutive_skip_errors = 0

            except (IOError, OSError) as e:
                # หาก os.path.isfile *ส่ง* ข้อยกเว้น แสดงว่าเป็นข้อผิดพลาดของดิสก์ที่สำคัญ ให้หยุดทันที
                raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์เมื่อตรวจสอบไฟล์ '{f}' ก่อนประมวลผล: {e} หยุดการทำงาน")
            except Exception as e:
                # ดักจับข้อผิดพลาดอื่น ๆ ที่ไม่คาดคิด
                raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดเมื่อตรวจสอบไฟล์ '{f}' ก่อนประมวลผล: {e} หยุดการทำงาน")

            try:
                file_start_time = time.time()
                file_size = os.path.getsize(source_path)

                if operation in ("move", "copy") and os.path.exists(target_path):
                    base, ext = os.path.splitext(f)
                    count = 1
                    while os.path.exists(target_path):
                        target_path = os.path.join(dst, f"{base}_copy{count}{ext}")
                        count += 1
                    self._log_process_step(f"ไฟล์ '{f}' มีอยู่แล้วในปลายทาง กำลังเปลี่ยนชื่อเป็น '{os.path.basename(target_path)}'")

                if operation == "move":
                    self._log_process_step(f"[ขั้นตอนการย้าย 1/2] กำลังพยายามคัดลอก '{f}' ไปยัง '{target_path}'")
                    shutil.copy2(source_path, target_path)

                    if os.path.exists(target_path) and os.path.getsize(source_path) == os.path.getsize(target_path):
                        self._log_process_step(f"[ขั้นตอนการย้าย 1/2] คัดลอก '{f}' สำเร็จ กำลังตรวจสอบความถูกต้อง")
                        if not self.operation_cancelled:
                            try:
                                self._log_process_step(f"[ขั้นตอนการย้าย 2/2] กำลังพยายามลบไฟล์ต้นฉบับ '{source_path}'")
                                os.remove(source_path)
                                self._log_action(f, "ลบ", "สำเร็จ", src=source_path) # สถานะแปลแล้ว
                                success = True
                                self._log_action(f, "ย้าย", "สำเร็จ", src=source_path, dst=target_path) # สถานะแปลแล้ว
                                self._log_process_step(f"[การย้ายเสร็จสมบูรณ์] '{f}' ย้ายสำเร็จแล้ว")
                            except (IOError, OSError) as delete_e:
                                # นี่คือข้อผิดพลาดที่สำคัญในขั้นตอนการลบของการดำเนินการย้าย
                                raise OperationCriticalError(f"ข้อผิดพลาดในการเข้าถึงดิสก์ระหว่างการลบไฟล์ต้นฉบับ '{source_path}': {delete_e} หยุดการทำงาน")
                            except Exception as delete_e:
                                # ข้อผิดพลาดอื่น ๆ ที่ไม่คาดคิดในขั้นตอนการลบของการดำเนินการย้าย
                                raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดระหว่างการลบไฟล์ต้นฉบับ '{source_path}': {delete_e} หยุดการทำงาน")
                        else:
                            self._log_action(f, "ย้าย", "ยกเลิกหลังคัดลอก", src=source_path, dst=target_path) # สถานะแปลแล้ว
                            self._log(f"⚠️ [ยกเลิกการย้าย] คัดลอกสำเร็จ แต่ข้ามการลบต้นฉบับเนื่องจากถูกยกเลิก: {source_path}", to_app_log=True, to_gui_log=True, show_popup=False)
                            success = False
                    else:
                        self._log_action(f, "ย้าย", "ขนาดไม่ตรงกัน", src=source_path, dst=target_path) # สถานะแปลแล้ว
                        self._log(f"❌ ข้อผิดพลาด: [ย้ายไม่สำเร็จ] ขนาดไฟล์ไม่ตรงกัน หรือไม่พบปลายทางหลังการคัดลอก ข้ามการลบ: {source_path}", to_app_log=True, to_gui_log=True, show_popup=True) # นี่คือข้อผิดพลาดในการดำเนินงาน ควรแสดง popup
                        success = False

                elif operation == "copy":
                    shutil.copy2(source_path, target_path)
                    self._log_action(f, "คัดลอก", "สำเร็จ", src=source_path, dst=target_path) # สถานะแปลแล้ว
                    success = True

                elif operation == "delete":
                    os.remove(source_path)
                    self._log_action(f, "ลบ", "สำเร็จ", src=source_path) # สถานะแปลแล้ว
                    success = True

                if success:
                    processed_count += 1
                    self.total_bytes_processed += file_size
                    self.consecutive_skip_errors = 0 # รีเซ็ตข้อผิดพลาดการข้ามไฟล์ติดต่อกันเมื่อประมวลผลสำเร็จ

                file_end_time = time.time()
                elapsed_file = file_end_time - file_start_time      
                total_elapsed_time = file_end_time - self.start_time 

                self._update_progress_gui(idx, total_files_to_process, operation, elapsed_file, total_elapsed_time,
                                          file_size, processed_count, skipped_initial_shutil, total_files_in_src_initial_count, total_size_to_process_bytes)

            except (IOError, OSError) as e:
                # บล็อกนี้จัดการข้อผิดพลาดที่เกี่ยวข้องกับดิสก์โดยเฉพาะ (เช่น ไดรฟ์ถูกถอดออก)
                # Re-raise เป็นข้อผิดพลาดที่สำคัญเพื่อหยุดการทำงานทั้งหมดทันที
                raise OperationCriticalError(f"ดิสก์หลุดหรือข้อผิดพลาดของระบบไฟล์เกิดขึ้นขณะประมวลผล {source_path}: {e} กำลังหยุดการทำงาน")
            except Exception as e:
                # บล็อกนี้จัดการข้อผิดพลาดอื่น ๆ ที่ไม่คาดคิดระหว่างการประมวลผลไฟล์เดียว
                self._log(f"❌ ข้อผิดพลาดในการประมวลผล {source_path}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) # นี่คือข้อผิดพลาดในการดำเนินงาน ควรแสดง popup
                self._log_action(f, operation, f"ข้อผิดพลาด: {e}", src=source_path, dst=target_path) # สถานะแปลแล้ว
                # เนื่องจากเราต้องการให้หยุดสำหรับข้อผิดพลาดประเภทนี้ เราจะ re-raise เป็น critical
                raise OperationCriticalError(f"ข้อผิดพลาดที่ไม่คาดคิดในการประมวลผล {source_path}: {e} กำลังหยุดการทำงาน")

        # --- ข้อความสถานะสุดท้ายหลังจากลูปเสร็จสมบูรณ์หรือหยุดชะงัก ---
        # หากการทำงานถูกยกเลิกเนื่องจากข้อผิดพลาดที่สำคัญ _safe_run จะจัดการข้อความสุดท้ายและการอัปเดต UI
        # มิฉะนั้น หากมาถึงที่นี่ แสดงว่าเสร็จสมบูรณ์ตามปกติหรือถูกผู้ใช้ยกเลิก
        if not self.operation_cancelled:
            # ตรวจสอบให้แน่ใจว่า src ยังสามารถเข้าถึงได้ก่อนที่จะพยายามแสดงไฟล์ที่เหลือ
            remaining_files_in_source_folder = "ไม่พร้อมใช้งาน (ไม่สามารถเข้าถึงต้นทางได้)"
            try:
                if os.path.exists(src):
                    remaining_files_in_source_folder = len([f for f in os.listdir(src) if os.path.isfile(os.path.join(src, f))])
            except (IOError, OSError) as e:
                self._log(f"⚠️ คำเตือน: ไม่สามารถระบุไฟล์ที่เหลือในต้นทาง '{src}' ได้ เนื่องจากข้อผิดพลาดในการเข้าถึงดิสก์: {e}", to_app_log=True, to_gui_log=False, show_popup=False)
            except Exception as e:
                 self._log(f"⚠️ คำเตือน: ข้อผิดพลาดที่ไม่คาดคิดในการระบุไฟล์ที่เหลือในต้นทาง '{src}' ได้: {e}", to_app_log=True, to_gui_log=False, show_popup=False)

            final_msg_detail = (f"ประมวลผล {processed_count:,} ไฟล์ " 
                                f"ข้ามไป {skipped_initial_shutil:,} ไฟล์ (จากทั้งหมด {total_files_in_src_initial_count:,} ไฟล์เริ่มต้น) "
                                f"เหลือในต้นทาง: {remaining_files_in_source_folder} ไฟล์")
            self._log(f"✅ การทำงานเสร็จสิ้น {final_msg_detail}", to_app_log=True, to_gui_log=True, show_popup=False) # ไม่มี popup สำหรับข้อความสำเร็จสุดท้าย
            self.progress_bar["value"] = 100 # ตั้งค่าเป็น 100% เมื่อเสร็จสิ้น
            self.progress_label.config(text=f"✅ เสร็จสิ้น")


    def _update_progress_gui(self, current_idx, total_eligible_files, operation, elapsed_file, total_elapsed_time,
                             current_file_size, processed_count, skipped_total_count, total_initial_files_in_src, total_size_to_process_bytes):
        """อัปเดตแถบความคืบหน้าและข้อความสถานะใน GUI"""
        if total_eligible_files == 0:
            progress = 100
        else:
            # ความคืบหน้าพื้นฐานตามจำนวนที่ประมวลผลแล้ว ไม่ใช่ current_idx
            progress = int((processed_count / total_eligible_files) * 100) 

        # คำนวณความเร็ว (ไฟล์ต่อนาที) และเวลาที่เหลือ
        files_per_minute = (processed_count / total_elapsed_time * 60) if total_elapsed_time > 0 else 0
        
        remaining_files = total_eligible_files - processed_count
        
        est_time_left_seconds = 0
        if files_per_minute > 0:
            est_time_left_seconds = (remaining_files / files_per_minute) * 60 # แปลงนาทีเป็นวินาที

        hours, rem = divmod(est_time_left_seconds, 3600)
        minutes, seconds = divmod(rem, 60)

        # แปลงไบต์เป็นรูปแบบที่อ่านง่ายขึ้น (MB, GB)
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
        operation_portion = f"📦 {operation.upper()} {processed_count:,}/{total_eligible_files:,} ไฟล์ ({processed_size_formatted}/{total_size_formatted})"
        # ปรับปรุงข้อความ ETA ให้แสดง files_per_minute ชัดเจน
        eta_portion = f"⏳ ประมาณการเวลาที่เหลือ: {int(hours)} ชม. {int(minutes)} น. {int(seconds)} ว. | ความเร็ว: {files_per_minute:,.2f} ไฟล์/นาที"
        
        # แสดงไฟล์ที่ถูกข้ามจากไฟล์เริ่มต้นทั้งหมด (ไฟล์ทั้งหมดในแหล่งที่มา ก่อนตัวกรองคุณสมบัติ)
        skipped_portion = f"ข้ามไป: {skipped_total_count:,}/{total_initial_files_in_src:,} ไฟล์"

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
                self._log(f"❌ ข้อผิดพลาดในการอ่านไฟล์วันที่รันล่าสุด {LAST_RUN_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 
                return ""
            except IOError as e:
                self._log(f"❌ ข้อผิดพลาดในการอ่านไฟล์วันที่รันล่าสุด {LAST_RUN_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 
                return ""
        return ""

    def _set_last_run_date(self, date_str):
        """บันทึกวันที่รัน Task ปัจจุบันลงในไฟล์ JSON"""
        try:
            with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
                json.dump({"last_run": date_str}, f, indent=4)
        except IOError as e:
            self._log(f"❌ ข้อผิดพลาดในการบันทึกไฟล์วันที่รันล่าสุด {LAST_RUN_FILE}: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 

    def _get_valid_datetime(self, year, month, day, time_obj):
        """
        Helper เพื่อสร้างอ็อบเจกต์ datetime จัดการวันเกินจำนวนวันของเดือน
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

        config = self._load_settings() 
        try:
            auto_day = int(config.get("auto_day", 1))
            auto_time_str = config.get("auto_time", "00:01")
            
            auto_interval = int(config.get("auto_interval", "1"))
            if auto_interval < 0:
                self._log(f"⚠️ 'ทำซ้ำทุก N เดือน' ถูกตั้งค่าเป็น {auto_interval} ซึ่งไม่ถูกต้อง ใช้ 1 เดือนในการคำนวณ", to_app_log=True, to_gui_log=True, show_popup=False) 
                auto_interval = 1 

            target_hour, target_minute = map(int, auto_time_str.split(":"))
            configured_auto_time_obj = datetime.time(target_hour, target_minute)

        except ValueError as e:
            self._log(f"❌ ข้อผิดพลาด: การตั้งค่าการทำงานอัตโนมัติไม่ถูกต้อง (วัน/เวลา/ช่วง): {e} โปรดตรวจสอบการตั้งค่า", to_app_log=True, to_gui_log=True, show_popup=True) 
            return False, datetime.datetime.now(), "❌ ข้อผิดพลาด: การตั้งค่ากำหนดการไม่ถูกต้อง"

        last_run_str = self._get_last_run_date()
        
        last_run_dt_from_file = None 
        if last_run_str:
            try:
                last_run_dt_from_file = datetime.datetime.strptime(last_run_str, "%Y-%m-%d").replace(
                    hour=configured_auto_time_obj.hour, minute=configured_auto_time_obj.minute, second=0, microsecond=0
                )
            except ValueError:
                self._log(f"❌ ข้อผิดพลาด: รูปแบบวันที่รันล่าสุดใน {LAST_RUN_FILE} ไม่ถูกต้อง: {last_run_str} กำลังละเว้นวันที่รันล่าสุดสำหรับการคำนวณ", to_app_log=True, to_gui_log=True, show_popup=True) 
        
        effective_last_run_dt = last_run_dt_from_file if last_run_dt_from_file else datetime.datetime(1900, 1, 1, 0, 0)

        run_now = False
        next_scheduled_run_display = None
        
        # --- การจัดการพิเศษสำหรับ auto_interval = 0 (รันทันทีสำหรับการทดสอบ/รันครั้งเดียวรายวัน) ---
        if auto_interval == 0:
            configured_time_today = now.replace(hour=configured_auto_time_obj.hour, minute=configured_auto_time_obj.minute, second=0, microsecond=0)
            
            if (effective_last_run_dt.date() < now.date() or effective_last_run_dt.year == 1900) and now >= configured_time_today:
                run_now = True
                next_scheduled_run_display = configured_time_today + datetime.timedelta(days=1)
                
            elif now < configured_time_today:
                run_now = False
                next_scheduled_run_display = configured_time_today 
            else: 
                run_now = False
                next_scheduled_run_display = configured_time_today + datetime.timedelta(days=1) 
            
            last_run_info = f"รันล่าสุด: {last_run_str if last_run_str else 'ไม่เคย'}"
            full_next_run_msg = f"{last_run_info} | กำหนดการถัดไป: {next_scheduled_run_display.strftime('%Y-%m-%d %H:%M')}"
            
            return run_now, next_scheduled_run_display, full_next_run_msg
        
        # --- การจัดกำหนดการรายเดือน (auto_interval > 0) ---
        
        current_candidate_dt = self._get_valid_datetime(now.year, now.month, auto_day, configured_auto_time_obj)
        
        while current_candidate_dt <= effective_last_run_dt:
            current_candidate_dt += relativedelta(months=auto_interval)
            current_candidate_dt = self._get_valid_datetime(
                current_candidate_dt.year,
                current_candidate_dt.month,
                auto_day,
                configured_auto_time_obj
            )
        
        run_now = (now >= current_candidate_dt)
        
        if run_now:
            next_scheduled_run_display = current_candidate_dt + relativedelta(months=auto_interval)
            next_scheduled_run_display = self._get_valid_datetime(
                next_scheduled_run_display.year,
                next_scheduled_run_display.month,
                auto_day,
                configured_auto_time_obj
            )
        else:
            next_scheduled_run_display = current_candidate_dt


        delta = relativedelta(next_scheduled_run_display, now)
        months_remaining = delta.years * 12 + delta.months
        days_remaining = delta.days
        hours_remaining = delta.hours
        minutes_remaining = delta.minutes

        remaining_time_parts = []
        if months_remaining > 0:
            remaining_time_parts.append(f"{months_remaining} เดือน")
        if days_remaining > 0:
            remaining_time_parts.append(f"{days_remaining} วัน")
        if hours_remaining > 0:
            remaining_time_parts.append(f"{hours_remaining} ชั่วโมง")
        if minutes_remaining > 0:
            remaining_time_parts.append(f"{minutes_remaining} นาที")
        
        remaining_time_str = ""
        if remaining_time_parts:
            remaining_time_str = "ในอีก " + ", ".join(remaining_time_parts)


        last_run_info = f"รันล่าสุด: {last_run_str if last_run_str else 'ไม่เคย'}"
        full_next_run_msg = f"{last_run_info} | กำหนดการถัดไป: {next_scheduled_run_display.strftime('%Y-%m-%d %H:%M')}{' | ' + remaining_time_str if remaining_time_str else ''}"
        
        return run_now, next_scheduled_run_display, full_next_run_msg

    def _scheduled_job(self):
        """
        ฟังก์ชันที่ถูกเรียกโดย Thread Background ทุกนาที
        เพื่อตรวจสอบว่าถึงเวลาที่จะรัน Task อัตโนมัติแล้วหรือยัง
        """
        self._log("Scheduler: กำลังตรวจสอบการทำงานตามกำหนดเวลา...", to_app_log=True, to_gui_log=True, show_popup=False) 
        
        # เพิ่มการตรวจสอบแฟล็ก is_task_running ก่อนพิจารณาการรัน
        if self.is_task_running:
            self._log("Scheduler: Task กำลังทำงานอยู่ กำลังข้ามการตรวจสอบเพื่อป้องกันการทำงานซ้ำซ้อน", to_app_log=True, to_gui_log=True, show_popup=False)
            # ไม่จำเป็นต้องอัปเดต next_run_label ที่นี่ _update_next_run_label จะจัดการเอง
            return 

        run_now, next_scheduled_run_display, next_run_info_msg = self._should_schedule_run()
        # ป้ายจะถูกอัปเดตโดย _update_next_run_label ซึ่งจะถูกเรียกซ้ำ
        
        if run_now:
            config = self._load_settings()
            auto_operation = config.get("auto_operation", "move")
            
            self._log(f"✅ ถึงเวลากำหนดการแล้ว - กำลังเริ่มการโอนย้ายข้อมูล ({auto_operation.upper()})", to_app_log=True, to_gui_log=True, show_popup=False) 
            self._run_in_thread(auto_operation) 
        else:
            self._log(f"Scheduler: ยังไม่ถึงเวลากำหนดการทำงาน ครั้งถัดไป: {next_run_info_msg}", to_app_log=True, to_gui_log=True, show_popup=False) 


    def _start_scheduler_thread(self):
        """เริ่มต้น Thread สำหรับการตรวจสอบ Task อัตโนมัติอย่างต่อเนื่อง"""
        def run_schedule_loop():
            # หน่วงเวลาเริ่มต้นเพื่อให้ GUI ตั้งค่าก่อนการตรวจสอบครั้งแรก
            time.sleep(5) 
            while True:
                try:
                    self._scheduled_job()
                except Exception as e:
                    self._log(f"❌ ข้อผิดพลาด: Scheduler Thread Error: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 
                time.sleep(30) # ตรวจสอบทุก 30 วินาที

        # เริ่ม Thread แบบ daemon เพื่อให้มันหยุดทำงานเมื่อโปรแกรมหลักปิด
        threading.Thread(target=run_schedule_loop, daemon=True).start()
        self._log("Scheduler: Thread scheduler พื้นหลังเริ่มทำงานแล้ว", to_app_log=True, to_gui_log=True, show_popup=False)


    def _update_next_run_label(self):
        """อัปเดตข้อความแสดงเวลาการทำงานรอบถัดไปใน GUI ด้วยการเคลื่อนไหวเมื่อมี Task กำลังทำงาน"""
        # ยกเลิกการเรียกที่กำหนดเวลาไว้ก่อนหน้าเพื่อป้องกันการอัปเดตพร้อมกันหลายครั้ง
        if self.after_id_update_label:
            self.master.after_cancel(self.after_id_update_label)
            self.after_id_update_label = None

        try:
            if self.is_task_running:
                # ทำให้ Emojis เคลื่อนไหว
                self.loading_dots_count = (self.loading_dots_count + 1) % len(self.loading_animation_emojis) 
                current_emoji = self.loading_animation_emojis[self.loading_dots_count]
                # ข้อความที่อัปเดตเป็นภาษาไทย
                self.next_run_label.config(text=f"กำลังดำเนินการโอนย้ายไฟล์ {current_emoji}")
                # กำหนดเวลาให้ตัวเองรันอีกครั้งใน 200ms สำหรับ Animation ที่เร็วขึ้น (ปรับได้ตามต้องการ)
                self.after_id_update_label = self.master.after(200, self._update_next_run_label)
            else:
                # รีเซ็ตจำนวนจุดเมื่อไม่ทำงาน
                self.loading_dots_count = 0 
                _, _, full_next_run_msg = self._should_schedule_run()
                self.next_run_label.config(text=f"⏳ {full_next_run_msg}")
                # กำหนดเวลาให้ตัวเองรันอีกครั้งใน 30 วินาที (ช่วงเวลาการอัปเดตปกติ)
                self.after_id_update_label = self.master.after(30000, self._update_next_run_label)

        except Exception as e:
            print(f"ERROR: Exception within _update_next_run_label main logic: {e}")
            self._log(f"❌ ข้อผิดพลาด: ไม่สามารถอัปเดตป้ายบอกเวลากำหนดการถัดไปได้: {e}", to_app_log=True, to_gui_log=True, show_popup=True) 
            # ในกรณีที่เกิดข้อผิดพลาด ให้ยังคงพยายามกำหนดเวลาใหม่เพื่อป้องกันการหยุดชะงักโดยสมบูรณ์
            self.after_id_update_label = self.master.after(30000, self._update_next_run_label)
#แก้ไขssdsdfasdfเอาขึ้น gitดกหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหหฟฟหกดฟหกดฟหกดหฟกด

if __name__ == "__main__":
    root = tk.Tk()
    try:
        app = FileManagerApp(root)
        # เพิ่มข้อความ Debug เพื่อยืนยันว่าถึงจุดนี้แล้ว
        print("DEBUG: FileManagerApp object created. Attempting to start Tkinter main loop...")
    except Exception as e:
        print(f"ERROR: Exception during FileManagerApp instantiation: {e}")
        # แสดงข้อความข้อผิดพลาดสุดท้ายหากไม่สามารถสร้างแอปพลิเคชันได้
        messagebox.showerror("Application Startup Error", f"Failed to start the application: {e}")
        sys.exit(1) # ออกหากไม่สามารถสร้างแอปได้ เพื่อป้องกันกระบวนการค้าง
    root.mainloop()