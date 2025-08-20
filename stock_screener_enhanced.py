"""
台股智能篩選系統 - 增強版（詳細日誌）
支援 20 個篩選條件與 FinMind API 整合
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import threading
import queue
import time
import os
import sys

# 加入 src 目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 匯入自訂模組
from src.enhanced_data_fetcher import EnhancedDataFetcher as DataFetcher
from src.complete_screening_engine import CompleteScreeningEngine as ScreeningEngine
from src.technical_calculator import TechnicalCalculator
from src.scoring_system import ScoringSystem
from src.excel_exporter import ExcelExporter

# 匯入詳細日誌系統
from setup_detailed_logging import get_detailed_logger, setup_module_logging


class StockScreenerEnhanced:
    """增強版股票篩選系統 GUI"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("台股智能篩選系統 v2.0 - Enhanced Edition")
        self.root.geometry("1400x850")
        
        # 初始化詳細日誌系統
        self.detailed_logger = get_detailed_logger("stock_screener")
        setup_module_logging()
        self.detailed_logger.logger.info("="*80)
        self.detailed_logger.logger.info("股票篩選系統啟動")
        self.detailed_logger.logger.info(f"啟動時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.detailed_logger.logger.info("="*80)
        
        # 設定樣式
        self.setup_styles()
        
        # 載入設定
        self.load_config()
        
        # 初始化元件
        self.data_fetcher = None
        self.screening_engine = None
        self.results = []
        self.screening_thread = None
        self.stop_screening = False
        
        # 統計資料
        self.stats = {
            'total_stocks': 0,
            'processed': 0,
            'passed': 0,
            'failed': 0,
            'conditions_stats': {}
        }
        
        # 建立 GUI
        self.create_gui()
        
        # 測試 API 連線
        self.root.after(100, self.test_api_connection)
    
    def setup_styles(self):
        """設定視覺樣式"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # 自訂顏色
        style.configure('Title.TLabel', font=('Arial', 12, 'bold'))
        style.configure('Success.TLabel', foreground='green')
        style.configure('Error.TLabel', foreground='red')
        style.configure('Accent.TButton', font=('Arial', 10, 'bold'))
    
    def load_config(self):
        """載入設定檔"""
        try:
            with open('config.json', 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except Exception as e:
            print(f"載入 config.json 失敗: {e}，使用預設值")
            self.config = {}
        
        try:
            with open('api_config.json', 'r', encoding='utf-8') as f:
                self.api_config = json.load(f)
        except Exception as e:
            print(f"載入 api_config.json 失敗: {e}，使用預設值")
            self.api_config = {
                'finmind': {
                    'api_token': '',
                    'base_url': 'https://api.finmindtrade.com/api/v4/data'
                }
            }
        
        # 從環境變數讀取 Token (優先級最高)
        env_token = os.getenv('FINMIND_TOKEN')
        if env_token:
            self.api_config.setdefault('finmind', {})['api_token'] = env_token
    
    def create_gui(self):
        """建立 GUI 介面"""
        # 頂部工具列
        self.create_toolbar()
        
        # 主要內容區
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左側控制面板
        left_frame = ttk.Frame(main_paned, width=350)
        self.create_control_panel(left_frame)
        main_paned.add(left_frame, weight=0)
        
        # 右側結果區
        right_frame = ttk.Frame(main_paned)
        self.create_result_area(right_frame)
        main_paned.add(right_frame, weight=1)
        
        # 底部狀態列
        self.create_statusbar()
    
    def create_toolbar(self):
        """建立工具列"""
        toolbar = ttk.Frame(self.root)
        toolbar.pack(fill=tk.X, padx=5, pady=2)
        
        # 主要按鈕
        self.btn_screen = ttk.Button(
            toolbar, text="🚀 立即篩選", 
            command=self.start_screening,
            style='Accent.TButton'
        )
        self.btn_screen.pack(side=tk.LEFT, padx=5)
        
        self.btn_stop = ttk.Button(
            toolbar, text="⏹ 停止",
            command=self.stop_screening_process,
            state='disabled'
        )
        self.btn_stop.pack(side=tk.LEFT)
        
        ttk.Separator(toolbar, orient='vertical').pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # 測試模式選項
        self.test_mode = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            toolbar, text="測試模式(只跑10檔)", 
            variable=self.test_mode
        ).pack(side=tk.LEFT, padx=5)
        
        # 詳細日誌選項
        self.verbose_log = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            toolbar, text="詳細日誌", 
            variable=self.verbose_log
        ).pack(side=tk.LEFT, padx=5)
        
        # 顯示筆數設定
        ttk.Label(toolbar, text="顯示前").pack(side=tk.LEFT, padx=(10, 2))
        self.max_results_var = tk.IntVar(value=30)
        results_spinbox = ttk.Spinbox(
            toolbar, from_=10, to=100, increment=10,
            textvariable=self.max_results_var, width=5
        )
        results_spinbox.pack(side=tk.LEFT)
        ttk.Label(toolbar, text="筆").pack(side=tk.LEFT, padx=(2, 5))
        
        ttk.Separator(toolbar, orient='vertical').pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        ttk.Button(
            toolbar, text="📊 匯出Excel",
            command=self.export_to_excel
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            toolbar, text="💾 儲存設定",
            command=self.save_settings
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            toolbar, text="📖 說明",
            command=self.show_help
        ).pack(side=tk.LEFT, padx=2)
        
        # API 連線狀態
        ttk.Label(toolbar, text="API狀態:").pack(side=tk.RIGHT, padx=5)
        self.api_status_label = ttk.Label(toolbar, text="未連線", foreground="gray")
        self.api_status_label.pack(side=tk.RIGHT)
    
    def create_control_panel(self, parent):
        """建立左側控制面板"""
        # 使用捲軸容器
        canvas = tk.Canvas(parent, width=330)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 快速設定
        self.create_quick_settings(scrollable_frame)
        
        # API 設定群組
        self.create_api_group(scrollable_frame)
        
        # 市場範圍群組
        self.create_market_group(scrollable_frame)
        
        # 成交量條件群組
        self.create_volume_group(scrollable_frame)
        
        # 技術指標群組
        self.create_technical_group(scrollable_frame)
        
        # 法人籌碼群組
        self.create_institutional_group(scrollable_frame)
        
        # 融資融券群組
        self.create_margin_group(scrollable_frame)
        
        # 基本面群組
        self.create_fundamental_group(scrollable_frame)
        
        # 漲幅控制群組
        self.create_price_change_group(scrollable_frame)
        
        # 排除條件群組
        self.create_exclusion_group(scrollable_frame)
        
        # 篩選門檻設定
        self.create_threshold_group(scrollable_frame)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
    
    def create_quick_settings(self, parent):
        """快速設定"""
        group = ttk.LabelFrame(parent, text="快速設定", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(
            group, text="寬鬆模式",
            command=lambda: self.apply_preset('loose')
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            group, text="標準模式",
            command=lambda: self.apply_preset('standard')
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            group, text="嚴格模式",
            command=lambda: self.apply_preset('strict')
        ).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(
            group, text="全部關閉",
            command=lambda: self.apply_preset('none')
        ).pack(side=tk.LEFT, padx=2)
    
    def apply_preset(self, mode):
        """套用預設模式"""
        if mode == 'loose':
            # 寬鬆模式 - 調整為更合理的門檻
            self.min_conditions_value.set(3)
            self.volume_surge1_enabled.set(True)
            self.volume_surge1_value.set(0.8)  # 降低門檻因應清淡成交量
            self.min_volume_enabled.set(True)
            self.min_volume_value.set(500)
            self.daily_kd_golden.set(False)
            self.monthly_kd_golden.set(False)
            self.above_ma20.set(False)
            self.break_60d_high.set(False)
            self.trust_buy_enabled.set(False)
            self.eps_enabled.set(False)
            self.roe_enabled.set(False)
            self.yield_enabled.set(False)
            self.log_message("套用寬鬆模式：最少 3 個條件", "INFO")
            
        elif mode == 'standard':
            # 標準模式 - 調整門檻
            self.min_conditions_value.set(5)
            self.volume_surge1_enabled.set(True)
            self.volume_surge1_value.set(1.0)  # 調整為 1.0 倍
            self.min_volume_enabled.set(True)
            self.min_volume_value.set(1000)
            self.daily_kd_golden.set(True)
            self.above_ma20.set(True)
            self.trust_buy_enabled.set(True)
            self.trust_buy_value.set(100)
            self.log_message("套用標準模式：最少 5 個條件", "INFO")
            
        elif mode == 'strict':
            # 嚴格模式 - 開啟所有條件
            self.min_conditions_value.set(10)
            self.volume_surge1_enabled.set(True)
            self.volume_surge2_enabled.set(True)
            self.volume_surge3_enabled.set(True)
            self.daily_kd_golden.set(True)
            self.monthly_kd_golden.set(True)
            self.above_ma20.set(True)
            self.break_60d_high.set(True)
            self.trust_buy_enabled.set(True)
            self.eps_enabled.set(True)
            self.roe_enabled.set(True)
            self.yield_enabled.set(True)
            self.log_message("套用嚴格模式：最少 10 個條件", "INFO")
            
        elif mode == 'none':
            # 關閉所有條件
            self.min_conditions_value.set(0)
            self.volume_surge1_enabled.set(False)
            self.volume_surge2_enabled.set(False)
            self.volume_surge3_enabled.set(False)
            self.min_volume_enabled.set(False)
            self.daily_kd_golden.set(False)
            self.monthly_kd_golden.set(False)
            self.above_ma20.set(False)
            self.break_60d_high.set(False)
            self.trust_buy_enabled.set(False)
            self.trust_pct_enabled.set(False)
            self.trust_5d_enabled.set(False)
            self.trust_holding_enabled.set(False)
            self.inst_5d_enabled.set(False)
            self.margin_ratio_enabled.set(False)
            self.margin_5d_enabled.set(False)
            self.eps_enabled.set(False)
            self.roe_enabled.set(False)
            self.yield_enabled.set(False)
            self.daily_change_enabled.set(False)
            self.change_5d_enabled.set(False)
            self.exclude_warning.set(False)
            self.exclude_disposition.set(False)
            self.exclude_limit_up_enabled.set(False)
            self.log_message("關閉所有條件", "INFO")
    
    def create_threshold_group(self, parent):
        """篩選門檻設定"""
        group = ttk.LabelFrame(parent, text="篩選門檻", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        frame = ttk.Frame(group)
        frame.pack(fill=tk.X)
        
        ttk.Label(frame, text="最少符合條件數:").pack(side=tk.LEFT)
        self.min_conditions_value = tk.IntVar(value=5)
        ttk.Spinbox(
            frame, from_=0, to=20, increment=1,
            textvariable=self.min_conditions_value, width=8
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(frame, text="(0=不限制)").pack(side=tk.LEFT)
    
    def create_api_group(self, parent):
        """API 設定群組"""
        group = ttk.LabelFrame(parent, text="API 設定", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(group, text="FinMind Token:").grid(row=0, column=0, sticky=tk.W)
        
        # Token 從設定或環境變數取得
        initial_token = self.api_config.get('finmind', {}).get('api_token', '')
        if os.getenv('FINMIND_TOKEN'):
            ttk.Label(group, text="(使用環境變數)", foreground="green").grid(row=0, column=1, sticky=tk.W)
        
        self.token_var = tk.StringVar(value=initial_token)
        token_entry = ttk.Entry(group, textvariable=self.token_var, width=30, show="*")
        token_entry.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=2)
        
        ttk.Button(
            group, text="測試連線",
            command=self.test_api_connection
        ).grid(row=2, column=0, pady=5)
        
        self.api_test_label = ttk.Label(group, text="")
        self.api_test_label.grid(row=2, column=1, padx=5)
    
    def create_market_group(self, parent):
        """市場範圍群組"""
        group = ttk.LabelFrame(parent, text="市場範圍", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        self.market_twse = tk.BooleanVar(value=True)
        self.market_otc = tk.BooleanVar(value=True)
        self.market_emerging = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(group, text="上市股票", variable=self.market_twse).pack(anchor=tk.W)
        ttk.Checkbutton(group, text="上櫃股票", variable=self.market_otc).pack(anchor=tk.W)
        ttk.Checkbutton(group, text="興櫃股票", variable=self.market_emerging).pack(anchor=tk.W)
    
    def create_volume_group(self, parent):
        """成交量條件群組"""
        group = ttk.LabelFrame(parent, text="成交量條件", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        # 爆量條件1
        frame1 = ttk.Frame(group)
        frame1.pack(fill=tk.X, pady=2)
        self.volume_surge1_enabled = tk.BooleanVar(value=True)
        ttk.Checkbutton(frame1, text="爆量條件1: >", variable=self.volume_surge1_enabled).pack(side=tk.LEFT)
        self.volume_surge1_value = tk.DoubleVar(value=1.5)
        ttk.Spinbox(frame1, from_=1.0, to=10.0, increment=0.1, textvariable=self.volume_surge1_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame1, text="倍 5日均量").pack(side=tk.LEFT)
        
        # 爆量條件2 (20日均量)
        frame2 = ttk.Frame(group)
        frame2.pack(fill=tk.X, pady=2)
        self.volume_surge2_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame2, text="爆量條件2: >", variable=self.volume_surge2_enabled).pack(side=tk.LEFT)
        self.volume_surge2_value = tk.DoubleVar(value=3.0)
        ttk.Spinbox(frame2, from_=1.0, to=10.0, increment=0.1, textvariable=self.volume_surge2_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame2, text="倍 20日均量").pack(side=tk.LEFT)
        
        # 爆量條件3 (60日均量)
        frame3 = ttk.Frame(group)
        frame3.pack(fill=tk.X, pady=2)
        self.volume_surge3_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame3, text="爆量條件3: >", variable=self.volume_surge3_enabled).pack(side=tk.LEFT)
        self.volume_surge3_value = tk.DoubleVar(value=5.0)
        ttk.Spinbox(frame3, from_=1.0, to=10.0, increment=0.1, textvariable=self.volume_surge3_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame3, text="倍 60日均量").pack(side=tk.LEFT)
        
        # 最低成交量
        frame4 = ttk.Frame(group)
        frame4.pack(fill=tk.X, pady=2)
        self.min_volume_enabled = tk.BooleanVar(value=True)
        ttk.Checkbutton(frame4, text="最低成交量: >", variable=self.min_volume_enabled).pack(side=tk.LEFT)
        self.min_volume_value = tk.IntVar(value=1000)
        ttk.Spinbox(frame4, from_=0, to=100000, increment=100, textvariable=self.min_volume_value, width=8).pack(side=tk.LEFT)
        ttk.Label(frame4, text="張").pack(side=tk.LEFT)
    
    def create_technical_group(self, parent):
        """技術指標群組"""
        group = ttk.LabelFrame(parent, text="技術指標", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        self.daily_kd_golden = tk.BooleanVar(value=False)
        self.monthly_kd_golden = tk.BooleanVar(value=False)
        self.above_ma20 = tk.BooleanVar(value=False)
        self.break_60d_high = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(group, text="日KD黃金交叉", variable=self.daily_kd_golden).pack(anchor=tk.W)
        ttk.Checkbutton(group, text="月KD黃金交叉", variable=self.monthly_kd_golden).pack(anchor=tk.W)
        ttk.Checkbutton(group, text="股價站上20日均線", variable=self.above_ma20).pack(anchor=tk.W)
        ttk.Checkbutton(group, text="突破60日新高", variable=self.break_60d_high).pack(anchor=tk.W)
        
        frame = ttk.Frame(group)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text="突破時成交量倍數:").pack(side=tk.LEFT)
        self.break_volume_multiplier = tk.DoubleVar(value=2.0)
        ttk.Spinbox(frame, from_=1.0, to=5.0, increment=0.1, textvariable=self.break_volume_multiplier, width=6).pack(side=tk.LEFT)
    
    def create_institutional_group(self, parent):
        """法人籌碼群組"""
        group = ttk.LabelFrame(parent, text="法人籌碼", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        # 投信買超
        frame1 = ttk.Frame(group)
        frame1.pack(fill=tk.X, pady=2)
        self.trust_buy_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame1, text="投信買超 >", variable=self.trust_buy_enabled).pack(side=tk.LEFT)
        self.trust_buy_value = tk.IntVar(value=500)
        ttk.Spinbox(frame1, from_=0, to=10000, increment=100, textvariable=self.trust_buy_value, width=8).pack(side=tk.LEFT)
        ttk.Label(frame1, text="張").pack(side=tk.LEFT)
        
        # 投信買超佔比
        frame2 = ttk.Frame(group)
        frame2.pack(fill=tk.X, pady=2)
        self.trust_pct_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame2, text="投信買超佔比 >", variable=self.trust_pct_enabled).pack(side=tk.LEFT)
        self.trust_pct_value = tk.DoubleVar(value=3.0)
        ttk.Spinbox(frame2, from_=0, to=100, increment=0.5, textvariable=self.trust_pct_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame2, text="%").pack(side=tk.LEFT)
        
        # 投信5日累計
        frame3 = ttk.Frame(group)
        frame3.pack(fill=tk.X, pady=2)
        self.trust_5d_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame3, text="投信5日累計 >", variable=self.trust_5d_enabled).pack(side=tk.LEFT)
        self.trust_5d_value = tk.IntVar(value=1000)
        ttk.Spinbox(frame3, from_=0, to=50000, increment=100, textvariable=self.trust_5d_value, width=8).pack(side=tk.LEFT)
        ttk.Label(frame3, text="張").pack(side=tk.LEFT)
        
        # 投信持股 (需月頻資料，暫時停用)
        frame4 = ttk.Frame(group)
        frame4.pack(fill=tk.X, pady=2)
        self.trust_holding_enabled = tk.BooleanVar(value=False)  # 預設關閉
        ttk.Checkbutton(frame4, text="投信持股 <", variable=self.trust_holding_enabled).pack(side=tk.LEFT)
        self.trust_holding_value = tk.DoubleVar(value=15.0)
        ttk.Spinbox(frame4, from_=0, to=100, increment=1, textvariable=self.trust_holding_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame4, text="% (需月頻資料)").pack(side=tk.LEFT)
        
        # 法人5日淨買超
        frame5 = ttk.Frame(group)
        frame5.pack(fill=tk.X, pady=2)
        self.inst_5d_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame5, text="法人5日淨買超 >", variable=self.inst_5d_enabled).pack(side=tk.LEFT)
        self.inst_5d_value = tk.IntVar(value=100)
        ttk.Spinbox(frame5, from_=0, to=10000, increment=100, textvariable=self.inst_5d_value, width=8).pack(side=tk.LEFT)
        ttk.Label(frame5, text="張").pack(side=tk.LEFT)
    
    def create_margin_group(self, parent):
        """融資融券群組"""
        group = ttk.LabelFrame(parent, text="融資融券", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        # 融資增減比
        frame1 = ttk.Frame(group)
        frame1.pack(fill=tk.X, pady=2)
        self.margin_ratio_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame1, text="融資增減比 <", variable=self.margin_ratio_enabled).pack(side=tk.LEFT)
        self.margin_ratio_value = tk.DoubleVar(value=5.0)
        ttk.Spinbox(frame1, from_=0, to=100, increment=1, textvariable=self.margin_ratio_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame1, text="%").pack(side=tk.LEFT)
        
        # 融資5日增幅
        frame2 = ttk.Frame(group)
        frame2.pack(fill=tk.X, pady=2)
        self.margin_5d_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame2, text="融資5日增幅 <", variable=self.margin_5d_enabled).pack(side=tk.LEFT)
        self.margin_5d_value = tk.DoubleVar(value=10.0)
        ttk.Spinbox(frame2, from_=0, to=100, increment=1, textvariable=self.margin_5d_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame2, text="%").pack(side=tk.LEFT)
    
    def create_fundamental_group(self, parent):
        """基本面群組"""
        group = ttk.LabelFrame(parent, text="基本面", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        # EPS (注意：目前使用預設值)
        frame1 = ttk.Frame(group)
        frame1.pack(fill=tk.X, pady=2)
        self.eps_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame1, text="EPS >", variable=self.eps_enabled).pack(side=tk.LEFT)
        self.eps_value = tk.DoubleVar(value=2.0)  # 調整預設值以配合資料
        ttk.Spinbox(frame1, from_=-10, to=100, increment=0.5, textvariable=self.eps_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame1, text="(資料可能為預設值)", foreground="orange").pack(side=tk.LEFT, padx=5)
        
        # ROE
        frame2 = ttk.Frame(group)
        frame2.pack(fill=tk.X, pady=2)
        self.roe_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame2, text="ROE >", variable=self.roe_enabled).pack(side=tk.LEFT)
        self.roe_value = tk.DoubleVar(value=10.0)
        ttk.Spinbox(frame2, from_=0, to=100, increment=1, textvariable=self.roe_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame2, text="%").pack(side=tk.LEFT)
        
        # 殖利率
        frame3 = ttk.Frame(group)
        frame3.pack(fill=tk.X, pady=2)
        self.yield_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame3, text="殖利率 >", variable=self.yield_enabled).pack(side=tk.LEFT)
        self.yield_value = tk.DoubleVar(value=3.0)
        ttk.Spinbox(frame3, from_=0, to=20, increment=0.5, textvariable=self.yield_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame3, text="%").pack(side=tk.LEFT)
    
    def create_price_change_group(self, parent):
        """漲幅控制群組"""
        group = ttk.LabelFrame(parent, text="漲幅控制", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        # 當日漲幅
        frame1 = ttk.Frame(group)
        frame1.pack(fill=tk.X, pady=2)
        self.daily_change_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame1, text="當日漲幅 <", variable=self.daily_change_enabled).pack(side=tk.LEFT)
        self.daily_change_value = tk.DoubleVar(value=7.0)
        ttk.Spinbox(frame1, from_=0, to=10, increment=0.5, textvariable=self.daily_change_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame1, text="%").pack(side=tk.LEFT)
        
        # 5日累計漲幅
        frame2 = ttk.Frame(group)
        frame2.pack(fill=tk.X, pady=2)
        self.change_5d_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame2, text="5日累計漲幅 <", variable=self.change_5d_enabled).pack(side=tk.LEFT)
        self.change_5d_value = tk.DoubleVar(value=20.0)
        ttk.Spinbox(frame2, from_=0, to=50, increment=1, textvariable=self.change_5d_value, width=6).pack(side=tk.LEFT)
        ttk.Label(frame2, text="%").pack(side=tk.LEFT)
    
    def create_exclusion_group(self, parent):
        """排除條件群組"""
        group = ttk.LabelFrame(parent, text="排除條件", padding="10")
        group.pack(fill=tk.X, padx=5, pady=5)
        
        self.exclude_warning = tk.BooleanVar(value=False)
        self.exclude_disposition = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(group, text="排除警示股", variable=self.exclude_warning).pack(anchor=tk.W)
        ttk.Checkbutton(group, text="排除處置股", variable=self.exclude_disposition).pack(anchor=tk.W)
        
        frame = ttk.Frame(group)
        frame.pack(fill=tk.X, pady=2)
        self.exclude_limit_up_enabled = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame, text="排除連續", variable=self.exclude_limit_up_enabled).pack(side=tk.LEFT)
        self.exclude_limit_up_days = tk.IntVar(value=3)
        ttk.Spinbox(frame, from_=1, to=10, increment=1, textvariable=self.exclude_limit_up_days, width=4).pack(side=tk.LEFT)
        ttk.Label(frame, text="日漲停").pack(side=tk.LEFT)
    
    def create_result_area(self, parent):
        """建立右側結果區"""
        # 頂部進度條
        progress_frame = ttk.Frame(parent)
        progress_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.progress_label = ttk.Label(progress_frame, text="就緒")
        self.progress_label.pack(side=tk.LEFT, padx=5)
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate')
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        self.progress_text = ttk.Label(progress_frame, text="0%")
        self.progress_text.pack(side=tk.LEFT, padx=5)
        
        # 分頁標籤
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 篩選結果分頁
        self.result_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.result_tab, text="篩選結果")
        self.create_result_table(self.result_tab)
        
        # 日誌分頁
        self.log_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.log_tab, text="執行日誌")
        self.create_log_area(self.log_tab)
        
        # 統計分頁
        self.stats_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.stats_tab, text="統計分析")
        self.create_stats_area(self.stats_tab)
    
    def create_result_table(self, parent):
        """建立結果表格"""
        # 表格框架
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 定義欄位
        columns = [
            '排名', '股票代碼', '股票名稱', '潛力分數', '收盤價',
            '漲跌幅%', '成交量', '爆量倍數', 'K值', 'D值',
            '投信買超', '符合條件', '關鍵信號'
        ]
        
        # 建立表格
        self.result_tree = ttk.Treeview(frame, columns=columns, show='headings', height=25)
        
        # 設定欄位
        column_widths = [50, 80, 100, 80, 80, 80, 100, 80, 60, 60, 80, 80, 150]
        for col, width in zip(columns, column_widths):
            self.result_tree.heading(col, text=col, command=lambda c=col: self.sort_results(c))
            self.result_tree.column(col, width=width, anchor=tk.CENTER)
        
        # 捲軸
        v_scrollbar = ttk.Scrollbar(frame, orient='vertical', command=self.result_tree.yview)
        h_scrollbar = ttk.Scrollbar(frame, orient='horizontal', command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # 配置
        self.result_tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)
        
        # 右鍵選單
        self.create_context_menu()
        self.result_tree.bind("<Button-3>", self.show_context_menu)
        
        # 雙擊事件
        self.result_tree.bind("<Double-Button-1>", self.show_stock_details)
    
    def create_log_area(self, parent):
        """建立日誌區域"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 日誌文字框
        self.log_text = tk.Text(frame, wrap=tk.WORD, height=30)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # 捲軸
        scrollbar = ttk.Scrollbar(frame, orient='vertical', command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        # 設定標籤樣式
        self.log_text.tag_configure('INFO', foreground='black')
        self.log_text.tag_configure('SUCCESS', foreground='green')
        self.log_text.tag_configure('WARNING', foreground='orange')
        self.log_text.tag_configure('ERROR', foreground='red')
        self.log_text.tag_configure('DETAIL', foreground='blue')
    
    def create_stats_area(self, parent):
        """建立統計區域"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 統計資訊標籤
        self.stats_text = tk.Text(frame, wrap=tk.WORD, height=30)
        self.stats_text.pack(fill=tk.BOTH, expand=True)
    
    def create_statusbar(self):
        """建立狀態列"""
        statusbar = ttk.Frame(self.root)
        statusbar.pack(fill=tk.X, side=tk.BOTTOM)
        
        # 篩選進度
        ttk.Label(statusbar, text="狀態:").pack(side=tk.LEFT, padx=5)
        self.status_label = ttk.Label(statusbar, text="就緒")
        self.status_label.pack(side=tk.LEFT, padx=5)
        
        ttk.Separator(statusbar, orient='vertical').pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # 符合條件數
        self.match_label = ttk.Label(statusbar, text="符合條件: 0 檔")
        self.match_label.pack(side=tk.LEFT, padx=5)
        
        # 統計資訊
        self.stats_label = ttk.Label(statusbar, text="處理: 0/0")
        self.stats_label.pack(side=tk.LEFT, padx=5)
        
        # 時間
        self.time_label = ttk.Label(statusbar, text="")
        self.time_label.pack(side=tk.RIGHT, padx=5)
        self.update_time()
    
    def create_context_menu(self):
        """建立右鍵選單"""
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="查看詳情", command=self.show_stock_details)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="複製代碼", command=self.copy_stock_code)
        self.context_menu.add_command(label="加入自選", command=self.add_to_watchlist)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="匯出選取", command=self.export_selected)
    
    def show_context_menu(self, event):
        """顯示右鍵選單"""
        self.context_menu.post(event.x_root, event.y_root)
    
    def test_api_connection(self):
        """測試 API 連線"""
        self.log_message("測試 FinMind API 連線...", "INFO")
        
        try:
            # 取得 token (如果使用者有輸入的話)
            if hasattr(self, 'token_var') and self.token_var.get():
                self.api_config['finmind']['api_token'] = self.token_var.get()
            
            # 先儲存設定到檔案，讓 DataFetcher 可以讀取
            try:
                with open('api_config.json', 'w', encoding='utf-8') as f:
                    json.dump(self.api_config, f, indent=2, ensure_ascii=False)
            except:
                pass  # 如果無法儲存，繼續使用記憶體中的設定
            
            # 建立增強版資料擷取器
            self.data_fetcher = DataFetcher()
            
            # 測試連線 (不在日誌顯示 Token)
            if self.data_fetcher.test_connection():
                self.api_status_label.config(text="已連線", foreground="green")
                self.api_test_label.config(text="✓ 連線成功", foreground="green")
                self.log_message("FinMind API 連線成功", "SUCCESS")
                return True
            else:
                self.api_status_label.config(text="連線失敗", foreground="red")
                self.api_test_label.config(text="✗ 連線失敗", foreground="red")
                self.log_message("FinMind API 連線失敗", "ERROR")
                return False
                
        except Exception as e:
            self.api_status_label.config(text="錯誤", foreground="red")
            self.api_test_label.config(text=f"✗ {str(e)[:30]}", foreground="red")
            self.log_message(f"API 連線錯誤: {e}", "ERROR")
            return False
    
    def start_screening(self):
        """開始篩選"""
        # 檢查 API 連線
        if not self.data_fetcher or not self.data_fetcher.test_connection():
            if not self.test_api_connection():
                messagebox.showerror("錯誤", "請先設定並測試 FinMind API Token")
                return
        
        # 清空結果
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)
        self.results = []
        
        # 重置統計
        self.stats = {
            'total_stocks': 0,
            'processed': 0,
            'passed': 0,
            'failed': 0,
            'conditions_stats': {}
        }
        
        # 更新UI狀態
        self.btn_screen.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.stop_screening = False
        
        # 取得篩選參數
        params = self.get_screening_params()
        
        # 顯示篩選參數
        self.log_message("="*60, "INFO")
        self.log_message("開始篩選程序", "INFO")
        self.log_message(f"最少符合條件數: {params.get('min_conditions_to_pass', 0)}", "INFO")
        
        # 列出啟用的條件
        enabled_conditions = []
        for key, value in params.items():
            if isinstance(value, dict) and value.get('enabled'):
                enabled_conditions.append(f"{key}: {value.get('value')}")
            elif isinstance(value, bool) and value:
                enabled_conditions.append(key)
        
        self.log_message(f"啟用條件數: {len(enabled_conditions)}", "INFO")
        for condition in enabled_conditions:
            self.log_message(f"  • {condition}", "DETAIL")
        
        # 在背景執行緒執行篩選
        self.screening_thread = threading.Thread(target=self.run_screening, args=(params,))
        self.screening_thread.daemon = True
        self.screening_thread.start()
    
    def run_screening(self, params):
        """執行篩選（背景執行緒）"""
        try:
            self.update_status("取得股票清單...")
            
            # 取得股票清單
            stock_list = self.data_fetcher.get_stock_list()
            
            if stock_list.empty:
                self.log_message("無法取得股票清單", "ERROR")
                return
            
            # 根據市場範圍篩選
            markets = []
            if params['market_twse']:
                markets.append('twse')
            if params['market_otc']:
                markets.extend(['otc', 'tpex'])  # Support both old and new OTC naming
            if params['market_emerging']:
                markets.append('emerging')
            
            # 套用市場篩選
            if markets and 'type' in stock_list.columns:
                stock_list = stock_list[stock_list['type'].isin(markets)].copy()
                self.log_message(f"市場篩選: {markets}，共 {len(stock_list)} 檔", "INFO")
            
            # 測試模式只跑前10檔 (以成交量排序)
            if self.test_mode.get():
                # 嘗試以最近一日成交量排序
                if 'latest_volume' in stock_list.columns:
                    stock_list = stock_list.sort_values('latest_volume', ascending=False).head(10)
                    self.log_message("測試模式：只篩選前 10 檔股票（以最近一日成交量排序）", "WARNING")
                else:
                    stock_list = stock_list.head(10)
                    self.log_message("測試模式：只篩選前 10 檔股票", "WARNING")
            
            total_stocks = len(stock_list)
            self.stats['total_stocks'] = total_stocks
            self.log_message(f"共 {total_stocks} 檔股票待篩選", "INFO")
            
            # 設定最少符合條件數
            min_conditions = params.get('min_conditions_to_pass', 0)
            
            # 初始化篩選引擎
            self.screening_engine = ScreeningEngine(params)
            self.screening_engine.min_conditions = min_conditions  # 設定門檻
            
            scoring_system = ScoringSystem()
            
            # 逐檔篩選
            qualified_stocks = []
            
            for idx, (_, stock) in enumerate(stock_list.iterrows()):
                if self.stop_screening:
                    break
                
                stock_id = stock.get('stock_id', stock.get('代號', ''))
                stock_name = stock.get('stock_name', stock.get('名稱', ''))
                
                # 更新進度
                progress = (idx + 1) / total_stocks * 100
                self.update_progress(progress, f"篩選中: {stock_id} {stock_name}")
                self.stats['processed'] = idx + 1
                self.update_stats_display()
                
                try:
                    # 取得股票資料
                    self.detailed_logger.logger.debug(f"開始取得 {stock_id} 資料")
                    stock_data = self.data_fetcher.get_all_data(stock_id)
                    
                    # 記錄取得的資料
                    self.detailed_logger.log_data_processing(
                        stock_id, 
                        "get_all_data_result",
                        {
                            'eps': stock_data.get('eps'),
                            'roe': stock_data.get('roe'),
                            'trust_holding': stock_data.get('trust_holding'),
                            'error': stock_data.get('error'),
                            'has_price_data': stock_data.get('price') is not None,
                            'has_inst_data': stock_data.get('institutional') is not None,
                            'has_margin_data': stock_data.get('margin') is not None
                        }
                    )
                    
                    if stock_data.get('error'):
                        self.detailed_logger.logger.warning(f"{stock_id} 資料錯誤: {stock_data['error']}")
                        if self.verbose_log.get():
                            self.log_message(f"  {stock_id}: 資料錯誤 - {stock_data['error']}", "WARNING")
                        continue
                    
                    # 資料驗證警告（檢查是否為 0 或異常值）
                    eps_value = stock_data.get('eps', 0)
                    roe_value = stock_data.get('roe', 0)
                    
                    # 詳細記錄 EPS 和 ROE 的來源
                    self.detailed_logger.logger.info(
                        f"{stock_id} 財務數據: EPS={eps_value:.2f}, ROE={roe_value:.1f}%, "
                        f"Trust={stock_data.get('trust_holding', 0):.2f}%"
                    )
                    
                    # 顯示實際取得的 EPS 和 ROE 值
                    self.log_message(f"  {stock_id} EPS: {eps_value:.2f}, ROE: {roe_value:.1f}%", "INFO")
                    
                    # 只有當值為 0 或負值時才警告
                    if eps_value <= 0:
                        self.log_message(f"  警告: {stock_id} EPS 資料異常: {eps_value}", "WARNING")
                    if roe_value <= 0:
                        self.log_message(f"  警告: {stock_id} ROE 資料異常: {roe_value}%", "WARNING")
                    
                    # 檢查篩選條件
                    self.detailed_logger.logger.debug(f"開始檢查 {stock_id} 篩選條件")
                    check_result = self.screening_engine.check_all_conditions(stock_data)
                    
                    # 記錄篩選結果
                    self.detailed_logger.log_screening_process(
                        stock_id,
                        self.get_screening_params(),
                        check_result
                    )
                    
                    # 詳細日誌
                    if self.verbose_log.get():
                        self.log_message(f"\n檢查 {stock_id} {stock_name}:", "DETAIL")
                        
                        # 列出各條件檢查結果和數值
                        values_dict = check_result.get('values', {})
                        for condition, passed in check_result.items():
                            if condition not in ['matched_count', 'passed', 'values']:
                                status = "✓" if passed else "✗"
                                # 如果有數值資訊，顯示數值
                                if condition in values_dict:
                                    self.log_message(f"  {status} {condition}: {values_dict[condition]}", "DETAIL")
                                else:
                                    self.log_message(f"  {status} {condition}: {passed}", "DETAIL")
                        
                        # 顯示連續漲停天數（如果有）
                        limit_up_streak = values_dict.get('limit_up_streak')
                        if limit_up_streak is not None:
                            self.log_message(f"  連續漲停天數: {limit_up_streak}", "DETAIL")
                        
                        self.log_message(f"  符合條件數: {check_result.get('matched_count', 0)}/{min_conditions}", "DETAIL")
                    
                    # 更新條件統計
                    for condition, passed in check_result.items():
                        if condition not in ['matched_count', 'passed', 'values']:
                            if condition not in self.stats['conditions_stats']:
                                self.stats['conditions_stats'][condition] = {'passed': 0, 'failed': 0}
                            if passed:
                                self.stats['conditions_stats'][condition]['passed'] += 1
                            else:
                                self.stats['conditions_stats'][condition]['failed'] += 1
                    
                    if check_result.get('passed', False):
                        # 計算分數
                        score = scoring_system.calculate_score(check_result)
                        
                        # 準備結果資料
                        result = {
                            'stock_id': stock_id,
                            'stock_name': stock_name,
                            'score': score['total_score'],
                            'grade': score['grade'],
                            'signal': score['key_signal'],
                            'matched_conditions': check_result['matched_count'],
                            'data': stock_data,
                            'check_result': check_result
                        }
                        
                        qualified_stocks.append(result)
                        self.add_result_to_table(result, len(qualified_stocks))
                        
                        self.stats['passed'] += 1
                        self.log_message(f"✓ {stock_id} {stock_name} 符合條件 (分數: {score['total_score']:.1f})", "SUCCESS")
                    else:
                        self.stats['failed'] += 1
                        
                except Exception as e:
                    self.log_message(f"篩選 {stock_id} 時發生錯誤: {e}", "ERROR")
                    continue
                
                # 避免請求過快
                time.sleep(0.5)
            
            # 排序結果
            qualified_stocks.sort(key=lambda x: x['score'], reverse=True)
            
            # 限制顯示筆數
            max_display = self.max_results_var.get()
            if len(qualified_stocks) > max_display:
                self.log_message(f"\n找到 {len(qualified_stocks)} 檔符合條件，顯示前 {max_display} 檔", "INFO")
                display_stocks = qualified_stocks[:max_display]
            else:
                display_stocks = qualified_stocks
            
            # 清空表格並重新顯示結果
            self.result_tree.delete(*self.result_tree.get_children())
            for idx, result in enumerate(display_stocks, 1):
                self.add_result_to_table(result, idx)
            
            # 更新統計
            self.update_statistics(qualified_stocks)
            
            # 顯示條件統計
            self.show_conditions_statistics()
            
            # 完成
            self.log_message("="*60, "INFO")
            self.log_message(f"篩選完成！", "SUCCESS")
            self.log_message(f"總計: {self.stats['processed']}/{self.stats['total_stocks']} 檔已處理", "INFO")
            self.log_message(f"符合: {self.stats['passed']} 檔", "SUCCESS")
            self.log_message(f"不符: {self.stats['failed']} 檔", "WARNING")
            self.update_status(f"完成 - 找到 {len(qualified_stocks)} 檔股票")
            
        except Exception as e:
            self.log_message(f"篩選過程發生錯誤: {e}", "ERROR")
            
        finally:
            # 恢復按鈕狀態
            self.root.after(0, lambda: self.btn_screen.config(state='normal'))
            self.root.after(0, lambda: self.btn_stop.config(state='disabled'))
    
    def show_conditions_statistics(self):
        """顯示條件統計"""
        self.log_message("\n條件統計結果:", "INFO")
        self.log_message("-"*40, "INFO")
        
        for condition, stats in self.stats['conditions_stats'].items():
            passed = stats['passed']
            failed = stats['failed']
            total = passed + failed
            if total > 0:
                pass_rate = passed / total * 100
                self.log_message(f"{condition}:", "INFO")
                self.log_message(f"  通過: {passed}/{total} ({pass_rate:.1f}%)", "DETAIL")
    
    def update_stats_display(self):
        """更新統計顯示"""
        self.root.after(0, lambda: self.stats_label.config(
            text=f"處理: {self.stats['processed']}/{self.stats['total_stocks']} | 符合: {self.stats['passed']}"
        ))
    
    def get_screening_params(self):
        """取得篩選參數"""
        params = {
            # 篩選門檻
            'min_conditions_to_pass': self.min_conditions_value.get(),
            
            # 市場範圍
            'market_twse': self.market_twse.get(),
            'market_otc': self.market_otc.get(),
            'market_emerging': self.market_emerging.get(),
            
            # 成交量條件
            'volume_surge1': {
                'enabled': self.volume_surge1_enabled.get(),
                'value': self.volume_surge1_value.get()
            },
            'volume_surge2': {
                'enabled': self.volume_surge2_enabled.get(),
                'value': self.volume_surge2_value.get()
            },
            'volume_surge3': {
                'enabled': self.volume_surge3_enabled.get(),
                'value': self.volume_surge3_value.get()
            },
            'min_volume': {
                'enabled': self.min_volume_enabled.get(),
                'value': self.min_volume_value.get()
            },
            
            # 技術指標
            'daily_kd_golden': self.daily_kd_golden.get(),
            'monthly_kd_golden': self.monthly_kd_golden.get(),
            'above_ma20': self.above_ma20.get(),
            'break_60d_high': self.break_60d_high.get(),
            'break_volume_multiplier': self.break_volume_multiplier.get(),
            
            # 法人籌碼
            'trust_buy': {
                'enabled': self.trust_buy_enabled.get(),
                'value': self.trust_buy_value.get()
            },
            'trust_pct': {
                'enabled': self.trust_pct_enabled.get(),
                'value': self.trust_pct_value.get()
            },
            'trust_5d': {
                'enabled': self.trust_5d_enabled.get(),
                'value': self.trust_5d_value.get()
            },
            'trust_holding': {
                'enabled': self.trust_holding_enabled.get(),
                'value': self.trust_holding_value.get()
            },
            'inst_5d': {
                'enabled': self.inst_5d_enabled.get(),
                'value': self.inst_5d_value.get()
            },
            
            # 融資融券
            'margin_ratio': {
                'enabled': self.margin_ratio_enabled.get(),
                'value': self.margin_ratio_value.get()
            },
            'margin_5d': {
                'enabled': self.margin_5d_enabled.get(),
                'value': self.margin_5d_value.get()
            },
            
            # 基本面
            'eps': {
                'enabled': self.eps_enabled.get(),
                'value': self.eps_value.get()
            },
            'roe': {
                'enabled': self.roe_enabled.get(),
                'value': self.roe_value.get()
            },
            'yield': {
                'enabled': self.yield_enabled.get(),
                'value': self.yield_value.get()
            },
            
            # 漲幅控制
            'daily_change': {
                'enabled': self.daily_change_enabled.get(),
                'value': self.daily_change_value.get()
            },
            'change_5d': {
                'enabled': self.change_5d_enabled.get(),
                'value': self.change_5d_value.get()
            },
            
            # 排除條件
            'exclude_warning': self.exclude_warning.get(),
            'exclude_disposition': self.exclude_disposition.get(),
            'exclude_limit_up': {
                'enabled': self.exclude_limit_up_enabled.get(),
                'days': self.exclude_limit_up_days.get()
            }
        }
        
        return params
    
    def add_result_to_table(self, result, rank):
        """新增結果到表格"""
        # 從結果中提取顯示資料
        stock_data = result.get('data', {})
        price_data = stock_data.get('price', pd.DataFrame())
        
        # 取得最新價格資料
        if not price_data.empty and isinstance(price_data, pd.DataFrame):
            latest_price = price_data.iloc[-1]['close'] if 'close' in price_data.columns else 0
            latest_volume = price_data.iloc[-1]['Trading_Volume'] if 'Trading_Volume' in price_data.columns else 0
            
            # 計算漲跌幅
            if len(price_data) > 1:
                prev_close = price_data.iloc[-2]['close'] if 'close' in price_data.columns else latest_price
                change_pct = ((latest_price - prev_close) / prev_close * 100) if prev_close > 0 else 0
            else:
                change_pct = 0
        else:
            latest_price = 0
            latest_volume = 0
            change_pct = 0
        
        # 從 check_result['values'] 回填關鍵值
        values = result.get('check_result', {}).get('values', {})
        
        # 爆量倍數 (優先取 volume_surge1_ratio，其次 volume_surge2/3)
        surge_ratio = values.get('volume_surge_1_5x')
        if surge_ratio and ':' in str(surge_ratio):
            # 從 "爆量倍數: 1.72x (門檻: 1.5x)" 提取數值
            try:
                surge_ratio = float(str(surge_ratio).split(':')[1].split('x')[0].strip())
            except:
                surge_ratio = None
        
        if not surge_ratio:
            surge_ratio = values.get('volume_surge1_ratio') or values.get('volume_surge_ratio')
        
        # KD 值
        kd_str = values.get('daily_kd_golden', '')
        kdK = None
        kdD = None
        if 'K=' in str(kd_str) and 'D=' in str(kd_str):
            try:
                # 從 "K=74.9, D=74.3" 提取數值
                parts = str(kd_str).split(',')
                kdK = float(parts[0].split('=')[1].strip())
                kdD = float(parts[1].split('=')[1].strip())
            except:
                pass
        
        if not kdK:
            kdK = values.get('kd_K')
        if not kdD:
            kdD = values.get('kd_D')
        
        # 投信買超
        trust_str = values.get('trust_buy', '')
        trust_buy_lots = None
        if '投信買超:' in str(trust_str):
            try:
                # 從 "投信買超: 326張 (門檻: 500張)" 提取數值
                trust_buy_lots = float(str(trust_str).split(':')[1].split('張')[0].strip())
            except:
                pass
        
        if not trust_buy_lots:
            trust_buy_lots = values.get('trust_buy_lots')
        
        # 格式化顯示
        surge_text = f"{surge_ratio:.2f}" if isinstance(surge_ratio, (int, float)) else "-"
        k_text = f"{kdK:.1f}" if isinstance(kdK, (int, float)) else "-"
        d_text = f"{kdD:.1f}" if isinstance(kdD, (int, float)) else "-"
        trust_text = f"{int(trust_buy_lots):,}" if isinstance(trust_buy_lots, (int, float)) else "-"
        
        # 準備顯示資料
        display_data = [
            rank,
            result['stock_id'],
            result['stock_name'],
            f"{result['score']:.1f}",
            f"{latest_price:.2f}",
            f"{change_pct:.2f}%",
            f"{int(latest_volume/1000):,}" if latest_volume > 0 else "0",
            surge_text,  # 爆量倍數
            k_text,      # K值
            d_text,      # D值
            trust_text,  # 投信買超（張）
            result['matched_conditions'],
            result['signal']
        ]
        
        # 插入表格
        self.root.after(0, lambda: self.result_tree.insert('', 'end', values=display_data))
        
        # 更新符合條件數
        self.root.after(0, lambda: self.match_label.config(text=f"符合條件: {rank} 檔"))
    
    def stop_screening_process(self):
        """停止篩選"""
        self.stop_screening = True
        self.btn_stop.config(state='disabled')
        self.log_message("使用者中止篩選", "WARNING")
        self.update_status("已停止")
    
    def export_to_excel(self):
        """匯出到 Excel"""
        if not self.results and self.result_tree.get_children():
            # 從表格收集資料
            self.results = []
            for item in self.result_tree.get_children():
                values = self.result_tree.item(item)['values']
                self.results.append({
                    '排名': values[0],
                    '股票代碼': values[1],
                    '股票名稱': values[2],
                    '潛力分數': values[3],
                    '收盤價': values[4],
                    '漲跌幅%': values[5],
                    '成交量': values[6],
                    '爆量倍數': values[7],
                    'K值': values[8],
                    'D值': values[9],
                    '投信買超': values[10],
                    '符合條件': values[11],
                    '關鍵信號': values[12]
                })
        
        if not self.results:
            messagebox.showwarning("警告", "沒有資料可匯出")
            return
        
        # 選擇儲存路徑
        filename = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=f"screening_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        
        if filename:
            try:
                # 建立 Excel 檔案
                df = pd.DataFrame(self.results)
                
                # 使用 ExcelWriter 加入格式
                with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                    df.to_excel(writer, sheet_name='篩選結果', index=False)
                    
                    # 取得工作表
                    workbook = writer.book
                    worksheet = writer.sheets['篩選結果']
                    
                    # 設定格式
                    header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'vcenter',
                        'align': 'center',
                        'fg_color': '#D7E4BD',
                        'border': 1
                    })
                    
                    # 套用格式到標題列
                    for col_num, value in enumerate(df.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                    
                    # 調整欄寬
                    worksheet.set_column('A:A', 8)   # 排名
                    worksheet.set_column('B:B', 12)  # 股票代碼
                    worksheet.set_column('C:C', 15)  # 股票名稱
                    worksheet.set_column('D:M', 12)  # 其他欄位
                    worksheet.set_column('M:M', 20)  # 關鍵信號
                    
                    # 凍結窗格
                    worksheet.freeze_panes(1, 0)
                
                messagebox.showinfo("成功", f"已匯出至:\n{filename}")
                self.log_message(f"已匯出 Excel: {filename}", "SUCCESS")
                
            except Exception as e:
                messagebox.showerror("錯誤", f"匯出失敗: {e}")
                self.log_message(f"匯出失敗: {e}", "ERROR")
    
    def save_settings(self):
        """儲存設定"""
        try:
            # 更新 API 設定
            self.api_config['finmind']['api_token'] = self.token_var.get()
            
            # 儲存 API 設定
            with open('api_config.json', 'w', encoding='utf-8') as f:
                json.dump(self.api_config, f, indent=2, ensure_ascii=False)
            
            # 更新並儲存篩選參數
            params = self.get_screening_params()
            self.config['screening_parameters'] = params
            
            with open('config.json', 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            
            messagebox.showinfo("成功", "設定已儲存")
            self.log_message("設定已儲存", "SUCCESS")
            
        except Exception as e:
            messagebox.showerror("錯誤", f"儲存失敗: {e}")
            self.log_message(f"儲存設定失敗: {e}", "ERROR")
    
    def show_help(self):
        """顯示說明"""
        help_text = """
台股智能篩選系統 v2.0 - Enhanced Edition
==========================================

【新功能】
1. 詳細日誌模式：顯示每檔股票的條件檢查結果
2. 測試模式：只篩選前 10 檔股票
3. 快速設定：寬鬆、標準、嚴格模式
4. 可調整最少符合條件數門檻
5. 條件統計：顯示各條件通過率

【使用步驟】
1. 選擇快速設定或自訂條件
2. 設定最少符合條件數（預設 5）
3. 開啟「詳細日誌」查看檢查過程
4. 使用「測試模式」快速測試
5. 點擊「立即篩選」開始

【模式說明】
- 寬鬆模式：最少 3 個條件，基本篩選
- 標準模式：最少 5 個條件，平衡篩選
- 嚴格模式：最少 10 個條件，精準篩選

【提示】
- 條件太多可能導致沒有股票符合
- 建議從寬鬆模式開始測試
- 查看日誌了解哪些條件最難通過
        """
        
        # 建立說明視窗
        help_window = tk.Toplevel(self.root)
        help_window.title("使用說明")
        help_window.geometry("600x500")
        
        text_widget = tk.Text(help_window, wrap=tk.WORD, padx=10, pady=10)
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert('1.0', help_text)
        text_widget.config(state='disabled')
        
        ttk.Button(help_window, text="關閉", command=help_window.destroy).pack(pady=10)
    
    def show_stock_details(self, event=None):
        """顯示股票詳情"""
        selection = self.result_tree.selection()
        if not selection:
            return
        
        item = self.result_tree.item(selection[0])
        values = item['values']
        
        details = f"""
股票代碼: {values[1]}
股票名稱: {values[2]}
潛力分數: {values[3]}
收盤價: {values[4]}
漲跌幅: {values[5]}
成交量: {values[6]}
爆量倍數: {values[7]}
K值: {values[8]}
D值: {values[9]}
投信買超: {values[10]}
符合條件數: {values[11]}
關鍵信號: {values[12]}
        """
        
        messagebox.showinfo("股票詳情", details)
    
    def copy_stock_code(self):
        """複製股票代碼"""
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item['values'][1]
            self.root.clipboard_clear()
            self.root.clipboard_append(stock_code)
            self.log_message(f"已複製股票代碼: {stock_code}", "INFO")
    
    def add_to_watchlist(self):
        """加入自選股"""
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item['values'][1]
            stock_name = item['values'][2]
            # 這裡可以實作自選股功能
            messagebox.showinfo("成功", f"已將 {stock_code} {stock_name} 加入自選股")
    
    def export_selected(self):
        """匯出選取項目"""
        selection = self.result_tree.selection()
        if not selection:
            messagebox.showwarning("警告", "請先選取要匯出的項目")
            return
        
        # 收集選取的資料
        selected_data = []
        for item in selection:
            values = self.result_tree.item(item)['values']
            selected_data.append({
                '排名': values[0],
                '股票代碼': values[1],
                '股票名稱': values[2],
                '潛力分數': values[3],
                '收盤價': values[4],
                '漲跌幅%': values[5],
                '成交量': values[6],
                '關鍵信號': values[12]
            })
        
        # 匯出到 CSV
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"selected_stocks_{datetime.now().strftime('%Y%m%d')}.csv"
        )
        
        if filename:
            try:
                df = pd.DataFrame(selected_data)
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                messagebox.showinfo("成功", f"已匯出 {len(selected_data)} 筆資料")
            except Exception as e:
                messagebox.showerror("錯誤", f"匯出失敗: {e}")
    
    def sort_results(self, column):
        """排序結果"""
        # 取得目前所有資料
        items = [(self.result_tree.set(k, column), k) for k in self.result_tree.get_children('')]
        
        # 嘗試以數值排序，失敗則以字串排序
        def to_num(v):
            try:
                # 移除百分號和逗號
                cleaned = str(v).replace('%', '').replace(',', '')
                return float(cleaned)
            except:
                return float('inf')  # 非數值放最後
        
        # 判斷是否為數值欄位
        try:
            # 檢查第一個非空值
            for val, _ in items:
                if val and val != '-':
                    to_num(val)  # 嘗試轉換
                    numeric = True
                    break
            else:
                numeric = False
        except:
            numeric = False
        
        # 排序
        if numeric:
            items.sort(key=lambda t: to_num(t[0]))
        else:
            items.sort(key=lambda t: t[0])
        
        # 切換升降序
        if not hasattr(self, '_sort_desc'):
            self._sort_desc = {}
        
        if column not in self._sort_desc:
            self._sort_desc[column] = False
        else:
            self._sort_desc[column] = not self._sort_desc[column]
        
        if self._sort_desc[column]:
            items.reverse()
        
        # 重新排列
        for index, (_, k) in enumerate(items):
            self.result_tree.move(k, '', index)
    
    def update_progress(self, value, text=""):
        """更新進度條"""
        self.root.after(0, lambda: self.progress_bar.config(value=value))
        self.root.after(0, lambda: self.progress_text.config(text=f"{value:.1f}%"))
        if text:
            self.root.after(0, lambda: self.progress_label.config(text=text))
    
    def update_status(self, text):
        """更新狀態"""
        self.root.after(0, lambda: self.status_label.config(text=text))
    
    def update_statistics(self, results):
        """更新統計資訊"""
        if not results:
            return
        
        # 計算統計
        stats_text = f"""
篩選統計報告
{'='*50}
篩選時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
總處理股票數: {self.stats['total_stocks']}
符合條件股票數: {len(results)}
通過率: {len(results)/self.stats['total_stocks']*100:.1f}%

分數分布:
A+ (90-100分): {len([r for r in results if r['score'] >= 90])} 檔
A  (80-90分):  {len([r for r in results if 80 <= r['score'] < 90])} 檔
B+ (70-80分):  {len([r for r in results if 70 <= r['score'] < 80])} 檔
B  (60-70分):  {len([r for r in results if 60 <= r['score'] < 70])} 檔
C  (60分以下): {len([r for r in results if r['score'] < 60])} 檔

平均分數: {sum(r['score'] for r in results) / len(results):.1f}
最高分數: {max(r['score'] for r in results):.1f}
最低分數: {min(r['score'] for r in results):.1f}

前五名股票:
"""
        for i, result in enumerate(results[:5], 1):
            stats_text += f"{i}. {result['stock_id']} {result['stock_name']} - {result['score']:.1f}分\n"
        
        # 更新統計區域
        self.root.after(0, lambda: self.stats_text.delete('1.0', tk.END))
        self.root.after(0, lambda: self.stats_text.insert('1.0', stats_text))
    
    def log_message(self, message, level="INFO"):
        """記錄日誌訊息"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}\n"
        
        # 插入到日誌區域
        self.root.after(0, lambda: self.log_text.insert(tk.END, log_entry, level))
        self.root.after(0, lambda: self.log_text.see(tk.END))
    
    def update_time(self):
        """更新時間顯示"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.config(text=current_time)
        self.root.after(1000, self.update_time)


def main():
    """主程式"""
    root = tk.Tk()
    app = StockScreenerEnhanced(root)
    root.mainloop()


if __name__ == "__main__":
    main()