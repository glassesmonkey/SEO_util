import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import logging
from urllib.parse import quote
import random
from threading import Thread
import os
import json
import schedule
import threading
import sys

class Config:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self):
        """加载配置文件"""
        default_config = {
            "last_file_path": "",
            "last_csv_path": "",
            "proxy_enabled": False,
            "proxy_host": "127.0.0.1",
            "proxy_port": "7890",
            "time_range": "24h",
            "scheduler_enabled": False,
            "schedule_time": "09:00",
            "use_existing_csv": False
        }
        
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return {**default_config, **json.load(f)}
            return default_config
        except Exception as e:
            print(f"Error loading config: {e}")
            return default_config

    def save_config(self):
        """保存配置文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving config: {e}")

class ScheduleManager:
    def __init__(self, callback):
        self.callback = callback
        self.running = False
        self.thread = None

    def start(self, time_str):
        """启动定时任务"""
        if self.running:
            return
            
        self.running = True
        schedule.clear()
        schedule.every().day.at(time_str).do(self.callback)
        
        self.thread = threading.Thread(target=self._run_schedule, daemon=True)
        self.thread.start()

    def stop(self):
        """停止定时任务"""
        self.running = False
        schedule.clear()
        if self.thread:
            self.thread = None

    def _run_schedule(self):
        """运行定时任务循环"""
        while self.running:
            schedule.run_pending()
            time.sleep(30)  # 每30秒检查一次

class GameMonitorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("游戏网站监控工具")
        self.root.geometry("600x800")
        
        # 加载配置
        self.config = Config()
        
        # 初始化定时管理器
        self.schedule_manager = ScheduleManager(self.scheduled_monitoring)
        
        # 设置样式
        self.setup_styles()
        self.setup_gui()
        self.setup_logging()
        
        # 加载保存的配置
        self.load_saved_config()
        
        # 绑定窗口关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        style.configure('TButton', padding=5)
        style.configure('TLabel', padding=5)
        style.configure('TFrame', padding=5)
        style.configure('Header.TLabel', font=('Helvetica', 10, 'bold'))
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('game_monitor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging

    def setup_gui(self):
        """设置GUI界面"""
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 文件选择部分
        file_frame = ttk.LabelFrame(main_frame, text="文件选择", padding=5)
        file_frame.pack(fill=tk.X, pady=5)

        # 网站列表文件选择
        site_file_frame = ttk.Frame(file_frame)
        site_file_frame.pack(fill=tk.X, pady=2)
        ttk.Label(site_file_frame, text="网站列表文件:").pack(side=tk.LEFT)
        self.file_path = tk.StringVar()
        ttk.Entry(site_file_frame, textvariable=self.file_path, width=40).pack(side=tk.LEFT, padx=5)
        ttk.Button(site_file_frame, text="浏览", command=self.browse_site_file).pack(side=tk.LEFT, padx=5)

        # CSV文件选择
        csv_file_frame = ttk.Frame(file_frame)
        csv_file_frame.pack(fill=tk.X, pady=2)
        self.use_existing_csv = tk.BooleanVar()
        ttk.Checkbutton(csv_file_frame, text="使用现有CSV文件", 
                       variable=self.use_existing_csv,
                       command=self.toggle_csv_fields).pack(side=tk.LEFT)
        self.csv_path = tk.StringVar()
        self.csv_entry = ttk.Entry(csv_file_frame, textvariable=self.csv_path, width=40)
        self.csv_entry.pack(side=tk.LEFT, padx=5)
        self.csv_button = ttk.Button(csv_file_frame, text="浏览", command=self.browse_csv_file)
        self.csv_button.pack(side=tk.LEFT, padx=5)

        # 代理设置部分
        proxy_frame = ttk.LabelFrame(main_frame, text="代理设置", padding=5)
        proxy_frame.pack(fill=tk.X, pady=5)

        # 代理启用选项
        proxy_enable_frame = ttk.Frame(proxy_frame)
        proxy_enable_frame.pack(fill=tk.X)
        self.proxy_enabled = tk.BooleanVar()
        ttk.Checkbutton(proxy_enable_frame, text="启用代理", 
                       variable=self.proxy_enabled,
                       command=self.toggle_proxy_fields).pack(side=tk.LEFT)

        # 代理设置框架
        self.proxy_settings_frame = ttk.Frame(proxy_frame)
        self.proxy_settings_frame.pack(fill=tk.X)

        # 代理地址输入
        proxy_addr_frame = ttk.Frame(self.proxy_settings_frame)
        proxy_addr_frame.pack(fill=tk.X)
        ttk.Label(proxy_addr_frame, text="代理地址:").pack(side=tk.LEFT)
        self.proxy_host = tk.StringVar()
        self.proxy_host_entry = ttk.Entry(proxy_addr_frame, textvariable=self.proxy_host, width=30)
        self.proxy_host_entry.pack(side=tk.LEFT, padx=5)

        # 代理端口输入
        proxy_port_frame = ttk.Frame(self.proxy_settings_frame)
        proxy_port_frame.pack(fill=tk.X)
        ttk.Label(proxy_port_frame, text="代理端口:").pack(side=tk.LEFT)
        self.proxy_port = tk.StringVar()
        self.proxy_port_entry = ttk.Entry(proxy_port_frame, textvariable=self.proxy_port, width=10)
        self.proxy_port_entry.pack(side=tk.LEFT, padx=5)

        # 定时任务设置
        schedule_frame = ttk.LabelFrame(main_frame, text="定时任务设置", padding=5)
        schedule_frame.pack(fill=tk.X, pady=5)
        
        self.scheduler_enabled = tk.BooleanVar()
        ttk.Checkbutton(schedule_frame, text="启用定时任务", 
                       variable=self.scheduler_enabled,
                       command=self.toggle_schedule_fields).pack(side=tk.LEFT)
        
        self.schedule_time = tk.StringVar(value="09:00")
        self.schedule_time_entry = ttk.Entry(schedule_frame, 
                                           textvariable=self.schedule_time, 
                                           width=10)
        self.schedule_time_entry.pack(side=tk.LEFT, padx=5)
        ttk.Label(schedule_frame, text="（格式: HH:MM）").pack(side=tk.LEFT)

        # 时间范围选择
        time_frame = ttk.LabelFrame(main_frame, text="搜索时间范围", padding=5)
        time_frame.pack(fill=tk.X, pady=5)

        self.time_range = tk.StringVar()
        ttk.Radiobutton(time_frame, text="最近24小时", variable=self.time_range, 
                       value="24h").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(time_frame, text="最近一周", variable=self.time_range, 
                       value="1w").pack(side=tk.LEFT, padx=5)

        # 开始按钮
        self.start_button = ttk.Button(main_frame, text="开始监控", command=self.start_monitoring)
        self.start_button.pack(pady=10)

        # 进度显示
        self.progress_var = tk.StringVar(value="准备就绪")
        ttk.Label(main_frame, textvariable=self.progress_var).pack(pady=5)

        # 结果显示区域
        result_frame = ttk.LabelFrame(main_frame, text="监控结果", padding=5)
        result_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # 创建文本框和滚动条
        self.result_text = tk.Text(result_frame, height=20, width=70)
        scrollbar = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.result_text.yview)
        self.result_text.configure(yscrollcommand=scrollbar.set)
        
        self.result_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    def load_saved_config(self):
       
        self.file_path.set(self.config.config["last_file_path"])
        self.csv_path.set(self.config.config["last_csv_path"])
        self.use_existing_csv.set(self.config.config["use_existing_csv"])
        self.proxy_enabled.set(self.config.config["proxy_enabled"])
        self.proxy_host.set(self.config.config["proxy_host"])
        self.proxy_port.set(self.config.config["proxy_port"])
        self.time_range.set(self.config.config["time_range"])
        self.scheduler_enabled.set(self.config.config["scheduler_enabled"])
        self.schedule_time.set(self.config.config["schedule_time"])
        
        # 更新各个字段状态
        self.toggle_proxy_fields()
        self.toggle_csv_fields()
        self.toggle_schedule_fields()

        # 如果定时任务被启用，启动定时任务
        if self.scheduler_enabled.get():
            self.schedule_manager.start(self.schedule_time.get())

    def save_current_config(self):
        """保存当前配置"""
        self.config.config.update({
            "last_file_path": self.file_path.get(),
            "last_csv_path": self.csv_path.get(),
            "use_existing_csv": self.use_existing_csv.get(),
            "proxy_enabled": self.proxy_enabled.get(),
            "proxy_host": self.proxy_host.get(),
            "proxy_port": self.proxy_port.get(),
            "time_range": self.time_range.get(),
            "scheduler_enabled": self.scheduler_enabled.get(),
            "schedule_time": self.schedule_time.get()
        })
        self.config.save_config()

    def toggle_proxy_fields(self):
        """切换代理设置字段的启用状态"""
        state = 'normal' if self.proxy_enabled.get() else 'disabled'
        self.proxy_host_entry.configure(state=state)
        self.proxy_port_entry.configure(state=state)

    def toggle_csv_fields(self):
        """切换CSV文件字段的启用状态"""
        state = 'normal' if self.use_existing_csv.get() else 'disabled'
        self.csv_entry.configure(state=state)
        self.csv_button.configure(state=state)

    def toggle_schedule_fields(self):
        """切换定时任务字段的启用状态"""
        state = 'normal' if self.scheduler_enabled.get() else 'disabled'
        self.schedule_time_entry.configure(state=state)
        
        if self.scheduler_enabled.get():
            self.schedule_manager.start(self.schedule_time.get())
        else:
            self.schedule_manager.stop()

    def browse_site_file(self):
        """浏览网站列表文件"""
        filename = filedialog.askopenfilename(
            title="选择网站列表文件",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.file_path.get()) if self.file_path.get() else os.getcwd()
        )
        if filename:
            self.file_path.set(filename)
            self.save_current_config()

    def browse_csv_file(self):
        """浏览CSV文件"""
        filename = filedialog.askopenfilename(
            title="选择CSV文件",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.csv_path.get()) if self.csv_path.get() else os.getcwd()
        )
        if filename:
            self.csv_path.set(filename)
            self.save_current_config()

    def on_closing(self):
        """窗口关闭时的处理"""
        self.save_current_config()
        self.schedule_manager.stop()
        self.root.destroy()

    def update_progress(self, message):
        """更新进度显示"""
        self.progress_var.set(message)
        self.result_text.insert(tk.END, message + "\n")
        self.result_text.see(tk.END)

    def scheduled_monitoring(self):
        """定时任务执行的监控函数"""
        self.root.after(0, lambda: self.start_button.configure(state='disabled'))
        self.root.after(0, lambda: self.result_text.delete(1.0, tk.END))
        self.root.after(0, lambda: self.update_progress("开始执行定时监控任务..."))
        
        Thread(target=self.run_monitor, daemon=True).start()

    def start_monitoring(self):
        """开始监控"""
        if not self.file_path.get():
            messagebox.showerror("错误", "请选择网站列表文件！")
            return

        if not os.path.exists(self.file_path.get()):
            messagebox.showerror("错误", "所选文件不存在！")
            return

        if self.use_existing_csv.get() and not self.csv_path.get():
            messagebox.showerror("错误", "请选择CSV文件！")
            return

        # 保存当前配置
        self.save_current_config()

        # 禁用开始按钮
        self.start_button.configure(state='disabled')
        
        # 清空结果显示
        self.result_text.delete(1.0, tk.END)
        
        # 在新线程中运行监控
        Thread(target=self.run_monitor, daemon=True).start()

    def run_monitor(self):
        """运行监控任务"""
        try:
            proxy_host = self.proxy_host.get() if self.proxy_enabled.get() else None
            proxy_port = self.proxy_port.get() if self.proxy_enabled.get() else None
            
            monitor = GameSiteMonitor(
                sites_file=self.file_path.get(),
                proxy_host=proxy_host,
                proxy_port=proxy_port,
                logger_callback=self.update_progress,
                existing_csv=self.csv_path.get() if self.use_existing_csv.get() else None
            )
            
            results_df = monitor.monitor_all_sites([self.time_range.get()])
            
            if not results_df.empty:
                self.update_progress("\n=== 监控统计 ===")
                self.update_progress(f"总计发现新页面: {len(results_df)}")
                self.update_progress("\n按网站统计:")
                self.update_progress(str(results_df['site'].value_counts()))
                self.update_progress("\n按时间范围统计:")
                self.update_progress(str(results_df['time_range'].value_counts()))
                
                # 显示保存位置
                self.update_progress(f"\n结果已保存至: {os.path.abspath(monitor.last_output_file)}")
            else:
                self.update_progress("未找到任何结果")
        except Exception as e:
            self.update_progress(f"发生错误: {str(e)}")
        finally:
            # 重新启用开始按钮
            self.root.after(0, lambda: self.start_button.configure(state='normal'))

class GameSiteMonitor:
    def __init__(self, sites_file="game_sites.txt", proxy_host=None, proxy_port=None, logger_callback=None, existing_csv=None):
        """
        初始化监控器
        :param sites_file: 网站列表文件
        :param proxy_host: 代理主机
        :param proxy_port: 代理端口
        :param logger_callback: 日志回调函数
        :param existing_csv: 现有的CSV文件路径
        """
        self.sites = self._load_sites(sites_file)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 设置代理
        self.proxies = None
        if proxy_host and proxy_port:
            self.proxies = {
                'http': f'http://{proxy_host}:{proxy_port}',
                'https': f'http://{proxy_host}:{proxy_port}'
            }
        
        self.logger_callback = logger_callback
        self.last_output_file = None
        self.existing_csv = existing_csv
        self.existing_urls = set()
        
        if existing_csv and os.path.exists(existing_csv):
            self._load_existing_urls()
            
        self.setup_logging()
        
        # 添加常用 User-Agent 列表
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Edge/119.0.0.0'
        ]

    def _load_existing_urls(self):
        """加载现有CSV文件中的URL"""
        try:
            df = pd.read_csv(self.existing_csv, encoding='utf-8-sig')
            if 'url' in df.columns:
                self.existing_urls = set(df['url'].tolist())
        except Exception as e:
            self.log_message(f"Error loading existing CSV: {str(e)}")

    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('game_monitor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging

    def _load_sites(self, filename):
        """加载网站列表"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            raise Exception(f"Sites file {filename} not found!")
        except Exception as e:
            raise Exception(f"Error loading sites file: {str(e)}")

    def log_message(self, message):
        """统一的日志记录函数"""
        if self.logger_callback:
            self.logger_callback(message)
        self.logger.info(message)

    def build_google_search_url(self, site, time_range):
        """构建Google搜索URL"""
        base_url = "https://www.google.com/search"
        if time_range == '24h':
            tbs = 'qdr:d'
        elif time_range == '1w':
            tbs = 'qdr:w'
        else:
            raise ValueError("Invalid time range")
        
        query = f'site:{site}'
        params = {
            'q': query,
            'tbs': tbs,
            'num': 100
        }
        
        query_string = '&'.join([f'{k}={quote(str(v))}' for k, v in params.items()])
        return f"{base_url}?{query_string}"

    def extract_search_results(self, html_content):
        """从Google搜索结果页面提取信息"""
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        
        for result in soup.select('div.g'):
            try:
                title_elem = result.select_one('h3')
                url_elem = result.select_one('a')
                
                if title_elem and url_elem:
                    title = title_elem.get_text()
                    url = url_elem['href']
                    
                    # URL去重检查
                    if url in self.existing_urls:
                        continue
                        
                    game_name = self.extract_game_name(title)
                    
                    if game_name:
                        results.append({
                            'title': title,
                            'url': url,
                            'game_name': game_name
                        })
            except Exception as e:
                self.log_message(f"Error extracting result: {str(e)}")
                
        return results

    def extract_game_name(self, title):
        """从标题中提取游戏名称"""
        patterns = [
            r'《(.+?)》',
            r'"(.+?)"',
            r'【(.+?)】',
            r'\[(.+?)\]'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                return match.group(1)
        
        cleaned_title = re.sub(r'(攻略|评测|资讯|下载|官网|专区|合集|手游|网游|页游|主机游戏|单机游戏)', '', title)
        return cleaned_title.strip()

    def monitor_site(self, site, time_range):
        """监控单个网站"""
        search_url = self.build_google_search_url(site, time_range)
        self.log_message(f"Monitoring {site} for {time_range} timeframe")
        
        try:
            max_retries = 3
            retry_delay = 60
            
            for attempt in range(max_retries):
                # 每次请求使用随机的 User-Agent
                headers = {
                    'User-Agent': random.choice(self.user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
                
                response = requests.get(
                    search_url, 
                    headers=headers,  # 使用新的headers
                    proxies=self.proxies,
                    timeout=30
                )
                
                if response.status_code == 200:
                    results = self.extract_search_results(response.text)
                    self.log_message(f"Found {len(results)} results for {site}")
                    return results
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        self.log_message(f"Rate limited for {site}, waiting {retry_delay} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # 指数退避
                    else:
                        self.log_message(f"Failed to fetch results for {site} after {max_retries} retries")
                        return []
                else:
                    self.log_message(f"Failed to fetch results for {site}: Status code {response.status_code}")
                    return []
                    
            return []
        except Exception as e:
            self.log_message(f"Error monitoring {site}: {str(e)}")
            return []

    def monitor_all_sites(self, time_ranges=None):
        """监控所有网站"""
        if time_ranges is None:
            time_ranges = ['24h', '1w']
            
        all_results = []
        
        for site in self.sites:
            for time_range in time_ranges:
                results = self.monitor_site(site, time_range)
                for result in results:
                    result.update({
                        'site': site,
                        'time_range': time_range,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                all_results.extend(results)
                
                # 增加随机延迟，避免触发限制
                delay = random.uniform(5, 15)  # 5-15秒的随机延迟
                time.sleep(delay)
        
        if all_results:
            df = pd.DataFrame(all_results)
            
            # 如果使用现有CSV文件，则追加到现有文件
            if self.existing_csv and os.path.exists(self.existing_csv):
                existing_df = pd.read_csv(self.existing_csv, encoding='utf-8-sig')
                df = pd.concat([existing_df, df], ignore_index=True)
                self.last_output_file = self.existing_csv
            else:
                self.last_output_file = f'game_monitor_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            
            df.to_csv(self.last_output_file, index=False, encoding='utf-8-sig')
            self.log_message(f"Results saved to {self.last_output_file}")
            return df
        else:
            self.log_message("No results found")
            return pd.DataFrame()

def main():
    root = tk.Tk()
    app = GameMonitorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()