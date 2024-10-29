import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import logging
from urllib.parse import quote
import random

class GameSiteMonitor:
    def __init__(self, sites_file="game_sites.txt"):
        """
        初始化监控器
        :param sites_file: 包含游戏网站列表的文本文件
        """
        self.sites = self._load_sites(sites_file)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.proxies = {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890'
        }
        self.setup_logging()

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

    def build_google_search_url(self, site, time_range):
        """
        构建Google搜索URL
        :param site: 网站域名
        :param time_range: 时间范围('24h' or '1w')
        :return: 编码后的搜索URL
        """
        base_url = "https://www.google.com/search"
        if time_range == '24h':
            tbs = 'qdr:d'  # 最近24小时
        elif time_range == '1w':
            tbs = 'qdr:w'  # 最近1周
        else:
            raise ValueError("Invalid time range")
        
        query = f'site:{site}'
        params = {
            'q': query,
            'tbs': tbs,
            'num': 100  # 每页结果数
        }
        
        query_string = '&'.join([f'{k}={quote(str(v))}' for k, v in params.items()])
        return f"{base_url}?{query_string}"

    def extract_search_results(self, html_content):
        """
        从Google搜索结果页面提取信息
        :param html_content: 页面HTML内容
        :return: 提取的URL和标题列表
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        
        # 查找搜索结果
        for result in soup.select('div.g'):
            try:
                title_elem = result.select_one('h3')
                url_elem = result.select_one('a')
                
                if title_elem and url_elem:
                    title = title_elem.get_text()
                    url = url_elem['href']
                    
                    # 提取可能的游戏名称
                    game_name = self.extract_game_name(title)
                    
                    if game_name:
                        results.append({
                            'title': title,
                            'url': url,
                            'game_name': game_name
                        })
            except Exception as e:
                self.logger.error(f"Error extracting result: {str(e)}")
                
        return results

    def extract_game_name(self, title):
        """
        从标题中提取可能的游戏名称
        :param title: 页面标题
        :return: 提取的游戏名称或None
        """
        # 这里可以根据具体网站的标题特征来优化游戏名称提取规则
        patterns = [
            r'《(.+?)》',  # 中文书名号
            r'"(.+?)"',    # 英文引号
            r'【(.+?)】',  # 中文方括号
            r'\[(.+?)\]'   # 英文方括号
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                return match.group(1)
        
        # 如果没有特定标记，返回清理后的标题
        cleaned_title = re.sub(r'(攻略|评测|资讯|下载|官网|专区|合集|手游|网游|页游|主机游戏|单机游戏)', '', title)
        return cleaned_title.strip()

    def monitor_site(self, site, time_range):
        """
        监控单个网站
        :param site: 网站域名
        :param time_range: 时间范围
        :return: 搜索结果列表
        """
        search_url = self.build_google_search_url(site, time_range)
        self.logger.info(f"Monitoring {site} for {time_range} timeframe")
        
        try:
            response = requests.get(
                search_url, 
                headers=self.headers,
                proxies=self.proxies,
                timeout=30  # 添加超时设置
            )
            if response.status_code == 200:
                results = self.extract_search_results(response.text)
                self.logger.info(f"Found {len(results)} results for {site}")
                return results
            else:
                self.logger.error(f"Failed to fetch results for {site}: Status code {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error monitoring {site}: {str(e)}")
            return []

    def monitor_all_sites(self, time_ranges=None):
        """
        监控所有网站
        :param time_ranges: 时间范围列表
        :return: 包含所有结果的DataFrame
        """
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
                
                # 随机延时，避免请求过快
                time.sleep(random.uniform(2, 5))
        
        # 转换为DataFrame并保存
        if all_results:
            df = pd.DataFrame(all_results)
            output_file = f'game_monitor_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            self.logger.info(f"Results saved to {output_file}")
            return df
        else:
            self.logger.warning("No results found")
            return pd.DataFrame()

def main():
    """主函数"""
    # 创建监控器实例
    monitor = GameSiteMonitor()
    
    # 开始监控
    results_df = monitor.monitor_all_sites()
    
    # 输出统计信息
    if not results_df.empty:
        print("\n=== 监控统计 ===")
        print(f"总计发现新页面: {len(results_df)}")
        print("\n按网站统计:")
        print(results_df['site'].value_counts())
        print("\n按时间范围统计:")
        print(results_df['time_range'].value_counts())

if __name__ == "__main__":
    main()