import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from firecrawl import FirecrawlApp
from loguru import logger
import nest_asyncio
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import time

class WebCrawler:
    def __init__(self, api_key: Optional[str] = None):
        """初始化WebCrawler实例

        Args:
            api_key: Firecrawl API密钥，如果不提供则从环境变量获取
        """
        # 加载环境变量
        load_dotenv()
        
        # 获取API密钥
        self.api_key = api_key or os.getenv('FIRECRAWL_API_KEY')
        if not self.api_key:
            raise ValueError('API密钥未设置，请设置FIRECRAWL_API_KEY环境变量或在初始化时提供api_key')
        
        # 初始化FirecrawlApp
        self.app = FirecrawlApp(api_key=self.api_key)
        
        # 配置日志
        logger.add('crawler.log', rotation='10 MB')
        
        # 初始化session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        })

        # 从环境变量获取配置
        self.base_url = os.getenv('BASE_URL')
        self.username = os.getenv('CSDN_USERNAME')
        self.password = os.getenv('CSDN_PASSWORD')
        self.cookie = os.getenv('CSDN_COOKIE')
        self.output_dir = os.getenv('OUTPUT_DIR', 'output')

        # 设置cookie
        if self.cookie:
            self.session.cookies.update(self._parse_cookie(self.cookie))
        
        self.is_logged_in = False

    def _parse_cookie(self, cookie_str: str) -> Dict[str, str]:
        """解析cookie字符串为字典

        Args:
            cookie_str: Cookie字符串

        Returns:
            Dict[str, str]: Cookie字典
        """
        cookie_dict = {}
        for item in cookie_str.split(';'):
            if '=' in item:
                name, value = item.strip().split('=', 1)
                cookie_dict[name] = value
        return cookie_dict

    def _save_to_markdown(self, content: str, url: str) -> str:
        """将爬取内容保存为Markdown文件

        Args:
            content: Markdown格式的内容
            url: 原始URL

        Returns:
            str: 保存的文件路径
        """
        try:
            # 使用配置的输出目录
            output_dir = os.path.join(os.getcwd(), self.output_dir)
            os.makedirs(output_dir, exist_ok=True)

            # 从URL中提取文件名
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            # 对于CSDN文章，使用文章ID作为文件名
            article_id = next((part for part in reversed(path_parts) if part.isdigit()), 'article')
            filename = f"csdn-{article_id}.md"

            # 保存文件
            file_path = os.path.join(output_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f'内容已保存到文件: {file_path}')
            return file_path

        except Exception as e:
            logger.error(f'保存Markdown文件时发生错误: {str(e)}')
            raise

    def scrape_single_url(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        """爬取单个URL并转换为Markdown

        Args:
            url: 要爬取的URL
            params: 爬取参数

        Returns:
            Dict: 爬取结果
        """
        try:
            default_params = {'formats': ['markdown', 'html']}
            params = params or default_params
            
            logger.info(f'开始爬取URL: {url}')
            result = self.app.scrape_url(url, params=params)
            logger.info(f'URL {url} 爬取完成')
            
            # 保存爬取结果到Markdown文件
            if 'markdown' in result:
                self._save_to_markdown(result['markdown'], url)
            
            return result
        except Exception as e:
            logger.error(f'爬取URL {url} 时发生错误: {str(e)}')
            raise
    
    def crawl_website(self, url: str, limit: int = 100, params: Optional[Dict[str, Any]] = None) -> Dict:
        """批量爬取网站内容

        Args:
            url: 起始URL
            limit: 最大爬取页面数
            params: 爬取参数

        Returns:
            Dict: 爬取结果
        """
        try:
            default_params = {
                'limit': limit,
                'scrapeOptions': {'formats': ['markdown', 'html']}
            }
            params = params or default_params
            
            logger.info(f'开始批量爬取网站: {url}')
            result = self.app.crawl_url(url, params=params)
            logger.info(f'网站 {url} 批量爬取完成')
            
            # 保存批量爬取结果到Markdown文件
            if 'documents' in result:
                for doc in result['documents']:
                    if 'markdown' in doc and 'url' in doc:
                        self._save_to_markdown(doc['markdown'], doc['url'])
            
            return result
        except Exception as e:
            logger.error(f'批量爬取网站 {url} 时发生错误: {str(e)}')
            raise
    
    async def crawl_website_async(self, url: str, limit: int = 100, params: Optional[Dict[str, Any]] = None):
        """异步爬取网站内容

        Args:
            url: 起始URL
            limit: 最大爬取页面数
            params: 爬取参数
        """
        try:
            nest_asyncio.apply()
            
            def on_document(detail):
                logger.info(f'已爬取文档: {detail}')
            
            def on_error(detail):
                logger.error(f'爬取错误: {detail["error"]}')
            
            def on_done(detail):
                logger.info(f'爬取完成，状态: {detail["status"]}')
            
            default_params = {
                'limit': limit,
                'scrapeOptions': {'formats': ['markdown', 'html']}
            }
            params = params or default_params
            
            logger.info(f'开始异步爬取网站: {url}')
            watcher = self.app.crawl_url_and_watch(url, params)
            
            watcher.add_event_listener('document', on_document)
            watcher.add_event_listener('error', on_error)
            watcher.add_event_listener('done', on_done)
            
            await watcher.connect()
        except Exception as e:
            logger.error(f'异步爬取网站 {url} 时发生错误: {str(e)}')
            raise

# 使用示例
if __name__ == '__main__':
    # 创建爬虫实例
    crawler = WebCrawler()
    
    # 爬取单个URL
    result = crawler.scrape_single_url('https://blog.csdn.net/javaeEEse/article/details/139613776')
    print('单URL爬取结果:', result)
    
    # 批量爬取网站
    # result = crawler.crawl_website('https://blog.csdn.net/javaeEEse/article/details/139613776', limit=5)
    # print('批量爬取结果:', result)

    def login(self) -> bool:
        """登录CSDN

        Returns:
            bool: 登录是否成功
        """
        try:
            # 先尝试使用cookie登录
            if self.cookie:
                response = self.session.get(self.base_url)
                response.raise_for_status()
                
                # 检查是否重定向到登录页面
                if '/user/login' not in response.url:
                    logger.info('Cookie验证成功，已登录状态')
                    self.is_logged_in = True
                    return True
                logger.warning('Cookie已失效，尝试使用用户名密码登录')
            
            # Cookie无效或不存在，尝试使用用户名密码登录
            if not (self.username and self.password):
                logger.error('未配置用户名和密码，无法登录')
                return False
            
            # 实现用户名密码登录逻辑
            login_url = 'https://passport.csdn.net/v1/register/pc/login'
            login_data = {
                'loginType': '1',
                'username': self.username,
                'password': self.password
            }
            
            response = self.session.post(login_url, json=login_data)
            response.raise_for_status()
            
            # 验证登录结果
            if response.json().get('code') == 0:
                logger.info('用户名密码登录成功')
                self.is_logged_in = True
                return True
            
            logger.error('用户名密码登录失败')
            return False
            
        except requests.RequestException as e:
            logger.error(f'登录过程中发生请求错误: {str(e)}')
            return False
        except Exception as e:
            logger.error(f'登录过程中发生错误: {str(e)}')
            return False