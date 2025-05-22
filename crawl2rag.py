#!/usr/bin/env python3
import os
import sys
import argparse
import yaml
import requests
import json
import random
import time
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, urljoin
import re
from loguru import logger
from bs4 import BeautifulSoup
# No longer using FirecrawlApp SDK
from ragflow_sdk import RAGFlow

class Crawl2RAG:
    def __init__(self, config_path: str = "config.yml"):
        """初始化Crawl2RAG实例

        Args:
            config_path: 配置文件路径
        """
        # 加载配置
        self.config = self._load_config(config_path)

        # 初始化Firecrawl API
        self.firecrawl_api_url = self.config['firecrawl']['api_url']
        self.firecrawl_api_key = self.config['firecrawl']['api_key']

        if not self.firecrawl_api_key:
            # 尝试从环境变量获取
            self.firecrawl_api_key = os.getenv('FIRECRAWL_API_KEY')
            if not self.firecrawl_api_key:
                raise ValueError('Firecrawl API密钥未设置，请在config.yml中设置或设置FIRECRAWL_API_KEY环境变量')

        # 配置输出目录
        self.output_dir = self.config['output']['dir']
        os.makedirs(self.output_dir, exist_ok=True)

        # 配置日志
        logger.add('crawl2rag.log', rotation='10 MB')

        # 初始化session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        })

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件

        Args:
            config_path: 配置文件路径

        Returns:
            Dict[str, Any]: 配置字典
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f'成功加载配置文件: {config_path}')
            return config
        except Exception as e:
            logger.error(f'加载配置文件时发生错误: {str(e)}')
            raise

    def _save_to_markdown(self, content: str, url: str, page_num: Optional[int] = None) -> str:
        """将爬取内容保存为Markdown文件

        Args:
            content: Markdown格式的内容
            url: 原始URL
            page_num: 页面编号（如果有）

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

            # 如果提供了页面编号，使用它作为文件名
            if page_num is not None:
                filename = f"page-{page_num}.md"
            else:
                # 否则尝试从URL中提取ID
                article_id = next((part for part in reversed(path_parts) if part.isdigit()), 'article')
                filename = f"article-{article_id}.md"

            # 保存文件
            file_path = os.path.join(output_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f'内容已保存到文件: {file_path}')
            return file_path

        except Exception as e:
            logger.error(f'保存Markdown文件时发生错误: {str(e)}')
            raise

    def _save_to_html(self, content: str, url: str, page_num: Optional[int] = None, title: str = '爬取页面') -> str:
        """将爬取内容保存为html文件

        Args:
            content: html格式的内容
            url: 原始URL
            page_num: 页面编号（如果有）
            title: 页面标题

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

            # 如果提供了页面编号，使用它作为文件名
            if page_num is not None:
                filename = f"page-{page_num}.html"
            else:
                # 否则尝试从URL中提取ID
                article_id = next((part for part in reversed(path_parts) if part.isdigit()), 'article')
                filename = f"article-{article_id}.html"

            # 使用 BeautifulSoup 处理 HTML 内容
            soup = BeautifulSoup(content, 'html.parser')
            
            # 移除所有 .author .feature_container .widget-relation .post-opt 元素
            for author_element in soup.select('.author'):
                author_element.decompose()
            for feature_container in soup.select('.feature_container'):
                feature_container.decompose()
            for widget_relation in soup.select('.widget-relation'):
                widget_relation.decompose()
            for post_opt in soup.select('.post-opt'):
                post_opt.decompose()

            # 检查并添加标题信息
            if not soup.find('head'):
                # 如果没有 head 标签，创建新的 head 标签
                head_tag = soup.new_tag('head')
                title_tag = soup.new_tag('title')
                title_tag.string = title
                head_tag.append(title_tag)
                
                if soup.find('body'):
                    # 如果有 body 标签，在 body 前插入 head
                    soup.body.insert_before(head_tag)
                else:
                    # 如果没有 body 标签，在开头添加
                    soup.insert(0, head_tag)

            # 保存文件
            file_path = os.path.join(output_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(str(soup))
            logger.info(f'内容已保存到文件: {file_path}')
            return file_path

        except Exception as e:
            logger.error(f'保存html文件时发生错误: {str(e)}')
            raise

    def process(self, base_url: str, start_page: int = None, end_page: int = None, page_numbers: List[int] = None, wait_min: float = 2.0, wait_max: float = 4.0, output_type: str = 'markdown') -> Dict[str, Any]:
        """处理完整流程：爬取页面并上传到RAGFlow

        Args:
            base_url: 基础URL
            start_page: 起始页码（当page_numbers为None时使用）
            end_page: 结束页码（当page_numbers为None时使用）
            page_numbers: 要处理的页码列表
            wait_min: 爬取页面之间的最小等待时间（秒）
            wait_max: 爬取页面之间的最大等待时间（秒）
            output_type: 输出类型，可选值为 'markdown' 或 'html'

        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            # 验证输出类型
            if output_type not in ['markdown', 'html']:
                raise ValueError("输出类型必须是 'markdown' 或 'html'")

            # 确定要处理的页码列表
            if page_numbers is not None:
                pages_to_process = page_numbers
            else:
                if start_page is None or end_page is None:
                    raise ValueError("当未提供page_numbers时，start_page和end_page都是必需的")
                pages_to_process = range(start_page, end_page + 1)

            # 爬取页面
            crawl_results = []
            for page_num in pages_to_process:
                # 如果不是第一个页面，随机等待指定范围内的时间
                if len(crawl_results) > 0:
                    wait_time = random.uniform(wait_min, wait_max)
                    logger.info(f'随机等待 {wait_time:.2f} 秒后继续爬取下一个页面')
                    time.sleep(wait_time)

                # 构建完整URL
                url = f"{base_url}{page_num}"

                try:
                    logger.info(f'开始爬取页面 {page_num}: {url}')

                    # 直接调用Firecrawl API爬取页面
                    api_endpoint = self.firecrawl_api_url

                    logger.info(f'Firecrawl API URL: {self.firecrawl_api_url}')
                    logger.info(f'构建的API端点: {api_endpoint}')

                    headers = {
                        "Authorization": f"Bearer {self.firecrawl_api_key}",
                        "Content-Type": "application/json"
                    }

                    # 构建请求数据
                    payload = {
                        "url": url,
                        "formats": ['html'],  # 同时请求两种格式
                        "timeout": 30000  # 设置超时时间为30秒
                    }

                    logger.info(f'请求Firecrawl API: {api_endpoint}')
                    logger.info(f'请求头: {headers}')
                    logger.info(f'请求数据: {json.dumps(payload, ensure_ascii=False)}')

                    # 发送请求
                    response = requests.post(api_endpoint, json=payload, headers=headers)

                    # 记录响应状态和头信息
                    logger.info(f'响应状态码: {response.status_code}')
                    logger.info(f'响应头: {dict(response.headers)}')

                    # 确保请求成功
                    response.raise_for_status()

                    # 解析响应
                    result = response.json()

                    logger.info(f'通过API调用成功爬取页面 {page_num}')

                    # 根据输出类型处理内容
                    if output_type == 'markdown' and result['data']['markdown']:
                        markdown_content = result['data']['markdown']
                        content_preview = markdown_content[:100] + '...' if len(markdown_content) > 100 else markdown_content
                        logger.info(f'获取到Markdown内容预览: {content_preview}')
                        
                        # 保存为Markdown文件
                        file_path = self._save_to_markdown(markdown_content, url, page_num)
                        
                        result['page_num'] = page_num
                        result['file_path'] = file_path
                        result['markdown'] = markdown_content
                        crawl_results.append(result)
                        
                        logger.info(f'页面 {page_num} Markdown内容保存成功')
                    
                    elif output_type == 'html' and result['data']['html']:
                        html_content = result['data']['html']
                        html_content_preview = html_content[:100] + '...' if len(html_content) > 100 else html_content
                        logger.info(f'获取到HTML内容预览: {html_content_preview}')
                        
                        # 获取标题
                        title = result['data']['metadata']['title'].split('|')[0]
                        
                        # 保存为HTML文件
                        file_path = self._save_to_html(html_content, url, page_num, title)
                        
                        result['page_num'] = page_num
                        result['file_path'] = file_path
                        result['html'] = html_content
                        crawl_results.append(result)
                        
                        logger.info(f'页面 {page_num} HTML内容保存成功')
                    else:
                        logger.warning(f'页面 {page_num} 爬取成功但未能提取{output_type}格式的有效内容')
                        # 记录响应结构，以便调试
                        logger.warning(f'响应结构: {json.dumps(list(result.keys()), ensure_ascii=False)}')
                        # 记录部分响应内容
                        truncated_result = {k: str(v)[:100] + '...' if isinstance(v, str) and len(v) > 100 else v
                                           for k, v in result.items()}
                        logger.warning(f'响应内容摘要: {json.dumps(truncated_result, ensure_ascii=False, indent=2)}')

                except requests.RequestException as req_err:
                    # 处理请求相关错误
                    logger.error(f'爬取页面 {page_num} 时发生请求错误: {str(req_err)}')
                    if hasattr(req_err, 'response') and req_err.response is not None:
                        # 记录响应状态码和内容
                        logger.error(f'错误响应状态码: {req_err.response.status_code}')
                        try:
                            error_content = req_err.response.json()
                            logger.error(f'错误响应内容: {json.dumps(error_content, ensure_ascii=False)}')
                        except:
                            logger.error(f'错误响应内容: {req_err.response.text[:500]}...')
                    continue
                except json.JSONDecodeError as json_err:
                    # 处理JSON解析错误
                    logger.error(f'爬取页面 {page_num} 时JSON解析错误: {str(json_err)}')
                    logger.error(f'响应内容: {response.text[:500]}...')
                    continue
                except Exception as e:
                    # 处理其他错误
                    logger.error(f'爬取页面 {page_num} 时发生错误: {str(e)}')
                    import traceback
                    logger.error(f'错误详情: {traceback.format_exc()}')
                    continue

            # 根据输出类型过滤结果
            filtered_results = []
            for result in crawl_results:
                if output_type == 'markdown' and 'markdown' in result:
                    filtered_results.append(result)
                elif output_type == 'html' and 'html' in result:
                    filtered_results.append(result)

            return {
                'status': 'success',
                'crawled_pages': len(filtered_results),
                'total_pages': len(pages_to_process),
                'output_type': output_type
            }

        except Exception as e:
            logger.error(f'处理过程中发生错误: {str(e)}')
            return {'status': 'error', 'message': str(e)}

def main():
    """主函数，处理命令行参数并执行爬取和上传流程"""
    parser = argparse.ArgumentParser(description='爬取网页并上传到RAGFlow知识库')

    parser.add_argument('--base_url', type=str, required=True,
                        help='基础URL，例如：https://learnblockchain.cn/article/')
    parser.add_argument('--start_page', type=int,
                        help='起始页码（当未指定input_file时使用）')
    parser.add_argument('--end_page', type=int,
                        help='结束页码（当未指定input_file时使用）')
    parser.add_argument('--input_file', type=str,
                        help='包含页码列表的文件路径，每行一个页码')
    parser.add_argument('--config', type=str, default='config.yml',
                        help='配置文件路径')
    parser.add_argument('--debug', action='store_true',
                        help='启用调试模式，输出更详细的日志')
    parser.add_argument('--wait-min', type=float, default=2.0,
                        help='爬取页面之间的最小等待时间（秒），默认为2秒')
    parser.add_argument('--wait-max', type=float, default=4.0,
                        help='爬取页面之间的最大等待时间（秒），默认为4秒')
    parser.add_argument('--type', type=str, default='markdown',
                        choices=['markdown', 'html'],
                        help='输出类型，可选值为 markdown 或 html，默认为 markdown')

    args = parser.parse_args()

    try:
        # 配置日志级别
        if args.debug:
            logger.remove()  # 移除默认处理器
            logger.add(sys.stderr, level="DEBUG")
            logger.add("crawl2rag_debug.log", level="DEBUG", rotation="10 MB")
            logger.debug("调试模式已启用")

        # 初始化Crawl2RAG
        crawler = Crawl2RAG(config_path=args.config)

        # 确定要处理的页码
        page_numbers = None
        if args.input_file:
            try:
                with open(args.input_file, 'r') as f:
                    page_numbers = [int(line.strip()) for line in f if line.strip()]
                if not page_numbers:
                    raise ValueError("输入文件为空或只包含空行")
            except ValueError as e:
                logger.error(f"读取页码文件时发生错误: {str(e)}")
                sys.exit(1)
            except Exception as e:
                logger.error(f"读取页码文件时发生错误: {str(e)}")
                sys.exit(1)
        elif args.start_page is None or args.end_page is None:
            logger.error("必须提供 --start_page 和 --end_page，或者提供 --input_file")
            sys.exit(1)

        # 执行处理流程
        result = crawler.process(
            base_url=args.base_url,
            start_page=args.start_page,
            end_page=args.end_page,
            page_numbers=page_numbers,
            wait_min=args.wait_min,
            wait_max=args.wait_max,
            output_type=args.type
        )

        # 输出结果
        print(json.dumps(result, indent=2, ensure_ascii=False))

        # 根据结果设置退出码
        if result['status'] == 'success':
            sys.exit(0)
        else:
            sys.exit(1)

    except Exception as e:
        logger.error(f'执行过程中发生错误: {str(e)}')
        print(f'错误: {str(e)}', file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
