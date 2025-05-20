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

        # 初始化RAGFlow API
        self.ragflow_api_url = self.config['ragflow']['api_url']
        self.ragflow_api_key = self.config['ragflow']['api_key']

        if not self.ragflow_api_key:
            # 尝试从环境变量获取
            self.ragflow_api_key = os.getenv('RAGFLOW_API_KEY')
            if not self.ragflow_api_key:
                raise ValueError('RAGFlow API密钥未设置，请在config.yml中设置或设置RAGFLOW_API_KEY环境变量')

        # 不再使用FirecrawlApp SDK，直接使用API调用

        # 初始化RAGFlow SDK
        self.ragflow = RAGFlow(api_key=self.ragflow_api_key, base_url=self.ragflow_api_url)

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

    def _chunk_text(self, text: str, chunk_size: int = 512, overlap: int = 100) -> List[str]:
        """将文本分段

        Args:
            text: 要分段的文本
            chunk_size: 每段的最大字符数
            overlap: 段落之间的重叠字符数

        Returns:
            List[str]: 分段后的文本列表
        """
        chunks = []
        start = 0
        text_length = len(text)

        while start < text_length:
            # 计算当前段落的结束位置
            end = min(start + chunk_size, text_length)

            # 如果不是最后一段，尝试在句子或段落边界处分割
            if end < text_length:
                # 尝试在句号、问号、感叹号后分割
                sentence_end = max(
                    text.rfind('。 ', start, end),
                    text.rfind('? ', start, end),
                    text.rfind('! ', start, end),
                    text.rfind('。\n', start, end),
                    text.rfind('?\n', start, end),
                    text.rfind('!\n', start, end)
                )

                # 如果找到了合适的句子边界，在那里分割
                if sentence_end != -1 and sentence_end > start:
                    end = sentence_end + 1  # +1 to include the punctuation
                else:
                    # 否则尝试在段落边界处分割
                    paragraph_end = text.rfind('\n\n', start, end)
                    if paragraph_end != -1 and paragraph_end > start:
                        end = paragraph_end + 2  # +2 to include the newlines

            # 添加当前段落
            chunks.append(text[start:end])

            # 更新下一段的起始位置，考虑重叠
            start = max(start, end - overlap)

        return chunks

    def crawl_page_range(self, base_url: str, start_page: int, end_page: int, wait_min: float = 2.0, wait_max: float = 4.0) -> List[Dict[str, Any]]:
        """爬取指定范围内的页面

        Args:
            base_url: 基础URL
            start_page: 起始页码
            end_page: 结束页码
            wait_min: 爬取页面之间的最小等待时间（秒）
            wait_max: 爬取页面之间的最大等待时间（秒）

        Returns:
            List[Dict[str, Any]]: 爬取结果列表
        """
        results = []

        for page_num in range(start_page, end_page + 1):
            # 如果不是第一个页面，随机等待指定范围内的时间
            if page_num > start_page:
                wait_time = random.uniform(wait_min, wait_max)
                logger.info(f'随机等待 {wait_time:.2f} 秒后继续爬取下一个页面')
                time.sleep(wait_time)

            # 构建完整URL
            url = f"{base_url}{page_num}"

            try:
                logger.info(f'开始爬取页面 {page_num}: {url}')

                # 直接调用Firecrawl API爬取页面
                # 确保API URL格式正确
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
                    "formats": ['markdown']
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

                # 尝试从不同的响应格式中提取内容
                markdown_content = None


                # 从data.markdown获取内容
                markdown_content = result['data']['markdown']
                logger.info(f'从响应的data.markdown字段获取内容')


                # 处理提取的内容
                if markdown_content:
                    # 记录内容预览
                    content_preview = markdown_content[:100] + '...' if len(markdown_content) > 100 else markdown_content
                    logger.info(f'获取到内容预览: {content_preview}')

                    # 保存为Markdown文件
                    file_path = self._save_to_markdown(markdown_content, url, page_num)

                    # 添加页码信息到结果
                    result['page_num'] = page_num
                    result['file_path'] = file_path
                    # 确保结果中有markdown字段，以便后续处理
                    result['markdown'] = markdown_content
                    results.append(result)

                    logger.info(f'页面 {page_num} 爬取成功')
                else:
                    logger.warning(f'页面 {page_num} 爬取成功但未能提取有效内容')
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

                # 继续爬取下一个页面
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
                # 继续爬取下一个页面
                continue

        logger.info(f'爬取完成，共爬取 {len(results)}/{end_page - start_page + 1} 个页面')
        return results

    def upload_to_ragflow(self, markdown_content: str, doc_id: str, knowledge_base_name: str) -> Dict[str, Any]:
        """上传Markdown内容到RAGFlow知识库

        Args:
            markdown_content: Markdown格式的内容
            doc_id: 文档ID
            knowledge_base_name: 知识库名称

        Returns:
            Dict[str, Any]: API响应
        """
        try:
            # 将文本分段
            chunks = self._chunk_text(markdown_content)
            logger.info(f'将内容分为 {len(chunks)} 个段落')

            # 使用RAGFlow SDK获取知识库和文档
            try:
                # 获取知识库
                datasets = self.ragflow.list_datasets(name=knowledge_base_name)
                if not datasets:
                    logger.error(f'未找到知识库: {knowledge_base_name}')
                    raise ValueError(f'未找到知识库: {knowledge_base_name}')

                dataset = datasets[0]
                logger.info(f'已找到知识库: {dataset.name}')

                # 获取或创建文档
                documents = dataset.list_documents(id=doc_id)
                if documents:
                    document = documents[0]
                    logger.info(f'已找到文档: {doc_id}')
                else:
                    # 如果文档不存在，创建新文档
                    document = dataset.create_document(
                        id=doc_id,
                        metadata={
                            'source': 'firecrawl',
                            'format': 'markdown'
                        }
                    )
                    logger.info(f'已创建新文档: {doc_id}')

                # 添加文本块
                added_count = 0
                for chunk in chunks:
                    if chunk.strip():  # 确保不添加空白内容
                        document.add_chunk(content=chunk)
                        added_count += 1

                logger.info(f'成功添加 {added_count} 个文本块到文档 {doc_id}')

                return {
                    'status': 'success',
                    'added_chunks': added_count,
                    'document_id': doc_id,
                    'knowledge_base': knowledge_base_name
                }

            except Exception as sdk_error:
                logger.error(f'使用RAGFlow SDK时发生错误: {str(sdk_error)}')

                # 如果SDK方法失败，回退到直接API调用
                logger.warning('回退到直接API调用')

                # 构建API请求
                url = f"{self.ragflow_api_url}/knowledge_bases/{knowledge_base_name}/documents/{doc_id}"
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {self.ragflow_api_key}'
                }

                # 构建请求数据
                data = {
                    'chunks': chunks,
                    'metadata': {
                        'source': 'firecrawl',
                        'format': 'markdown'
                    }
                }

                # 发送请求
                response = self.session.post(url, headers=headers, json=data)
                response.raise_for_status()

                result = response.json()
                logger.info(f'通过直接API调用成功上传到RAGFlow知识库 {knowledge_base_name}，文档ID: {doc_id}')
                return result

        except Exception as e:
            logger.error(f'上传到RAGFlow时发生错误: {str(e)}')
            raise

    def process(self, base_url: str, start_page: int, end_page: int, doc_id: str, knowledge_base_name: str, skip_rag: bool = False, wait_min: float = 3.0, wait_max: float = 10.0) -> Dict[str, Any]:
        """处理完整流程：爬取页面并上传到RAGFlow

        Args:
            base_url: 基础URL
            start_page: 起始页码
            end_page: 结束页码
            doc_id: RAGFlow文档ID
            knowledge_base_name: RAGFlow知识库名称
            skip_rag: 是否跳过上传到RAGFlow
            wait_min: 爬取页面之间的最小等待时间（秒）
            wait_max: 爬取页面之间的最大等待时间（秒）

        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            # 爬取页面
            crawl_results = self.crawl_page_range(base_url, start_page, end_page, wait_min, wait_max)

            if not crawl_results:
                logger.warning('未爬取到任何页面，无法上传到RAGFlow')
                return {'status': 'error', 'message': '未爬取到任何页面'}

            # 如果不跳过上传到RAGFlow
            upload_results = []
            uploaded_pages = 0

            if not skip_rag:
                # 逐个上传每个页面的内容到RAGFlow
                for result in crawl_results:
                    if 'markdown' in result:
                        page_num = result['page_num']
                        page_content = result['markdown']

                        # 为每个页面创建唯一的文档ID
                        page_doc_id = f"{doc_id}-page-{page_num}"

                        logger.info(f"正在上传页面 {page_num} 到RAGFlow，文档ID: {page_doc_id}")

                        # 上传单个页面内容
                        try:
                            page_result = self.upload_to_ragflow(page_content, page_doc_id, knowledge_base_name)
                            upload_results.append({
                                'page_num': page_num,
                                'doc_id': page_doc_id,
                                'result': page_result
                            })
                            uploaded_pages += 1
                            logger.info(f"页面 {page_num} 上传成功")
                        except Exception as e:
                            logger.error(f"页面 {page_num} 上传失败: {str(e)}")
                            upload_results.append({
                                'page_num': page_num,
                                'doc_id': page_doc_id,
                                'error': str(e)
                            })

                # 汇总上传结果
                upload_result = {
                    'uploaded_pages': uploaded_pages,
                    'total_pages': len(crawl_results),
                    'details': upload_results
                }
            else:
                logger.info("跳过上传到RAGFlow")
                upload_result = {
                    'skipped': True,
                    'message': '根据参数设置，跳过上传到RAGFlow'
                }

            return {
                'status': 'success',
                'crawled_pages': len(crawl_results),
                'total_pages': end_page - start_page + 1,
                'uploaded_pages': uploaded_pages,
                'ragflow_results': upload_result
            }

        except Exception as e:
            logger.error(f'处理过程中发生错误: {str(e)}')
            return {'status': 'error', 'message': str(e)}

def main():
    """主函数，处理命令行参数并执行爬取和上传流程"""
    parser = argparse.ArgumentParser(description='爬取网页并上传到RAGFlow知识库')

    parser.add_argument('--base_url', type=str, required=True,
                        help='基础URL，例如：https://learnblockchain.cn/article/')
    parser.add_argument('--start_page', type=int, required=True,
                        help='起始页码')
    parser.add_argument('--end_page', type=int, required=True,
                        help='结束页码')
    parser.add_argument('--doc_id', type=str, required=True,
                        help='RAGFlow文档ID')
    parser.add_argument('--knowledge_base_name', type=str, required=True,
                        help='RAGFlow知识库名称')
    parser.add_argument('--config', type=str, default='config.yml',
                        help='配置文件路径')
    parser.add_argument('--debug', action='store_true',
                        help='启用调试模式，输出更详细的日志')
    parser.add_argument('--skiprag', action='store_true',
                        help='仅爬取网页，不上传至RAGFlow')
    parser.add_argument('--wait-min', type=float, default=3.0,
                        help='爬取页面之间的最小等待时间（秒），默认为3秒')
    parser.add_argument('--wait-max', type=float, default=10.0,
                        help='爬取页面之间的最大等待时间（秒），默认为10秒')

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

        # 执行处理流程
        result = crawler.process(
            base_url=args.base_url,
            start_page=args.start_page,
            end_page=args.end_page,
            doc_id=args.doc_id,
            knowledge_base_name=args.knowledge_base_name,
            skip_rag=args.skiprag,
            wait_min=args.wait_min,
            wait_max=args.wait_max
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
