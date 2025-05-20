# firecrawl2ragflow

使用 Firecrawl 抓取网页内容，然后将其转换并存储到 RAGFlow 知识库中。

## 功能特点

- 支持通过配置文件配置 Firecrawl API 和 RAGFlow API
- 支持批量抓取指定范围内的网页
- 自动将抓取的内容分段并上传到 RAGFlow 知识库
- 使用 RAGFlow SDK 进行高效的知识库交互
- 支持智能文本分段，在句子或段落边界处进行分割
- 直接调用 Firecrawl API 而不使用 SDK，确保与本地部署的 API 完全兼容

## 安装

```bash
# 克隆仓库
git clone https://github.com/yourusername/firecrawl2ragflow.git
cd firecrawl2ragflow

# 安装依赖
pip install -r requirements.txt
```

## 配置

在使用前，请先配置 `config.yml` 文件。注意，本工具设计用于连接本地部署的 Firecrawl 和 RAGFlow API，而不是云端服务：

```yaml
# Firecrawl API Configuration
firecrawl:
  api_url: "http://localhost:8000/api"  # 本地部署的 Firecrawl API URL
  api_key: "your_firecrawl_api_key"     # 你的 Firecrawl API key

# RAGFlow API Configuration
ragflow:
  api_url: "http://localhost:8001/api"  # 本地部署的 RAGFlow API URL
  api_key: "your_ragflow_api_key"       # 你的 RAGFlow API key

# Output Configuration
output:
  dir: "output"  # Directory to save crawled content
```

## 使用方法

```bash
python crawl2rag.py --base_url https://learnblockchain.cn/article/ --start_page 1 --end_page 1000 --doc_id blockchain-articles --knowledge_base_name blockchain
```

### 参数说明

- `--base_url`: 基础URL，例如：https://learnblockchain.cn/article/
- `--start_page`: 起始页码
- `--end_page`: 结束页码
- `--doc_id`: RAGFlow文档ID
- `--knowledge_base_name`: RAGFlow知识库名称
- `--config`: 配置文件路径（默认为 config.yml）

## 示例

抓取区块链文章并上传到知识库：

```bash
python crawl2rag.py --base_url https://learnblockchain.cn/article/ --start_page 1 --end_page 10 --doc_id blockchain-basics --knowledge_base_name crypto
```

## 许可证

MIT
