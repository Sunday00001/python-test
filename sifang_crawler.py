import os
import sys
import time
import random
import requests
import concurrent.futures
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from PIL import Image
import io
import json
import argparse
import logging

# 配置日志，设置更详细的日志格式和级别
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# 默认保存路径
DEFAULT_SAVE_PATH = r"E:\pachong\结果\sifang.lat"

class SifangCrawler:
    def __init__(self, save_path, verify=False, use_proxy=False, proxy_config=None):
        self.save_path = save_path
        self.verify = verify
        self.use_proxy = use_proxy
        self.proxy_config = proxy_config if proxy_config else {}
        
        # 初始化会话，支持cookie持久化
        self.session = requests.Session()
        
        # 加载和保存cookies
        self.cookie_file = os.path.join(save_path, 'cookies.json')
        self.load_cookies()
        
        # 配置连接池
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # 设置请求头
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://sifang.lat/',
            'Connection': 'keep-alive'
        })
        
        # 设置超时
        self.session.timeout = (15, 60)
        
        # 相册链接存储区
        self.album_queue = []
        
        # 最大并发线程数 - 相册并发控制在3~5个
        self.max_workers = 4  # 相册并发数
        self.img_max_workers = 8  # 图片并发数，降低为8个，减少第三方服务器压力
        
        # 已完成相册记录
        self.completed_albums = set()
        self.completed_albums_file = os.path.join(save_path, 'completed_albums.txt')
        self.load_completed_albums()
        
        # 统计信息
        self.stats = {
            'total_albums': 0,
            'downloaded_albums': 0,
            'total_images': 0,
            'downloaded_images': 0,
            'current_album': '',
            'current_image': 0,
            'current_album_progress': 0
        }
        
        # 失败队列
        self.failed_albums = []
        self.failed_images = []
        
        # 实时进度显示锁
        self.stats_lock = threading.Lock()
    
    def load_cookies(self):
        """加载cookies"""
        if os.path.exists(self.cookie_file):
            try:
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                self.session.cookies.update(cookies)
                logger.info(f"已加载 {len(cookies)} 个cookies")
            except Exception as e:
                logger.error(f"加载cookies失败: {e}")
    
    def save_cookies(self):
        """保存cookies"""
        try:
            cookies = self.session.cookies.get_dict()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存 {len(cookies)} 个cookies")
        except Exception as e:
            logger.error(f"保存cookies失败: {e}")
    
    def load_completed_albums(self):
        """加载已完成的相册记录"""
        if os.path.exists(self.completed_albums_file):
            try:
                with open(self.completed_albums_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        self.completed_albums.add(line.strip())
                logger.info(f"已加载 {len(self.completed_albums)} 个已完成相册记录")
            except Exception as e:
                logger.error(f"加载已完成相册记录失败: {e}")
    
    def save_completed_album(self, album_url):
        """保存已完成的相册记录"""
        if album_url not in self.completed_albums:
            self.completed_albums.add(album_url)
            try:
                with open(self.completed_albums_file, 'a', encoding='utf-8') as f:
                    f.write(f"{album_url}\n")
            except Exception as e:
                logger.error(f"保存已完成相册记录失败: {e}")
    
    def update_stats(self, key, value):
        """更新统计信息，线程安全"""
        with self.stats_lock:
            self.stats[key] = value
    
    def print_progress(self):
        """打印详细实时进度"""
        with self.stats_lock:
            # 清除当前行
            print(f"\r\033[K", end="", flush=True)
            
            # 打印详细进度信息
            print(f"[全局进度] 总相册: {self.stats['total_albums']} | 已完成: {self.stats['downloaded_albums']} | 总图片: {self.stats['total_images']} | 已下载: {self.stats['downloaded_images']} | ", end="", flush=True)
            
            # 打印当前相册信息
            current_album = self.stats['current_album'][:25] + '...' if len(self.stats['current_album']) > 25 else self.stats['current_album']
            print(f"[当前相册] {current_album} | 进度: {self.stats['current_album_progress']}% | 当前图片: {self.stats['current_image']} | ", end="", flush=True)
    
    def random_delay(self, min_seconds=4, max_seconds=8):
        """随机延迟，模拟真实用户行为"""
        # 增加延迟范围，模拟更真实的用户行为
        delay = random.uniform(min_seconds, max_seconds)
        logger.info(f"随机延迟 {delay:.2f} 秒")
        time.sleep(delay)
    
    def retry_request(self, url, max_retries=5, method='get', **kwargs):
        """带指数退避重试机制的请求，增强抗443和10054能力，增加访问频率控制"""
        retries = 0
        while retries < max_retries:
            try:
                # 添加随机用户代理
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15'
                ]
                self.session.headers['User-Agent'] = random.choice(user_agents)
                
                # 添加随机Referer
                self.session.headers['Referer'] = url
                
                # 设置代理（预留功能）
                if self.use_proxy and self.proxy_config:
                    kwargs['proxies'] = self.proxy_config
                
                # 增加请求间隔，降低访问频率
                # 随机延迟2-5秒，进一步降低被封锁的风险
                request_delay = random.uniform(2, 5)
                logger.info(f"请求前延迟 {request_delay:.2f} 秒，降低访问频率")
                time.sleep(request_delay)
                
                response = self.session.request(method, url, **kwargs)
                
                # 特殊处理443错误
                if response.status_code == 443:
                    logger.error(f"请求 {url} 遇到443 Forbidden，可能被反爬机制拦截")
                    # 等待更长时间再重试
                    time.sleep(random.uniform(15, 30))
                    retries += 1
                    continue
                
                response.raise_for_status()
                
                # 检查是否跳转到其他网站
                if 'sifang.lat' not in response.url:
                    logger.warning(f"请求 {url} 跳转到了 {response.url}，跳过该请求")
                    return None
                
                # 请求成功后，添加额外延迟，降低整体访问频率
                post_delay = random.uniform(1, 3)
                logger.info(f"请求成功，延迟 {post_delay:.2f} 秒")
                time.sleep(post_delay)
                
                return response
            except requests.RequestException as e:
                # 特殊处理10054错误（连接被重置）
                if isinstance(e, requests.exceptions.ConnectionError) and "10054" in str(e):
                    logger.error(f"请求 {url} 遇到10054错误，连接被远程服务器重置，可能是访问频率过高")
                    # 等待更长时间再重试
                    time.sleep(random.uniform(20, 40))
                else:
                    logger.error(f"请求 {url} 失败: {e}")
                
                retries += 1
                if retries >= max_retries:
                    logger.error(f"请求 {url} 失败，已重试 {max_retries} 次: {e}")
                    logger.info("等待用户检查网络，按任意键继续...")
                    input()
                    retries = 0
                else:
                    # 指数退避策略，增加初始延迟
                    base_delay = 6
                    max_delay = 45
                    delay = min(base_delay * (2 ** (retries - 1)) + random.uniform(0, 2), max_delay)
                    logger.warning(f"请求 {url} 失败，{retries}/{max_retries} 重试，延迟 {delay:.2f} 秒: {e}")
                    time.sleep(delay)
    
    def is_image_corrupted(self, img_path):
        """检查图片是否损坏"""
        try:
            with Image.open(img_path) as img:
                img.verify()
                img.load()
            return False
        except Exception as e:
            logger.error(f"图片 {img_path} 损坏: {e}")
            return True
    
    def get_all_albums(self, start_url):
        """获取所有相册链接"""
        current_url = start_url
        
        while current_url:
            logger.info(f"处理页: {current_url}")
            response = self.retry_request(current_url)
            if not response:
                logger.error(f"获取页面 {current_url} 失败，跳过")
                current_url = self.get_next_page(current_url, None)
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取当前页相册
            albums = self.get_album_list(current_url, soup)
            
            # 添加到队列
            for album_url in albums:
                if album_url not in self.completed_albums and not any(item['url'] == album_url for item in self.album_queue):
                    self.album_queue.append({'url': album_url, 'status': 'pending'})
            
            logger.info(f"当前队列中共有 {len(self.album_queue)} 个相册")
            
            # 获取下一页
            current_url = self.get_next_page(current_url, soup)
            
            # 列表页爬取延迟
            self.random_delay(2, 4)
        
        logger.info(f"总共找到 {len(self.album_queue)} 个相册")
        self.update_stats('total_albums', len(self.album_queue))
        return self.album_queue
    
    def get_album_list(self, page_url, soup):
        """获取当前页相册列表"""
        album_links = []
        # 查找所有相册链接
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            if '/albums/' in href:
                full_url = urljoin(page_url, href)
                if full_url not in album_links:
                    album_links.append(full_url)
        logger.info(f"页面 {page_url} 找到 {len(album_links)} 个相册")
        return album_links
    
    def get_next_page(self, page_url, soup):
        """获取下一页链接，增强对箭头按钮的识别"""
        if not soup:
            return None
        
        # 查找下一页按钮
        next_button = None
        
        # 1. 尝试查找包含箭头的按钮（增强箭头识别）
        arrow_patterns = ['→', '›', '»', '下一页', 'Next', 'next', 'more']
        for a in soup.find_all('a', href=True):
            # 检查标签文本和属性中的箭头
            if any(pattern in str(a) for pattern in arrow_patterns):
                next_button = a
                logger.debug(f"找到箭头样式下一页按钮: {a}")
                break
        
        # 2. 如果没找到，查找包含page参数且页码递增的链接
        if not next_button:
            parsed_url = urlparse(page_url)
            current_page = int(parse_qs(parsed_url.query).get('page', ['1'])[0])
            
            for a in soup.find_all('a', href=True):
                href = a.get('href')
                if 'page=' in href:
                    try:
                        page_num = int(parse_qs(urlparse(href).query).get('page', ['0'])[0])
                        if page_num == current_page + 1:
                            next_button = a
                            logger.debug(f"找到页码递增下一页按钮: {a}")
                            break
                    except ValueError:
                        continue
        
        # 3. 如果找到下一页按钮，返回链接
        if next_button and 'href' in next_button.attrs:
            next_url = urljoin(page_url, next_button['href'])
            # 检查是否在当前域名内
            if 'sifang.lat' in next_url:
                return next_url
            else:
                logger.warning(f"下一页链接 {next_url} 不在当前域名内，跳过")
                return None
        
        # 4. 最后尝试通过构造URL获取下一页
        parsed_url = urlparse(page_url)
        current_page = int(parse_qs(parsed_url.query).get('page', ['1'])[0])
        
        # 查找最大页码
        max_page = current_page
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            if 'page=' in href:
                try:
                    page_num = int(parse_qs(urlparse(href).query).get('page', ['0'])[0])
                    if page_num > max_page:
                        max_page = page_num
                except ValueError:
                    continue
        
        next_page = current_page + 1
        if next_page <= max_page:
            next_params = parse_qs(parsed_url.query)
            next_params['page'] = [str(next_page)]
            next_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{urlencode(next_params, doseq=True)}"
            logger.debug(f"构造下一页URL: {next_url}")
            return next_url
        
        logger.debug(f"未找到下一页链接")
        return None
    
    def get_album_images(self, album_url):
        """获取相册中的所有图片（增强箭头翻页按钮识别和广告过滤）"""
        logger.info(f"获取相册图片: {album_url}")
        # 专辑详情页请求延迟
        self.random_delay(3, 6)
        
        all_image_links = []
        current_url = album_url
        album_name = None
        
        while current_url:
            logger.info(f"处理相册页: {current_url}")
            response = self.retry_request(current_url)
            if not response:
                logger.error(f"获取相册页 {current_url} 失败，跳过")
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 第一次获取时，获取相册名称
            if not album_name:
                # 获取相册名称
                album_name = soup.title.text.strip() if soup.title else f"album_{album_url.split('/')[-1]}"
                # 清理文件夹名称
                if ' | ' in album_name:
                    album_name = album_name.split(' | ')[0]
                album_name = ''.join(c for c in album_name if c not in '<>:/\\|?*').strip()[:50]
                logger.info(f"相册名称: {album_name}")
            
            # 1. 查找翻页按钮（增强箭头识别）
            next_page_button = None
            arrow_patterns = ['→', '›', '»', '下一页', 'Next', 'next', 'more']
            
            # 尝试多种方式查找下一页按钮
            for a in soup.find_all('a', href=True):
                # 检查标签文本、属性和样式
                a_str = str(a)
                if any(pattern in a_str for pattern in arrow_patterns):
                    next_page_button = a
                    logger.debug(f"找到相册内翻页按钮: {a}")
                    break
            
            # 如果没找到，检查包含page参数的链接
            if not next_page_button:
                parsed_url = urlparse(current_url)
                current_page = int(parse_qs(parsed_url.query).get('page', ['1'])[0])
                
                for a in soup.find_all('a', href=True):
                    href = a.get('href')
                    if 'page=' in href:
                        try:
                            page_num = int(parse_qs(urlparse(href).query).get('page', ['0'])[0])
                            if page_num == current_page + 1:
                                next_page_button = a
                                logger.debug(f"找到页码递增翻页按钮: {a}")
                                break
                        except ValueError:
                            continue
            
            # 2. 尝试找到主要内容区域
            img_container = None
            
            # 尝试多种方式查找主要内容区域
            possible_containers = [
                # 1. 查找class包含content的div
                soup.find_all('div', {'class': lambda x: x and 'content' in x.lower()}),
                # 2. 查找id或class包含main的元素
                soup.find_all(['div', 'main'], {'id': 'main', 'class': lambda x: x and 'main' in x.lower()}),
                # 3. 查找class包含post或article的元素
                soup.find_all(['div', 'article'], {'class': lambda x: x and ('post' in x.lower() or 'article' in x.lower())}),
                # 4. 最后使用body
                [soup.body]
            ]
            
            for container_list in possible_containers:
                if container_list:
                    img_container = container_list[0]
                    logger.debug(f"使用主要内容容器: {img_container}")
                    break
            
            # 3. 查找所有图片，只保留当前主题图片，规避推荐相册
            page_images = []
            
            # 1. 首先尝试查找文章内容区域内的图片
            article_content = soup.find('article') or soup.find('div', {'class': 'post-content'}) or soup.find('div', {'class': 'entry-content'})
            if article_content:
                logger.debug("使用文章内容区域查找图片")
                img_tags = article_content.find_all('img', src=True)
            else:
                # 2. 如果没有找到文章内容区域，查找所有img标签
                logger.debug("使用全局img标签查找图片")
                img_tags = soup.find_all('img', src=True)
            
            logger.debug(f"当前页找到 {len(img_tags)} 个img标签")
            
            for img in img_tags:
                src = img.get('src')
                if not src:
                    continue
                
                # 处理图片URL，确保完整
                if src.startswith('//'):
                    src = f"https:{src}"
                elif src.startswith('/'):
                    src = urljoin(current_url, src)
                
                # 跳过非常明显的广告图片
                obvious_ad_keywords = ['ad/', 'ads/', 'advertisement/', 'banner/', 'doubleclick.net', 'googlesyndication.com', 'adserver.', 'affiliate.', 'tracking.']
                if any(ad_keyword in src.lower() for ad_keyword in obvious_ad_keywords):
                    logger.debug(f"图片URL包含明显广告关键词，跳过: {src}")
                    continue
                
                # 检查图片是否是隐藏的悬浮广告
                is_hidden = False
                if img.has_attr('style'):
                    style = img['style'].lower()
                    if 'opacity:0' in style or 'visibility:hidden' in style or 'display:none' in style:
                        is_hidden = True
                        logger.debug(f"图片样式为隐藏，跳过: {src}")
                        continue
                
                # 检查图片尺寸，跳过极小的图片（可能是广告或图标）
                if img.has_attr('width') and img.has_attr('height'):
                    try:
                        width = int(img['width'])
                        height = int(img['height'])
                        # 跳过宽度或高度小于100px的图片
                        if width < 100 or height < 100:
                            logger.debug(f"图片尺寸过小 ({width}x{height})，跳过: {src}")
                            continue
                    except ValueError:
                        pass
                
                # 检查图片是否在推荐相册区域
                is_in_recommend_area = False
                
                # 1. 检查图片的父级链接是否指向其他相册
                parent = img.parent
                while parent:
                    if parent.name == 'a' and parent.has_attr('href'):
                        parent_href = parent['href']
                        # 如果父级链接是指向其他相册的链接，跳过该图片
                        if '/albums/' in parent_href and parent_href != current_url:
                            logger.debug(f"图片在指向其他相册的链接内，跳过: {src}")
                            is_in_recommend_area = True
                            break
                    parent = parent.parent
                
                if is_in_recommend_area:
                    continue
                
                # 2. 检查图片是否在推荐区域内
                # 查找推荐区域容器
                recommend_containers = soup.find_all(['div', 'section'], {'class': lambda x: x and any(keyword in x.lower() for keyword in ['recommend', 'related', '推荐', '相关', 'more', '更多'])})
                for container in recommend_containers:
                    if container.find('img', src=src):
                        logger.debug(f"图片在推荐区域内，跳过: {src}")
                        is_in_recommend_area = True
                        break
                
                if is_in_recommend_area:
                    continue
                
                # 3. 检查图片URL特征，区分当前主题图片和推荐相册图片
                # 当前主题图片特征：
                # - 通常是第三方托管的（如 imgbox.com, wp.com 等）
                # - 直接指向图片文件
                # - 文件名包含图片相关扩展名
                
                # 推荐相册图片特征：
                # - 可能是网站内部图片
                # - 可能指向其他相册的缩略图
                
                # 确保是图片文件
                img_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
                if any(src.lower().endswith(ext) for ext in img_extensions):
                    # 特别处理第三方托管图片，这些通常是当前主题的图片
                    if 'wp.com' in src or 'imgbox.com' in src or 'imgur.com' in src or 'pixhost.to' in src:
                        logger.debug(f"找到第三方托管图片（当前主题）: {src}")
                        if src not in page_images and src not in all_image_links:
                            page_images.append(src)
                            logger.debug(f"添加有效图片: {src}")
                    # 处理网站内部图片，只保留当前主题的
                    elif 'sifang.lat' in src:
                        # 检查是否是当前主题的图片，而非推荐相册的图片
                        # 通常当前主题的图片会在文章内容区域内
                        if article_content and article_content.find('img', src=src):
                            logger.debug(f"找到网站内部当前主题图片: {src}")
                            if src not in page_images and src not in all_image_links:
                                page_images.append(src)
                                logger.debug(f"添加有效图片: {src}")
                        else:
                            logger.debug(f"网站内部图片不在文章内容区域，可能是推荐图片，跳过: {src}")
                    # 其他第三方图片，视为当前主题图片
                    else:
                        logger.debug(f"找到其他第三方图片: {src}")
                        if src not in page_images and src not in all_image_links:
                            page_images.append(src)
                            logger.debug(f"添加有效图片: {src}")
            
            # 4. 确保至少找到一些图片
            if not page_images and img_tags:
                logger.warning(f"当前页没有找到有效图片，尝试放宽条件")
                # 尝试放宽条件，但仍需规避推荐相册
                for img in img_tags:
                    src = img.get('src')
                    if not src:
                        continue
                    
                    if src.startswith('//'):
                        src = f"https:{src}"
                    elif src.startswith('/'):
                        src = urljoin(current_url, src)
                    
                    # 只过滤绝对确定的广告
                    if 'ad.' not in src.lower() and 'ads.' not in src.lower() and 'advertisement' not in src.lower():
                        # 检查是否是推荐相册图片
                        is_recommend = False
                        
                        # 检查父级链接是否指向其他相册
                        parent = img.parent
                        while parent and not is_recommend:
                            if parent.name == 'a' and parent.has_attr('href'):
                                parent_href = parent['href']
                                if '/albums/' in parent_href and parent_href != current_url:
                                    is_recommend = True
                            parent = parent.parent
                        
                        if not is_recommend:
                            img_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
                            if any(src.lower().endswith(ext) for ext in img_extensions):
                                if src not in page_images and src not in all_image_links:
                                    page_images.append(src)
                                    logger.debug(f"放宽条件后找到图片: {src}")
            
            # 添加当前页图片到总列表
            all_image_links.extend(page_images)
            logger.info(f"当前页添加 {len(page_images)} 张有效图片，累计 {len(all_image_links)} 张")
            
            # 5. 获取下一页链接
            current_url = None
            if next_page_button and 'href' in next_page_button.attrs:
                next_url = urljoin(current_url or album_url, next_page_button['href'])
                # 检查是否是同一相册的下一页
                if 'sifang.lat' in next_url and '/albums/' in next_url:
                    current_url = next_url
                    logger.debug(f"获取到相册下一页链接: {current_url}")
                else:
                    logger.info(f"下一页 {next_url} 不是同一相册或域名，停止翻页")
            else:
                logger.info(f"未找到下一页链接，停止翻页")
            
            # 相册内翻页延迟
            if current_url:
                self.random_delay(2, 4)
        
        logger.info(f"相册 {album_name} 共找到 {len(all_image_links)} 张图片")
        return album_name, all_image_links
    
    def download_image(self, image_url, save_path):
        """下载单张图片，增强访问频率控制"""
        if os.path.exists(save_path):
            if not self.verify:
                logger.info(f"图片 {save_path} 已存在，跳过")
                return True
            # 验证图片完整性
            if not self.is_image_corrupted(save_path):
                logger.info(f"图片 {save_path} 已存在且完整，跳过")
                return True
            else:
                logger.info(f"图片 {save_path} 已存在但损坏，重新下载")
        
        # 图片下载专用的重试机制，增加更严格的延迟
        max_retries = 5
        for retry in range(max_retries):
            try:
                # 图片下载前增加更长的延迟，2-6秒
                img_delay = random.uniform(2, 6)
                logger.info(f"下载图片前延迟 {img_delay:.2f} 秒，降低图片服务器访问频率")
                time.sleep(img_delay)
                
                # 使用专门的请求设置，降低访问频率
                response = self.session.get(image_url, stream=True, timeout=(30, 60))
                response.raise_for_status()
                
                # 验证响应内容是否为图片
                content_type = response.headers.get('Content-Type', '')
                if not content_type.startswith('image/'):
                    logger.error(f"响应内容不是图片，URL: {image_url}，Content-Type: {content_type}")
                    return False
                
                # 原子化写入
                temp_path = save_path + '.tmp'
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # 验证图片完整性
                if self.is_image_corrupted(temp_path):
                    os.unlink(temp_path)
                    logger.error(f"下载的图片 {image_url} 损坏")
                    # 损坏图片重试延迟
                    time.sleep(random.uniform(3, 5))
                    continue
                
                # 重命名临时文件
                os.replace(temp_path, save_path)
                
                # 更新统计信息
                self.update_stats('downloaded_images', self.stats['downloaded_images'] + 1)
                self.print_progress()
                
                # 图片下载成功后延迟1-3秒
                time.sleep(random.uniform(1, 3))
                return True
            except requests.exceptions.ConnectionError as e:
                # 特殊处理10054错误
                if "10054" in str(e):
                    logger.error(f"下载图片 {image_url} 遇到10054错误，第 {retry+1}/{max_retries} 次重试")
                    # 遇到连接错误时，等待更长时间
                    wait_time = random.uniform(10, 20) + (retry * 5)
                    logger.info(f"等待 {wait_time:.2f} 秒后重试")
                    time.sleep(wait_time)
                else:
                    logger.error(f"下载图片 {image_url} 遇到连接错误，第 {retry+1}/{max_retries} 次重试: {e}")
                    time.sleep(random.uniform(5, 10))
            except requests.RequestException as e:
                logger.error(f"下载图片 {image_url} 失败，第 {retry+1}/{max_retries} 次重试: {e}")
                # 其他请求错误，等待5-10秒
                time.sleep(random.uniform(5, 10))
            except Exception as e:
                logger.error(f"下载图片 {image_url} 遇到未知错误，第 {retry+1}/{max_retries} 次重试: {e}")
                time.sleep(random.uniform(3, 8))
        
        logger.error(f"图片 {image_url} 下载失败，已重试 {max_retries} 次")
        return False
    
    def download_album(self, album_item, album_index, total_albums):
        """下载单个相册"""
        album_url = album_item['url']
        # 处理total_albums为-1的情况（边爬边下载模式）
        if total_albums > 0:
            logger.info(f"[专辑 {album_index}/{total_albums}] 开始处理: {album_url}")
        else:
            logger.info(f"[专辑 {album_index}] 开始处理: {album_url}")
        
        try:
            # 更新统计信息
            self.update_stats('current_album', album_url.split('/')[-1])
            self.update_stats('current_album_progress', 0)
            
            # 获取相册图片
            album_name, image_links = self.get_album_images(album_url)
            
            # 创建保存目录
            album_dir = os.path.join(self.save_path, album_name)
            os.makedirs(album_dir, exist_ok=True)
            
            # 更新统计信息
            self.update_stats('total_images', self.stats['total_images'] + len(image_links))
            
            # 并发下载图片
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.img_max_workers) as executor:
                futures = []
                
                # 提交所有图片下载任务
                for i, img_url in enumerate(image_links, 1):
                    # 生成保存文件名
                    img_name = f"image_{i:03d}{os.path.splitext(urlparse(img_url).path)[1]}"
                    save_path = os.path.join(album_dir, img_name)
                    futures.append((executor.submit(self.download_image, img_url, save_path), img_url, save_path, i))
                
                # 等待下载完成并更新进度
                downloaded_images = 0
                for future, img_url, save_path, img_index in futures:
                    result = future.result()
                    if result:
                        downloaded_images += 1
                    else:
                        # 图片下载失败，添加到失败队列
                        self.failed_images.append({
                            'url': img_url,
                            'save_path': save_path,
                            'album_url': album_url,
                            'album_name': album_name
                        })
                        logger.debug(f"图片 {img_url} 添加到失败队列")
                    
                    # 更新进度
                    progress = int((downloaded_images / len(image_links)) * 100)
                    self.update_stats('current_album_progress', progress)
                    self.update_stats('current_image', img_index)
                    self.print_progress()
            
            # 更新统计信息
            self.update_stats('downloaded_albums', self.stats['downloaded_albums'] + 1)
            
            logger.info(f"[专辑 {album_index}/{total_albums}] {album_name} 下载完成，成功 {downloaded_images}/{len(image_links)} 张图片")
            
            # 保存已完成记录
            self.save_completed_album(album_url)
            
            # 保存cookies
            self.save_cookies()
            
            return True
        except Exception as e:
            logger.error(f"[专辑 {album_index}/{total_albums}] 处理相册 {album_url} 失败: {e}")
            # 相册处理失败，添加到失败队列
            self.failed_albums.append({
                'url': album_url,
                'error': str(e)
            })
            logger.debug(f"相册 {album_url} 添加到失败队列")
            return False
    
    def download(self):
        """开始下载，实现边爬边下载"""
        logger.info(f"开始爬取网站: https://sifang.lat/")
        logger.info(f"保存路径: {self.save_path}")
        logger.info(f"相册并发数: {self.max_workers}")
        logger.info(f"图片并发数: {self.img_max_workers}")
        
        # 创建保存目录
        os.makedirs(self.save_path, exist_ok=True)
        
        # 创建线程安全的相册队列
        import queue
        album_queue = queue.Queue()
        
        # 线程安全的标记，用于控制爬取线程的退出
        import threading
        crawl_complete = threading.Event()
        
        # 爬取线程函数：持续获取相册链接并添加到队列
        def crawl_albums():
            current_url = 'https://sifang.lat/'
            album_count = 0
            
            while current_url and not crawl_complete.is_set():
                logger.info(f"处理页: {current_url}")
                response = self.retry_request(current_url)
                if not response:
                    logger.error(f"获取页面 {current_url} 失败，跳过")
                    current_url = self.get_next_page(current_url, None)
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 获取当前页相册
                albums = self.get_album_list(current_url, soup)
                
                # 添加到队列
                for album_url in albums:
                    if album_url not in self.completed_albums:
                        album_queue.put({'url': album_url, 'status': 'pending'})
                        album_count += 1
                        logger.info(f"已将相册添加到队列: {album_url}，队列当前大小: {album_queue.qsize()}")
                
                # 获取下一页
                current_url = self.get_next_page(current_url, soup)
                
                # 列表页爬取延迟
                self.random_delay(4, 8)
            
            logger.info(f"爬取线程完成，共找到 {album_count} 个相册")
            crawl_complete.set()
        
        # 启动爬取线程
        crawl_thread = threading.Thread(target=crawl_albums, daemon=True)
        crawl_thread.start()
        
        # 并发处理相册：从队列中获取相册并下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = set()
            
            # 持续处理队列中的相册，直到爬取完成且队列为空
            while not (crawl_complete.is_set() and album_queue.empty()):
                try:
                    # 尝试从队列中获取相册，超时时间为5秒
                    album_item = album_queue.get(timeout=5)
                    
                    # 提交下载任务
                    future = executor.submit(self.download_album, album_item, self.stats['total_albums'] + 1, -1)
                    futures.add(future)
                    
                    # 更新总相册数
                    self.update_stats('total_albums', self.stats['total_albums'] + 1)
                    
                    # 限制并发任务数，确保不超过max_workers
                    if len(futures) >= self.max_workers:
                        # 等待至少一个任务完成
                        done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        
                        # 处理完成的任务
                        for future in done:
                            try:
                                result = future.result()
                                if result:
                                    album_item['status'] = 'downloaded'
                                else:
                                    album_item['status'] = 'failed'
                            except Exception as e:
                                logger.error(f"处理相册失败: {e}")
                                album_item['status'] = 'failed'
                except queue.Empty:
                    # 队列为空，继续检查
                    continue
            
            # 等待所有剩余任务完成
            if futures:
                concurrent.futures.wait(futures)
        
        # 保存cookies
        self.save_cookies()
        
        # 打印最终统计信息
        print()
        logger.info(f"所有相册下载完成，共处理 {total_albums} 个相册")
        logger.info(f"成功下载 {self.stats['downloaded_albums']} 个相册")
        logger.info(f"总共下载 {self.stats['downloaded_images']} 张图片")
        
        # 打印失败队列统计
        print()
        logger.info(f"=== 失败队列统计 ===")
        logger.info(f"失败相册数量: {len(self.failed_albums)}")
        logger.info(f"失败图片数量: {len(self.failed_images)}")
        
        # 询问用户是否重试失败队列
        if self.failed_albums or self.failed_images:
            retry_input = input("\n是否重试失败的相册和图片？(y/Y 是, n/N 否): ").strip().lower()
            if retry_input == 'y':
                logger.info("开始重试失败队列...")
                
                # 重试失败的相册
                if self.failed_albums:
                    logger.info(f"开始重试 {len(self.failed_albums)} 个失败的相册")
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = {}
                        for i, failed_album in enumerate(self.failed_albums, 1):
                            album_item = {'url': failed_album['url'], 'status': 'pending'}
                            future = executor.submit(self.download_album, album_item, i, len(self.failed_albums))
                            futures[future] = (i, failed_album)
                        
                        # 等待任务完成
                        for future in concurrent.futures.as_completed(futures):
                            i, failed_album = futures[future]
                            try:
                                result = future.result()
                                if result:
                                    logger.info(f"重试相册 {failed_album['url']} 成功")
                                else:
                                    logger.error(f"重试相册 {failed_album['url']} 仍失败")
                            except Exception as e:
                                logger.error(f"重试相册 {failed_album['url']} 发生异常: {e}")
                
                # 重试失败的图片
                if self.failed_images:
                    logger.info(f"开始重试 {len(self.failed_images)} 张失败的图片")
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.img_max_workers) as executor:
                        futures = []
                        for i, failed_image in enumerate(self.failed_images, 1):
                            futures.append((executor.submit(self.download_image, failed_image['url'], failed_image['save_path']), i))
                        
                        # 等待下载完成
                        success_count = 0
                        for future, img_index in futures:
                            result = future.result()
                            if result:
                                success_count += 1
                            
                            # 更新进度
                            progress = int((img_index / len(self.failed_images)) * 100)
                            print(f"\r重试图片进度: {progress}% | 成功: {success_count}/{img_index}", end="", flush=True)
                        
                        print()
                        logger.info(f"图片重试完成，成功 {success_count}/{len(self.failed_images)} 张")
                
                logger.info("所有失败队列重试完成")
            else:
                logger.info("跳过失败队列重试")
                
                # 保存失败记录到文件，方便后续处理
                failed_log_file = os.path.join(self.save_path, 'failed_items.log')
                with open(failed_log_file, 'w', encoding='utf-8') as f:
                    f.write("=== 失败相册 ===\n")
                    for album in self.failed_albums:
                        f.write(f"URL: {album['url']}\n")
                        f.write(f"Error: {album['error']}\n\n")
                    
                    f.write("=== 失败图片 ===\n")
                    for img in self.failed_images:
                        f.write(f"URL: {img['url']}\n")
                        f.write(f"Save Path: {img['save_path']}\n")
                        f.write(f"Album URL: {img['album_url']}\n")
                        f.write(f"Album Name: {img['album_name']}\n\n")
                
                logger.info(f"失败记录已保存到 {failed_log_file}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='sifang.lat 爬虫')
    parser.add_argument('--verify', action='store_true', help='验证并修复已存在的损坏文件')
    parser.add_argument('--use-proxy', action='store_true', help='使用代理（预留功能）')
    args = parser.parse_args()
    
    # 自定义保存路径
    save_path = input("请输入保存路径（留空使用默认路径）: ").strip()
    if not save_path:
        save_path = DEFAULT_SAVE_PATH
    
    logger.info(f"使用保存路径: {save_path}")
    
    # 初始化爬虫
    crawler = SifangCrawler(save_path, args.verify, args.use_proxy)
    
    # 开始下载
    crawler.download()

if __name__ == '__main__':
    main()
