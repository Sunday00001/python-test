import os
import time
import random
import requests
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MeituSpider:
    def __init__(self, save_path, verify=False, page_sleep=5, album_sleep=3):
        self.save_path = save_path
        self.verify = verify
        self.page_sleep = page_sleep
        self.album_sleep = album_sleep
        self.session = self._init_session()
        self.base_url = "https://xn--drdgbhrb-xx6n10qjm3s.tljkd-01.sbs"
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"
        ]
        self.failed_images = []
        self.failed_albums = []
    
    def _init_session(self):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _get_random_user_agent(self):
        return random.choice(self.user_agents)
    
    def _get_response(self, url, retries=5):
        headers = {
            "User-Agent": self._get_random_user_agent(),
            "Referer": self.base_url
        }
        
        for i in range(retries):
            start_time = time.time()
            try:
                logger.debug(f"[请求] 开始请求: {url}")
                logger.debug(f"[请求] 请求头: {headers}")
                response = self.session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                request_time = time.time() - start_time
                logger.info(f"[请求] {url} 成功，耗时 {request_time:.2f}秒")
                
                delay = random.uniform(4, 8)
                logger.info(f"[请求] {url} 成功，{delay:.2f}秒后继续")
                time.sleep(delay)
                return response
            except Exception as e:
                request_time = time.time() - start_time
                if i < retries - 1:
                    delay = random.uniform(4, 8)
                    logger.warning(f"[请求] {url} 失败，{i+1}/{retries} 重试，耗时 {request_time:.2f}秒，{delay:.2f}秒后重试: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"[请求] {url} 失败，{retries} 次重试后仍失败，总耗时 {request_time:.2f}秒: {e}")
                    return None
    
    def _parse_albums(self, url):
        response = self._get_response(url)
        
        if not response:
            logger.error(f"[解析] 请求失败，无法解析页面: {url}")
            return [], None
        
        start_parse_time = time.time()
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            logger.info(f"[解析] 开始解析相册列表: {url}")
            
            albums = []
            album_elements = soup.select(".videos-list-wrap .video-item-col")
            logger.info(f"[解析] 找到 {len(album_elements)} 个相册元素")
            
            for i, album in enumerate(album_elements):
                try:
                    album_url = album.get("href")
                    album_title = album.select_one(".video-desc-content").text.strip()
                    if album_url and album_title:
                        full_url = f"{self.base_url}{album_url}"
                        albums.append((album_title, full_url))
                        logger.debug(f"[解析] 解析相册 {i+1}/{len(album_elements)}: {album_title} - {full_url}")
                    else:
                        logger.debug(f"[解析] 跳过无效相册元素: {album}")
                except Exception as e:
                    logger.error(f"[解析] 解析相册 {i+1}/{len(album_elements)} 失败: {e}")
            
            next_page = None
            next_page_element = soup.select_one(".mo-paging .paging-item--next")
            if next_page_element and next_page_element.get("href"):
                next_page = f"{self.base_url}{next_page_element.get('href')}"
                logger.info(f"[解析] 找到下一页链接: {next_page}")
            else:
                logger.info(f"[解析] 未找到下一页链接")
            
            parse_time = time.time() - start_parse_time
            logger.info(f"[解析] 相册列表解析完成，耗时 {parse_time:.2f}秒，共解析到 {len(albums)} 个相册")
            
            return albums, next_page
        except Exception as e:
            logger.error(f"[解析] 解析页面失败: {url}, 错误: {e}")
            return [], None
    
    def _parse_album_images(self, album_url):
        start_parse_time = time.time()
        logger.info(f"[解析] 开始解析相册图片: {album_url}")
        
        response = self._get_response(album_url)
        soup = BeautifulSoup(response.text, "html.parser")
        
        book_pages = soup.select_one("#book-pages")
        if not book_pages:
            logger.error(f"[解析] 未找到相册图片容器: {album_url}")
            return []
        
        screenshots = book_pages.get("data-screenshots", "")
        if not screenshots:
            logger.error(f"[解析] 未找到相册图片URL: {album_url}")
            return []
        
        images = []
        for img_url in screenshots.split("#$"):
            img_url = img_url.strip()
            if img_url:
                if img_url.startswith('$'):
                    img_url = img_url[1:]
                images.append(img_url)
        parse_time = time.time() - start_parse_time
        logger.info(f"[解析] 从相册 {album_url} 中提取到 {len(images)} 张图片，耗时 {parse_time:.2f}秒")
        
        if images:
            logger.debug(f"[解析] 前5张图片URL: {images[:5]}")
        
        return images
    
    def _validate_image(self, image_path):
        try:
            with Image.open(image_path) as img:
                img.verify()
            return True
        except Exception:
            return False
    
    def _download_image(self, img_url, save_path):
        if os.path.exists(save_path):
            logger.info(f"[下载] 图片已存在，跳过: {save_path}")
            return True
        
        start_download_time = time.time()
        logger.info(f"[下载] 开始下载: {img_url}")
        
        response = self._get_response(img_url)
        
        if response is None:
            logger.error(f"[下载] 请求失败，添加到失败列表: {img_url}")
            self.failed_images.append((img_url, save_path))
            return False
        
        content_type = response.headers.get("Content-Type", "")
        if not content_type.startswith("image/"):
            logger.error(f"[下载] 下载的不是图片，可能是错误页: {img_url}")
            self.failed_images.append((img_url, save_path))
            return False
        
        file_size = len(response.content)
        logger.info(f"[下载] 图片大小: {file_size} 字节，类型: {content_type}")
        
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
        logger.info(f"[下载] 确保保存目录存在: {save_dir}")
        
        temp_path = f"{save_path}.tmp"
        logger.info(f"[下载] 正在写入文件: {temp_path}")
        
        try:
            start_write_time = time.time()
            with open(temp_path, "wb") as f:
                f.write(response.content)
            write_time = time.time() - start_write_time
            logger.info(f"[下载] 文件写入完成，耗时 {write_time:.2f}秒")
        except Exception as e:
            logger.error(f"[下载] 文件写入失败: {e}")
            self.failed_images.append((img_url, save_path))
            return False
        
        if self.verify:
            start_verify_time = time.time()
            logger.info(f"[下载] 正在验证文件: {temp_path}")
            try:
                if not self._validate_image(temp_path):
                    logger.error(f"[下载] 下载的图片损坏: {img_url}")
                    os.remove(temp_path)
                    self.failed_images.append((img_url, save_path))
                    return False
                verify_time = time.time() - start_verify_time
                logger.info(f"[下载] 文件验证完成，耗时 {verify_time:.2f}秒")
            except Exception as e:
                logger.error(f"[下载] 文件验证失败: {e}")
                os.remove(temp_path)
                self.failed_images.append((img_url, save_path))
                return False
        
        try:
            os.rename(temp_path, save_path)
            total_time = time.time() - start_download_time
            logger.info(f"[下载] 下载完成: {save_path}，总耗时 {total_time:.2f}秒")
            return True
        except Exception as e:
            logger.error(f"[下载] 文件重命名失败: {e}")
            os.remove(temp_path)
            self.failed_images.append((img_url, save_path))
            return False
    
    def _download_album(self, album_info, total_albums, album_index):
        album_title, album_url = album_info
        start_album_time = time.time()
        logger.info(f"[专辑 {album_index+1}/{total_albums}] 开始处理相册: {album_title}")
        
        safe_title = "".join([c for c in album_title if c not in '<>"/\\|?*'])[:50]
        album_dir = os.path.join(self.save_path, safe_title)
        os.makedirs(album_dir, exist_ok=True)
        logger.info(f"[专辑 {album_index+1}/{total_albums}] 相册目录: {album_dir}")
        
        images = self._parse_album_images(album_url)
        if not images:
            logger.error(f"[专辑 {album_index+1}/{total_albums}] 未解析到图片: {album_url}")
            self.failed_albums.append(album_info)
            return False
        
        logger.info(f"[专辑 {album_index+1}/{total_albums}] 开始下载 {len(images)} 张图片")
        
        start_image_time = time.time()
        image_failures = []
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for img_index, img_url in enumerate(images):
                img_name = f"{img_index+1:03d}.jpg"
                img_path = os.path.join(album_dir, img_name)
                futures.append(executor.submit(self._download_image, img_url, img_path))
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if not result:
                        image_failures.append(result)
                except Exception as e:
                    logger.error(f"[专辑 {album_index+1}/{total_albums}] 下载图片时发生异常: {e}")
                    image_failures.append(None)
        
        image_time = time.time() - start_image_time
        album_time = time.time() - start_album_time
        logger.info(f"[专辑 {album_index+1}/{total_albums}] 相册处理完成，共下载 {len(images) - len(image_failures)} 张图片，耗时 {album_time:.2f}秒")
        logger.info(f"[专辑 {album_index+1}/{total_albums}] 其中图片下载耗时 {image_time:.2f}秒")
        
        return True
    
    def _retry_failed_images(self):
        if not self.failed_images:
            return
        
        logger.info(f"[主程序] 开始重试 {len(self.failed_images)} 张失败的图片")
        success_count = 0
        
        for img_url, save_path in self.failed_images:
            if self._download_image(img_url, save_path):
                success_count += 1
        
        logger.info(f"[主程序] 图片重试完成，成功 {success_count}/{len(self.failed_images)} 张")
    
    def _retry_failed_albums(self):
        if not self.failed_albums:
            return
        
        logger.info(f"[主程序] 开始重试 {len(self.failed_albums)} 个失败的相册")
        retry_count = 0
        
        for album_info in self.failed_albums:
            if self._download_album(album_info, len(self.failed_albums), retry_count):
                retry_count += 1
        
        logger.info(f"[主程序] 相册重试完成，成功 {retry_count}/{len(self.failed_albums)} 个")
    
    def run(self):
        start_total_time = time.time()
        logger.info("[主程序] 开始爬取美图色色网站")
        logger.info(f"[主程序] 保存路径: {self.save_path}")
        logger.info(f"[主程序] 验证选项: {self.verify}")
        logger.info(f"[主程序] 列表页延迟: {self.page_sleep}秒")
        logger.info(f"[主程序] 专辑页延迟: {self.album_sleep}秒")
        
        logger.info(f"[主程序] 正在创建保存目录: {self.save_path}")
        os.makedirs(self.save_path, exist_ok=True)
        logger.info(f"[主程序] 保存目录创建完成")
        
        all_albums = []
        current_url = "https://xn--drdgbhrb-xx6n10qjm3s.tljkd-01.sbs/t/13/"
        page = 1
        total_pages = 0
        processed_urls = set()
        
        start_list_time = time.time()
        logger.info(f"[主程序] 开始爬取相册列表")
        
        while current_url and current_url not in processed_urls:
            processed_urls.add(current_url)
            logger.info(f"[主程序] 正在爬取第 {page} 页相册列表: {current_url}")
            albums, next_page = self._parse_albums(current_url)
            all_albums.extend(albums)
            logger.info(f"[主程序] 第 {page} 页解析到 {len(albums)} 个相册")
            
            for i, (album_title, album_url) in enumerate(albums):
                logger.info(f"[主程序]   发现相册 {i+1}: {album_title} - {album_url}")
            
            total_pages += 1
            
            if next_page and next_page not in processed_urls:
                current_url = next_page
                page += 1
                logger.info(f"[主程序] 列表页爬取延迟 {self.page_sleep}秒")
                time.sleep(self.page_sleep)
            else:
                if next_page and next_page in processed_urls:
                    logger.warning(f"[主程序] 发现重复的下一页链接，停止翻页: {next_page}")
                current_url = None
        
        list_time = time.time() - start_list_time
        total_albums = len(all_albums)
        logger.info(f"[主程序] 相册列表爬取完成，共爬取 {total_pages} 页，解析到 {total_albums} 个相册，耗时 {list_time:.2f}秒")
        
        start_download_time = time.time()
        failed_albums = []
        logger.info(f"[主程序] 开始下载相册，最多同时处理5个相册")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            logger.info(f"[主程序] 线程池创建完成，最大工作线程数: 5")
            futures = []
            
            start_submit_time = time.time()
            for album_index, album_info in enumerate(all_albums):
                futures.append(executor.submit(self._download_album, album_info, total_albums, album_index))
            submit_time = time.time() - start_submit_time
            logger.info(f"[主程序] 所有任务提交完成，共 {len(futures)} 个任务，耗时 {submit_time:.2f}秒")
            
            logger.info(f"[主程序] 开始处理任务结果")
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result is not None and not result:
                        failed_albums.append(result)
                except Exception as e:
                    logger.error(f"[主程序] 处理相册失败: {e}")
        
        if failed_albums:
            logger.info(f"[主程序] 共有 {len(failed_albums)} 个相册下载失败，开始重试")
            retry_futures = []
            with ThreadPoolExecutor(max_workers=3) as executor:
                for album_info in failed_albums:
                    retry_futures.append(executor.submit(self._download_album, album_info, len(failed_albums), failed_albums.index(album_info)))
            
            for future in as_completed(retry_futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[主程序] 重试相册失败: {e}")
        
        download_time = time.time() - start_download_time
        total_time = time.time() - start_total_time
        logger.info(f"[主程序] 所有相册爬取完成，共耗时 {total_time:.2f}秒")
        logger.info(f"[主程序] 其中: 列表爬取耗时 {list_time:.2f}秒，相册下载耗时 {download_time:.2f}秒")
        logger.info(f"[主程序] 平均每个相册处理时间: {download_time/total_albums:.2f}秒")
        
        total_failed = len(self.failed_albums) + len(self.failed_images)
        if total_failed > 0:
            logger.info(f"[主程序] 共有 {len(self.failed_albums)} 个相册和 {len(self.failed_images)} 张图片下载失败")
            
            retry_input = input("是否重试因失败跳过的图集？(y/N): ").strip().lower()
            if retry_input in ['y', '']:
                logger.info("[主程序] 开始重试失败的图集")
                
                if self.failed_albums:
                    self._retry_failed_albums()
                
                if self.failed_images:
                    self._retry_failed_images()
                
                logger.info("[主程序] 重试完成")
            else:
                logger.info("[主程序] 不重试失败的图集，运行结束")
        else:
            logger.info("[主程序] 所有图集下载成功，无失败项")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="美图色色相册爬虫")
    parser.add_argument("--page-sleep", type=float, default=5, help="列表页爬取延迟")
    parser.add_argument("--album-sleep", type=float, default=3, help="专辑详情页请求延迟")
    parser.add_argument("--no-verify", action="store_true", help="不验证文件，仅下载")
    parser.add_argument("--test", action="store_true", help="测试模式，使用默认路径无需交互")
    args = parser.parse_args()
    
    if args.test:
        save_path = "E:\pachong\结果\美图色色"
        logger.info(f"[测试模式] 使用默认保存路径: {save_path}")
    else:
        user_input_path = input("请输入保存路径 (留空使用默认路径 E:\pachong\结果\美图色色): ").strip()
        save_path = user_input_path if user_input_path else "E:\pachong\结果\美图色色"
    
    verify = not args.no_verify
    
    spider = MeituSpider(
        save_path=save_path,
        verify=verify,
        page_sleep=args.page_sleep,
        album_sleep=args.album_sleep
    )
    
    spider.run()