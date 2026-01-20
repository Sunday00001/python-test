#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import os
import time
import random
import re
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import zipfile
from PIL import Image
import io
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.text import Text

# 初始化rich控制台
console = Console()

# 全局状态字典，用于存储下载状态
download_status = {}
# 全局计数器
total_albums_count = 0
processed_albums_count = 0

# 全局配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

session = requests.Session()
session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20))
session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20))

def update_download_status(album_name, status, progress=0, tag_name="", speed=0):
    """更新下载状态"""
    global download_status
    download_status[album_name] = {
        'tag': tag_name,
        'status': status,
        'progress': progress,
        'speed': speed
    }

def get_stats_text():
    """获取统计信息文本"""
    global total_albums_count, processed_albums_count
    
    # 避免除以零错误
    if total_albums_count > 0:
        progress_percent = processed_albums_count / total_albums_count * 100
    else:
        progress_percent = 0
    
    return f"[bold white on blue]总计: {total_albums_count} 个相册 | 已处理: {processed_albums_count} 个 | 进度: {progress_percent:.1f}%[/bold white on blue]"

def create_status_table():
    """创建状态表格"""
    table = Table(title="相册下载状态", show_header=True, header_style="bold magenta")
    table.add_column("标签", width=20, style="cyan")
    table.add_column("相册名称", width=40, style="green")
    table.add_column("进度", width=15, style="yellow")
    table.add_column("速度", width=15, style="red")
    table.add_column("状态", width=20, style="blue")
    
    # 按照状态排序：等待下载 -> 正在下载 -> 下载完成 -> 跳过
    status_order = {"等待下载": 0, "正在下载": 1, "下载完成": 2, "跳过，本地已存在": 3}
    sorted_albums = sorted(
        download_status.items(),
        key=lambda x: (status_order.get(x[1]['status'], 4), x[0])
    )
    
    if sorted_albums:
        for album_name, status_info in sorted_albums:
            # 格式化进度
            if status_info['status'] in ["正在下载", "下载完成", "跳过，本地已存在"]:
                progress_str = f"{status_info['progress']:.1f}%"
            else:
                progress_str = "-"
            
            # 格式化速度，确保实时更新
            if status_info['status'] == "正在下载":
                speed = status_info.get('speed', 0)
                if speed < 1024:
                    speed_str = f"{speed:.1f} KB/s"
                else:
                    speed_str = f"{speed/1024:.1f} MB/s"
            else:
                speed_str = "-"
            
            # 根据状态设置不同的颜色
            status_text = status_info['status']
            if status_info['status'] == "下载完成":
                status_text = Text(status_text, style="green bold")
            elif status_info['status'] == "正在下载":
                status_text = Text(status_text, style="yellow bold")
            elif status_info['status'] == "等待下载":
                status_text = Text(status_text, style="blue")
            elif status_info['status'] == "跳过，本地已存在":
                status_text = Text(status_text, style="cyan bold")
            
            table.add_row(
                status_info['tag'],
                album_name,
                progress_str,
                speed_str,
                status_text
            )
    else:
        # 添加一行提示信息，避免表格看起来是空的
        table.add_row(
            "等待中",
            "暂无相册信息",
            "-",
            "-",
            "等待下载",
            style="italic dim"
        )
    
    return table

def get_soup(url):
    """获取网页的BeautifulSoup对象"""
    try:
        response = session.get(url, headers=HEADERS, timeout=30)
        response.encoding = 'gb2312'
        return BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        console.print(f"[red]获取页面 {url} 失败: {e}[/red]")
        return None

def get_tags():
    """获取所有标签链接和名称"""
    tags = []
    url = 'https://www.ku1372.cc/b/tag/'
    soup = get_soup(url)
    if not soup:
        return tags
    
    # 找到所有包含标签的ul列表
    ul_list = soup.find_all('ul')
    for ul in ul_list:
        li_list = ul.find_all('li')
        for li in li_list:
            a_tag = li.find('a')
            span_tag = li.find('span')
            if a_tag and span_tag:
                tag_url = a_tag.get('href')
                tag_name = a_tag.text.strip()
                tag_count = span_tag.text.strip()
                tags.append({
                    'name': f"{tag_name} {tag_count}",
                    'url': tag_url
                })
    return tags

def get_albums(tag_url):
    """获取标签下的所有相册链接"""
    albums = []
    page = 1
    
    while True:
        # 构建页码URL
        if page == 1:
            url = tag_url
        else:
            # 提取tag_id，实际分页URL格式为list_{tag_id}_{page}.html
            # 从tag_url中提取数字ID
            import re
            tag_id_match = re.search(r'/b/(\d+)/?', tag_url)
            if tag_id_match:
                tag_id = tag_id_match.group(1)
                # 检查URL格式，构建正确的分页URL
                if tag_url.endswith('/'):
                    url = f"{tag_url}list_{tag_id}_{page}.html"
                else:
                    url = f"{tag_url}/list_{tag_id}_{page}.html"
            else:
                # 如果无法提取tag_id，使用之前的格式作为备选
                if tag_url.endswith('/'):
                    url = f"{tag_url}list_{page}.html"
                else:
                    url = f"{tag_url}/list_{page}.html"
        
        soup = get_soup(url)
        if not soup:
            break
        
        # 查找相册列表
        list_div = soup.find('div', class_='m-list')
        if not list_div:
            break
        
        # 查找所有相册链接
        li_list = list_div.find_all('li')
        if not li_list:
            break
        
        for li in li_list:
            a_tag = li.find('a')
            if a_tag:
                album_url = a_tag.get('href')
                album_name = a_tag.get('title', '').strip()
                albums.append({
                    'name': album_name,
                    'url': album_url
                })
        
        # 检查是否有下一页
        page_div = soup.find('div', class_='page')
        if not page_div:
            print(f"未找到分页控件，结束爬取 (第{page}页)")
            break
        
        # 尝试多种方式查找下一页
        next_page = None
        
        # 1. 通过文本匹配查找下一页
        text_matches = page_div.find_all('a')
        for a in text_matches:
            a_text = a.text.strip()
            if a_text in ['下一页', 'ÏÂÒ»Ò³', 'Next', 'next'] or '下一页' in a_text:
                next_page = a
                break
        
        # 2. 如果文本匹配失败，查找包含href且href中包含list的链接
        if not next_page:
            href_matches = page_div.find_all('a', href=re.compile(r'list_'))
            for a in href_matches:
                # 排除当前页链接
                if 'this-page' not in a.get('class', []):
                    # 尝试解析页码，找到比当前页大1的页码
                    page_match = re.search(r'_([0-9]+)\.html', a.get('href', ''))
                    if page_match:
                        link_page = int(page_match.group(1))
                        if link_page == page + 1:
                            next_page = a
                            break
        
        if not next_page:
            print(f"未找到下一页链接，结束爬取 (第{page}页)")
            break
        
        page += 1
        print(f"准备爬取下一页: 第{page}页")
        # 增加页面爬取延迟
        time.sleep(random.randint(4, 8))
    
    return albums

def get_download_link(album_url):
    """获取相册的下载链接"""
    soup = get_soup(album_url)
    if not soup:
        return None
    
    # 查找下载链接
    title_div = soup.find('div', class_='Title111')
    if not title_div:
        return None
    
    download_a = title_div.find('a', text=re.compile(r'点击打包下载本套图|µã»÷´ò°üÏÂÔØ±¾Ì×Í¼'))
    if download_a:
        return download_a.get('href')
    
    return None

def extract_zip(zip_path, extract_dir, delete_after=False):
    """解压单个压缩包"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"解压成功: {os.path.basename(zip_path)}")
        
        if delete_after:
            os.remove(zip_path)
            print(f"删除原压缩包: {os.path.basename(zip_path)}")
        
        return True
    except Exception as e:
        print(f"解压失败 {os.path.basename(zip_path)}: {e}")
        return False

def verify_image(image_path):
    """验证图像是否损坏"""
    try:
        with Image.open(image_path) as img:
            img.verify()
        return True
    except Exception as e:
        print(f"图像损坏: {image_path} - {e}")
        return False

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='爬取ku1372网站相册')
    parser.add_argument('--verify', action='store_true', help='启用已存在文件验证')
    parser.add_argument('--max-workers', type=int, default=2, help='最大下载线程数')
    args = parser.parse_args()
    
    # 获取保存路径（使用原始字符串避免转义警告）
    default_path = r"E:\pachong\结果\ku1372"
    save_path = input(f"请输入保存路径（默认：{default_path}）: ").strip()
    if not save_path:
        save_path = default_path
    
    # 创建保存目录
    os.makedirs(save_path, exist_ok=True)
    
    # 询问解压选项（在下载开始之前）
    console.print("\n=== 解压选项设置 ===")
    extract_choice = input("全站下载完成后是否解压压缩包？(y/n，默认y): ").strip().lower()
    should_extract = extract_choice in ['y', '']
    delete_after = False
    
    if should_extract:
        delete_choice = input("解压完成后是否删除原压缩包？(y/n，默认y): ").strip().lower()
        delete_after = delete_choice in ['y', '']
    
    # 获取所有标签
    tags = get_tags()
    if not tags:
        console.print("[red]获取标签失败，程序退出[/red]")
        return
    
    console.print(f"\n[bold cyan]共找到 {len(tags)} 个标签[/bold cyan]")
    
    # 开始下载，使用Live显示动态表格和统计信息
    total_success = 0
    
    # 创建Live上下文，一开始就显示统计信息和表格
    with Live(None, refresh_per_second=2, console=console) as live:
        # 定义内容渲染函数
        def render_content():
            """渲染统计信息和表格"""
            from rich.console import Group
            
            # 获取统计信息和表格
            table = create_status_table()
            
            # 使用Group来组合多个Renderable对象
            group = Group(
                get_stats_text(),
                "",
                table
            )
            
            return group
        
        # 初始化Live显示
        live.update(render_content())
        # 遍历所有标签
        for tag_index, tag in enumerate(tags):
            tag_name = tag['name']
            console.print(f"\n[bold magenta]=== 开始处理标签 {tag_index+1}/{len(tags)}: {tag_name} ===[/bold magenta]")
            
            # 初始化标签目录
            tag_dir = os.path.join(save_path, tag_name)
            os.makedirs(tag_dir, exist_ok=True)
            
            # 分页爬取和下载
            page = 1
            has_more_pages = True
            
            while has_more_pages:
                console.print(f"[yellow]正在爬取第 {page} 页相册...[/yellow]")
                
                # 构建页码URL
                if page == 1:
                    current_url = tag['url']
                else:
                    # 提取tag_id
                    tag_id_match = re.search(r'/b/(\d+)/?', tag['url'])
                    if tag_id_match:
                        tag_id = tag_id_match.group(1)
                        if tag['url'].endswith('/'):
                            current_url = f"{tag['url']}list_{tag_id}_{page}.html"
                        else:
                            current_url = f"{tag['url']}/list_{tag_id}_{page}.html"
                    else:
                        console.print(f"[red]无法提取tag_id，跳过第 {page} 页[/red]")
                        break
                
                # 爬取当前页相册
                soup = get_soup(current_url)
                if not soup:
                    console.print(f"[red]爬取第 {page} 页失败[/red]")
                    break
                
                # 查找相册列表
                list_div = soup.find('div', class_='m-list')
                if not list_div:
                    console.print(f"[red]第 {page} 页未找到相册列表[/red]")
                    break
                
                # 查找所有相册链接
                li_list = list_div.find_all('li')
                if not li_list:
                    console.print(f"[red]第 {page} 页未找到相册[/red]")
                    break
                
                console.print(f"[green]第 {page} 页找到 {len(li_list)} 个相册[/green]")
                
                # 提取当前页相册
                current_page_albums = []
                for li in li_list:
                    a_tag = li.find('a')
                    if a_tag:
                        album_url = a_tag.get('href')
                        album_name = a_tag.get('title', '').strip()
                        if album_name:
                            current_page_albums.append({
                                'name': album_name,
                                'url': album_url
                            })
                
                # 将当前页相册添加到表格中
                for album in current_page_albums:
                    # 更新全局计数器
                    global total_albums_count
                    total_albums_count += 1
                    
                    # 初始化相册状态
                    update_download_status(album['name'], "等待下载", 0, tag_name)
                    
                    # 更新显示内容
                    live.update(render_content())
                
                # 定义线程安全的成功计数器
                thread_success_count = 0
                
                # 定义一个内部函数来下载相册，这样可以访问render_content
                def download_album_wrapper(album, tag_dir, verify=False, tag_name=""):
                    nonlocal thread_success_count
                    album_name = album['name']
                    album_url = album['url']
                    
                    # 清理文件名
                    safe_name = re.sub(r'[\\/:*?"<>|]', '_', album_name)
                    save_path = os.path.join(tag_dir, f"{safe_name}.zip")
                    
                    # 如果文件已存在，根据verify参数决定是否验证
                    if os.path.exists(save_path):
                        if verify:
                            update_download_status(album_name, "正在下载", 0, tag_name)
                            live.update(render_content())
                            console.print(f"[yellow]验证已存在文件: {safe_name}[/yellow]")
                            # 简单验证文件大小，大于1KB认为有效
                            if os.path.getsize(save_path) > 1024:
                                update_download_status(album_name, "跳过，本地已存在", 100, tag_name)
                                live.update(render_content())
                                return True
                            else:
                                console.print(f"[orange]文件已存在但无效，重新下载: {safe_name}[/orange]")
                                os.remove(save_path)
                        else:
                            update_download_status(album_name, "跳过，本地已存在", 100, tag_name)
                            live.update(render_content())
                            console.print(f"[green]跳过，本地已存在: {safe_name}[/green]")
                            return True
                    
                    # 重试机制，最多重试5次
                    retry_count = 0
                    max_retries = 5
                    
                    while retry_count < max_retries:
                        try:
                            # 获取下载链接
                            download_url = get_download_link(album_url)
                            if not download_url:
                                update_download_status(album_name, "下载完成", 0, tag_name)
                                live.update(render_content())
                                console.print(f"[red]获取下载链接失败: {album_name}[/red]")
                                return False
                            
                            # 添加专辑详情页请求延迟
                            delay = random.randint(2, 4)
                            console.print(f"[blue]等待 {delay} 秒后下载: {album_name}[/blue]")
                            time.sleep(delay)
                            
                            update_download_status(album_name, "正在下载", 0, tag_name)
                            live.update(render_content())
                            console.print(f"[green]正在下载: {album_name} (尝试 {retry_count+1}/{max_retries})[/green]")
                            
                            # 添加超时重试机制
                            response = session.get(download_url, headers=HEADERS, stream=True, timeout=60)
                            response.raise_for_status()  # 检查HTTP状态码
                            
                            total_size = int(response.headers.get('content-length', 0))
                            console.print(f"[cyan]文件大小: {total_size / 1024 / 1024:.2f} MB[/cyan]")
                            
                            downloaded_size = 0
                            start_time = time.time()
                            last_update_time = start_time
                            
                            with open(save_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                                        downloaded_size += len(chunk)
                                        
                                        # 计算下载速度和进度
                                        current_time = time.time()
                                        elapsed_time = current_time - start_time
                                        
                                        # 每秒更新一次状态和表格
                                        if current_time - last_update_time >= 1.0 and total_size > 0:
                                            progress = (downloaded_size / total_size) * 100
                                            
                                            # 计算下载速度（KB/s）
                                            if elapsed_time > 0:
                                                speed_kbps = (downloaded_size / elapsed_time) / 1024
                                            else:
                                                speed_kbps = 0
                                            
                                            update_download_status(album_name, "正在下载", progress, tag_name, speed_kbps)
                                            # 更新表格
                                            live.update(render_content())
                                            last_update_time = current_time
                            
                            # 下载完成后更新最终状态
                            final_time = time.time()
                            total_elapsed = final_time - start_time
                            if total_elapsed > 0:
                                avg_speed_kbps = (downloaded_size / total_elapsed) / 1024
                                console.print(f"[cyan]平均下载速度: {avg_speed_kbps:.2f} KB/s[/cyan]")
                            
                            # 验证下载内容是否为HTML错误页
                            with open(save_path, 'rb') as f:
                                content = f.read(512)  # 读取更多内容进行验证
                                if b'<!DOCTYPE html>' in content or b'<html>' in content or b'<head>' in content:
                                    update_download_status(album_name, "等待下载", 0, tag_name)
                                    live.update(render_content())
                                    console.print(f"[red]下载失败，返回HTML错误页: {album_name}[/red]")
                                    os.remove(save_path)
                                    retry_count += 1
                                    # 重试前延迟
                                    time.sleep(random.randint(4, 8))
                                    continue
                            
                            # 验证文件大小
                            final_size = os.path.getsize(save_path)
                            console.print(f"[cyan]实际下载大小: {final_size / 1024 / 1024:.2f} MB[/cyan]")
                            
                            if final_size < 1024:
                                update_download_status(album_name, "等待下载", 0, tag_name)
                                live.update(render_content())
                                console.print(f"[red]下载失败，文件太小 ({final_size} bytes): {album_name}[/red]")
                                os.remove(save_path)
                                retry_count += 1
                                # 重试前延迟
                                time.sleep(random.randint(4, 8))
                                continue
                            
                            # 验证文件完整性（简单检查）
                            if total_size > 0 and abs(final_size - total_size) > 1024:
                                update_download_status(album_name, "等待下载", 0, tag_name)
                                live.update(render_content())
                                console.print(f"[red]下载失败，文件大小不匹配 (预期: {total_size}, 实际: {final_size}): {album_name}[/red]")
                                os.remove(save_path)
                                retry_count += 1
                                # 重试前延迟
                                time.sleep(random.randint(4, 8))
                                continue
                            
                            update_download_status(album_name, "下载完成", 100, tag_name)
                            live.update(render_content())
                            console.print(f"[green]✓ 下载成功: {album_name}[/green]")
                            thread_success_count += 1
                            return True
                        except requests.exceptions.RequestException as e:
                            update_download_status(album_name, "等待下载", 0, tag_name)
                            live.update(render_content())
                            console.print(f"[red]网络请求失败 (尝试 {retry_count+1}/{max_retries}): {e}[/red]")
                            retry_count += 1
                            if os.path.exists(save_path):
                                os.remove(save_path)
                            # 重试前延迟
                            time.sleep(random.randint(4, 8))
                        except Exception as e:
                            update_download_status(album_name, "等待下载", 0, tag_name)
                            live.update(render_content())
                            console.print(f"[red]下载专辑 {album_name} 失败 (尝试 {retry_count+1}/{max_retries}): {e}[/red]")
                            retry_count += 1
                            if os.path.exists(save_path):
                                os.remove(save_path)
                            # 重试前延迟
                            time.sleep(random.randint(4, 8))
                    
                    # 5次重试失败后，继续执行
                    update_download_status(album_name, "下载完成", 0, tag_name)
                    live.update(render_content())
                    console.print(f"[red]✗ 专辑 {album_name} 下载失败，已重试5次[/red]")
                    return False
                
                # 使用线程池并发下载当前页相册
                console.print(f"[green]使用 {args.max_workers} 个线程下载当前页相册[/green]")
                with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                    # 提交所有下载任务
                    future_to_album = {
                        executor.submit(download_album_wrapper, album, tag_dir, verify=args.verify, tag_name=tag_name): album
                        for album in current_page_albums
                    }
                    
                    # 处理完成的任务
                    for future in as_completed(future_to_album):
                        album = future_to_album[future]
                        try:
                            future.result()  # 获取结果，捕获异常
                        except Exception as e:
                            console.print(f"[red]处理相册 {album['name']} 时发生异常: {e}[/red]")
                        
                        # 更新已处理计数器
                        global processed_albums_count
                        processed_albums_count += 1
                        
                        # 更新显示内容
                        live.update(render_content())
                
                # 更新总成功数
                total_success += thread_success_count
                
                # 页面下载完成后延迟
                time.sleep(random.randint(2, 5))
                
                # 检查是否有下一页
                page_div = soup.find('div', class_='page')
                if not page_div:
                    console.print(f"[yellow]第 {page} 页未找到分页控件，结束该标签爬取[/yellow]")
                    has_more_pages = False
                    break
                
                # 查找下一页链接
                next_page = None
                text_matches = page_div.find_all('a')
                for a in text_matches:
                    a_text = a.text.strip()
                    if a_text in ['下一页', 'ÏÂÒ»Ò³', 'Next', 'next'] or '下一页' in a_text:
                        next_page = a
                        break
                
                if not next_page:
                    console.print(f"[yellow]第 {page} 页未找到下一页链接，结束该标签爬取[/yellow]")
                    has_more_pages = False
                    break
                
                # 进入下一页
                page += 1
                console.print(f"[cyan]准备爬取下一页: 第 {page} 页[/cyan]")
                
                # 页面爬取间隔
                time.sleep(random.randint(4, 8))
            
            console.print(f"[bold magenta]=== 标签 {tag_index+1}/{len(tags)} 处理完成 ===[/bold magenta]")
    
    # 执行解压操作（如果用户选择了解压）
    if should_extract:
        console.print("\n[bold blue]=== 开始解压压缩包 ===[/bold blue]")
        # 遍历所有标签目录
        for tag in tags:
            tag_dir = os.path.join(save_path, tag['name'])
            if os.path.exists(tag_dir):
                for file in os.listdir(tag_dir):
                    if file.endswith('.zip'):
                        zip_path = os.path.join(tag_dir, file)
                        extract_zip(zip_path, tag_dir, delete_after)
    
    # 总结数据
    console.print(f"\n[bold green]=== 下载完成 ===[/bold green]")
    console.print(f"[cyan]总标签数: {len(tags)}[/cyan]")
    console.print(f"[green]总成功下载数: {total_success}[/green]")
    console.print(f"[yellow]总处理相册数: {processed_albums_count}[/yellow]")

if __name__ == "__main__":
    main()