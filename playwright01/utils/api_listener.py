import json
from typing import Optional, Callable, Dict, Any
from urllib.parse import parse_qs
from playwright.sync_api import TimeoutError, Error, Response
from playwright01.utils.logger import *

"""
@Author: glm-5
@Date: 2026-04-05 10:00:00
@Description: 监听api
"""

class ApiResponseListener:
    def __init__(self, page, max_body_size: int = 1024 * 1024):
        """
        :param page: Playwright 的 Page 对象
        :param max_body_size: 允许捕获的最大响应体大小(字节)，默认 1MB，防止大文件导致内存溢出
        """
        self.page = page
        self.max_body_size = max_body_size
        self.api_call_info: Optional[Dict[str, Any]] = None

    def wait_for_api_call(
            self,
            target_url: str,
            trigger_action: Optional[Callable[[], None]] = None,
            timeout: int = 10000
    ) -> Optional[Dict[str, Any]]:
        """
        监听并捕获包含指定URL片段的API调用。
        :param target_url: URL中必须包含的片段
        :param trigger_action: 触发API请求的函数
        :param timeout: 等待响应的超时时间（毫秒）
        :return: 成功返回包含完整信息的字典，失败返回 None
        """
        url_pattern = f"**{target_url}**"

        try:
            logger.debug(f"开始监听 API 调用，目标URL包含: {target_url}，超时: {timeout}ms")

            with self.page.expect_response(url_pattern, timeout=timeout) as response_info:
                if trigger_action is not None:
                    logger.debug("执行触发动作...")
                    trigger_action()

            response: Response = response_info.value

            # 判断响应体大小，防止抓取大文件（如视频、大图片下载）导致卡死
            content_length = response.headers.get("content-length")
            if content_length and int(content_length) > self.max_body_size:
                logger.warning(f"响应体过大 ({content_length} bytes)，跳过 body 解析。URL: {response.url}")
                resp_body = f"[响应体过大，已跳过。大小: {content_length} bytes]"
            else:
                resp_body = self._parse_response_body(response)

            request = response.request
            req_body_info = self._parse_request_body(request)

            # 统一构建完整的数据结构
            self.api_call_info = {
                "status": response.status,
                "url": response.url,
                "request": {
                    "method": request.method,
                    "headers": dict(request.headers),
                    **req_body_info
                },
                "response": {
                    "headers": dict(response.headers),
                    "body": resp_body
                }
            }

            logger.info(f"成功捕获 API: [{response.status}] {request.method} {response.url}")
            logger.debug(f"请求数据: {req_body_info}")
            logger.debug(f"响应数据: {str(resp_body)[:500]}...")  # 日志中只打印前500字符

            return self.api_call_info

        except Error as e:
            if "Timeout" in str(e):
                logger.error(f"监听超时！在 {timeout}ms 内未捕获到包含 '{target_url}' 的响应。请检查 Network 面板。")
            else:
                logger.error(f"Playwright 错误: {e}")
            return None
        except Exception as e:
            logger.error(f"未知异常: {e}", exc_info=True)
            return None

    def _parse_request_body(self, request) -> Dict[str, Any]:
        """解析请求体"""
        body_info = {"post_data_raw": request.post_data, "post_data_json": None, "post_data_form": None}
        if not request.post_data:
            return body_info

        content_type = request.headers.get("content-type", "").lower()
        try:
            if "application/json" in content_type:
                body_info["post_data_json"] = json.loads(request.post_data)
            elif "application/x-www-form-urlencoded" in content_type:
                parsed = parse_qs(request.post_data)
                body_info["post_data_form"] = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
        except Exception as e:
            logger.debug(f"解析请求体失败: {e}")

        return body_info

    def _parse_response_body(self, response: Response) -> Any:
        """安全地解析响应体"""
        try:
            # 优先尝试解析 JSON
            return response.json()
        except Exception:
            try:
                text = response.text()
                # 如果是纯文本或 HTML，做一下截断保护
                return text if len(text) <= self.max_body_size else f"[文本过长，已截断] {text[:self.max_body_size]}"
            except Exception as e:
                logger.debug(f"解析响应文本失败: {e}")
                return "[无法解析响应体]"

    # --- 便捷的 Getter 方法 ---
    def get_info(self) -> Optional[Dict[str, Any]]:
        return self.api_call_info

    def get_response_status(self) -> Optional[int]:
        return self.api_call_info.get("status") if self.api_call_info else None

    def get_response_url(self) -> Optional[str]:
        return self.api_call_info.get("url") if self.api_call_info else None

    def get_request_data(self) -> Optional[Dict[str, Any]]:
        return self.api_call_info.get("request") if self.api_call_info else None

    def get_response_data(self) -> Any:
        if self.api_call_info:
            return self.api_call_info.get("response", {}).get("body")
        return None


def capture_api_call(
        page,
        target_url: str,
        trigger_action: Callable[[], None],
        timeout: int = 15000
) -> Optional[Dict[str, Any]]:
    """
    便捷函数：一键监听并捕获API调用。
    :param page: Playwright Page 对象
    :param target_url: 目标API URL片段
    :param trigger_action: 触发API请求的函数
    :param timeout: 超时时间(毫秒)
    :return: 成功时返回包含接口详情的字典，失败时返回 None
    """
    listener = ApiResponseListener(page)
    return listener.wait_for_api_call(
        target_url=target_url,
        trigger_action=trigger_action,
        timeout=timeout
    )