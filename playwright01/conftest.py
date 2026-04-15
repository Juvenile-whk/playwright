# ============================================================================
# 标准库
# ============================================================================
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
import math
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, cast
from urllib.parse import urlsplit

# ============================================================================
# 第三方库
# ============================================================================
import allure
import pytest
import requests
from allure import step
from bs4 import BeautifulSoup
from filelock import FileLock, Timeout
from playwright.sync_api import Browser, BrowserContext, Error, Page, Playwright, expect
from playwright.sync_api._generated import Locator as _Locator
from playwright._impl._locator import Locator as LocatorImpl
from playwright._impl._sync_base import mapping
from pytest_playwright.pytest_playwright import CreateContextCallback
from slugify import slugify

# ============================================================================
# 项目模块
# ============================================================================
from playwright01.data_module.globalconfig import *  # 如果需要访问 globalconfig.xxx
from playwright01.utils.GetPath import get_path
from playwright01.utils.bug_helper import BugHelper
from playwright01.utils.globalMap import GlobalMap
from playwright01.utils.logger import *
from playwright01.utils.my_date import return_time_add_days
from playwright01.utils.pattern_util import clean_title, clean_title_simple




api_Count = []
time_out = 60000 # 全局时间，Locator类使用
# 统一缓存本次会话的接口性能记录（仅 xhr/fetch）
_api_perf_records: List[Dict[str, Any]] = []


def get_page_title(page: Page) -> str:
    """兼容 page.title 为方法或字符串属性的场景。"""
    title_attr = getattr(page, "title", "")
    if callable(title_attr):
        try:
            return title_attr() or ""
        except Exception:
            return ""
    return title_attr or ""

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """在每个测试开始前设置日志格式"""
    # 设置 pytest 运行时的日志格式
    # 影响 Allure 报告中显示的日志格式
    # 通过修改 root logger 的处理器格式来实现
    import logging
    # 确保 root logger 使用我们想要的格式
    formatter = logging.Formatter("%(levelname)s %(asctime)s %(name)s:%(filename)s:%(lineno)d %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

# @pytest.fixture
# def page(context: BrowserContext) -> Page:
#     print("this is my page")
#     return context.new_page()

@pytest.fixture(scope="session", autouse=True)
def test_init(base_url):
    global_map = GlobalMap()
    global_map.set("baseurl", base_url)
    env = re.search("(https://www.)(.*)(.com)", base_url).group(2)
    global_map.set("env", env)


@pytest.fixture(scope="session")
def browser_context_args(
        pytestconfig: Any,
        playwright: Playwright,
        device: Optional[str],
        base_url: Optional[str],
        # _pw_artifacts_folder: tempfile.TemporaryDirectory,
) -> Dict:
    width, height = pytestconfig.getoption("--viewport")
    context_args = {}
    if device:
        context_args.update(playwright.devices[device])
    if base_url:
        context_args["base_url"] = base_url
    # video_option = pytestconfig.getoption("--video")
    # capture_video = video_option in ["on", "retain-on-failure"]
    # if capture_video:
    #     context_args["record_video_dir"] = _pw_artifacts_folder.name
    return {
        **context_args,
        "viewport": {
            "width": width,
            "height": height,
        },
        "record_video_size": {
            "width": width,
            "height": height,
        },
    }


def pytest_addoption(parser: Any) -> None:
    group = parser.getgroup("playwright", "Playwright")
    group.addoption(
        "--viewport",
        action="store",
        # default=[1920, 1080],
        default=[1024, 768],
        help="viewport size set",
        type=int,
        nargs=2,
    )
    group.addoption(
        "--ui_timeout",
        default=30_000,
        help="locator timeout and expect timeout",
    )
    group.addoption(
        "--rerun_strategy",
        action="store",
        default=None,
        #  这里不使用nargs="*"是因为无限个args对参数的位置有要求,或者测试目标需要用参数指定
        help="testcase rerun strategy set, eg: screenshot=retain-on-failure,video=retain-on-failure,tracing=retain-on-failure",
    )
    group.addoption(
        "--allure_report_auto_open",
        action="store",
        default="off",
        help="if finish test, allure report auto open, eg: /Users/liuyunlong/Desktop/pw-allure",
    )


@pytest.fixture(scope="session")
def ui_timeout(pytestconfig):
    timeout = float(pytestconfig.getoption("--ui_timeout"))
    expect.set_options(timeout=timeout)
    global time_out
    time_out = float(pytestconfig.getoption("--ui_timeout"))
    return timeout


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item):
    outcome = yield
    report = outcome.get_result()
    if report.failed:
        try:
            for context in item.funcargs['browser'].contexts:
                for page in context.pages:
                    if page.is_closed():
                        continue
                    bytes_png = page.screenshot(timeout=5000, full_page=True)
                    allure.attach(bytes_png, f"失败截图---{get_page_title(page)}")
        except:
            ...


@pytest.fixture()
def _artifacts_recorder(
        request: pytest.FixtureRequest,
        playwright: Playwright,
        pytestconfig: Any,
        _pw_artifacts_folder: tempfile.TemporaryDirectory,
) -> Generator["ArtifactsRecorder", None, None]:
    _pw_artifacts_folder.name = _build_pw_artifacts_temp_dir(request)
    artifacts_recorder = ArtifactsRecorder(
        pytestconfig, request, playwright, _pw_artifacts_folder
    )
    yield artifacts_recorder
    # If request.node is missing rep_call, then some error happened during execution
    # that prevented teardown, but should still be counted as a failure
    failed = request.node.rep_call.failed if hasattr(request.node, "rep_call") else True
    artifacts_recorder.did_finish_test(failed)


def truncate_file_name(file_name: str) -> str:
    if len(file_name) < 256:
        return file_name
    return f"{file_name[:100]}-{hashlib.sha256(file_name.encode()).hexdigest()[:7]}-{file_name[-100:]}"


def _build_artifact_test_folder(
        pytestconfig: Any, request: pytest.FixtureRequest, folder_or_file_name: str
) -> str:
    output_dir = pytestconfig.getoption("--output")
    return os.path.join(
        output_dir,
        #  修改为request.node.name,以便支持中文用例名称,所有的request.node.name都是这个目的
        truncate_file_name(request.node.name),
        truncate_file_name(folder_or_file_name),
    )


def _build_pw_artifacts_temp_dir(request: pytest.FixtureRequest) -> str:
    """Build an isolated playwright artifact temp dir for this test execution."""
    project_root = Path(__file__).resolve().parents[1]
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    execution_count = getattr(request.node, "execution_count", 1)
    node_hash = hashlib.sha256(request.node.nodeid.encode("utf-8")).hexdigest()[:10]
    temp_dir = (
        project_root
        / ".temp"
        / "Temp"
        / worker_id
        / f"{node_hash}-r{execution_count}-{create_guid()[:8]}"
    )
    temp_dir.mkdir(parents=True, exist_ok=True)
    return str(temp_dir)


def _save_video_with_retry(video: Any, target_path: str, retries: int = 1, wait_seconds: float = 0.3) -> bool:
    """Save video and ensure output is non-empty; retry once for slow flush scenarios."""
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            video.save_as(path=target_path)
            if os.path.isfile(target_path) and os.path.getsize(target_path) > 0:
                return True
        except Error as exc:
            last_error = exc
        if attempt < retries:
            time.sleep(wait_seconds)

    if last_error:
        logger.warning(f"video.save_as failed after retry, path={target_path}, err={last_error}")
    else:
        size = os.path.getsize(target_path) if os.path.isfile(target_path) else -1
        logger.warning(f"video saved but file size invalid, path={target_path}, size={size}")
    return False


@pytest.fixture
def new_context(
        browser: Browser,
        browser_context_args: Dict,
        _artifacts_recorder: "ArtifactsRecorder",
        request: pytest.FixtureRequest,
        ui_timeout: float,
        pytestconfig: Any,
        _pw_artifacts_folder: tempfile.TemporaryDirectory,
) -> Generator[CreateContextCallback, None, None]:
    browser_context_args = browser_context_args.copy()
    browser_context_args["ignore_https_errors"] = True
    context_args_marker = next(request.node.iter_markers("browser_context_args"), None)
    additional_context_args = context_args_marker.kwargs if context_args_marker else {}
    browser_context_args.update(additional_context_args)
    contexts: List[BrowserContext] = []

    def _new_context(**kwargs: Any) -> BrowserContext:
        #  复制browser_context_args,防止污染参数
        browser_context_args_copy = browser_context_args.copy()
        #  获取重试的log策略并转成列表
        _rerun_strategy = pytestconfig.getoption("--rerun_strategy").split(",")
        #  获取重试次数,此处为2则为重试2次,加上第1次,一共跑3次
        _reruns = pytestconfig.getoption("--reruns")
        video_option = pytestconfig.getoption("--video")
        #  重试log策略(默认None)和重试次数(默认0)参数必须都有值
        if _rerun_strategy and _reruns:
            #  使用空字符串去补足轮次和策略的对应关系:
            if _reruns + 1 > len(_rerun_strategy):
                _init_rerun_strategy = [""] * (1 + _reruns - len(_rerun_strategy)) + _rerun_strategy
            #  使用切片来处理多余的策略(如果相等,则切片是本身),可根据自身设计改成从后往前切
            else:
                _init_rerun_strategy = _rerun_strategy[:_reruns + 1]
            #  这里减1是因为request.node.execution_count从1开始,我们取列表下标从0开始
            rerun_round = request.node.execution_count - 1
            _round_rerun_strategy = _init_rerun_strategy[rerun_round]

            #  这里先判断是否有log策略
            if _round_rerun_strategy:
                if "video" in _round_rerun_strategy:
                    video_option = _round_rerun_strategy.split("=")[-1]
                else:
                    video_option = "off"
            else:
                video_option = "off"
        #  这里只判断了video,是因为创建context时必须设置record_video_dir后才开始主动录屏
        capture_video = video_option in ["on", "retain-on-failure"]
        browser_context_args_copy.update(kwargs)
        if capture_video:
            video_option_dict = {"record_video_dir": _pw_artifacts_folder.name}
            #  字典的update可以直接传字典,也可以解包,解包相当于kwargs
            browser_context_args_copy.update(video_option_dict)
        my_context = browser.new_context(**browser_context_args_copy)
        my_context.set_default_timeout(ui_timeout)
        my_context.set_default_navigation_timeout(ui_timeout * 2)
        original_close = my_context.close

        def _close_wrapper(*args: Any, **my_kwargs: Any) -> None:
            if my_context in contexts:
                contexts.remove(my_context)
            try:
                _artifacts_recorder.on_will_close_browser_context(my_context)
            except Exception as e:
                logger.warning(f"failed to collect context artifacts before close: {e}")
            finally:
                original_close(*args, **my_kwargs)

        my_context.close = _close_wrapper
        contexts.append(my_context)
        _artifacts_recorder.on_did_create_browser_context(my_context)
        return my_context

    yield cast(CreateContextCallback, _new_context)
    for context in contexts.copy():
        context.close()

# _pw_artifacts_folder 注意这个值 当报错信息出现这种用户名导致的路径错误时 C:\\Users\\xxx_DA~1\\AppData\\Local\\Temp\\6\\playwright-pytest-4wba_jsy
class ArtifactsRecorder:
    def __init__(
            self,
            pytestconfig: Any,
            request: pytest.FixtureRequest,
            playwright: Playwright,
            pw_artifacts_folder: tempfile.TemporaryDirectory,
    ) -> None:
        self._request = request
        self._pytestconfig = pytestconfig
        self._playwright = playwright
        self._pw_artifacts_folder = pw_artifacts_folder
        # tempfile.py _candidate_tempdir_list的_os.getenv(envname)调用os.py的getenv方法
        os.makedirs(self._pw_artifacts_folder.name, exist_ok=True)
        self._all_pages: List[Page] = []
        self._screenshots: List[str] = []
        self._traces: List[str] = []
        self._rerun_strategy = pytestconfig.getoption("--rerun_strategy").split(",")
        self._reruns = pytestconfig.getoption("--reruns")
        #  这里逻辑了上面的一致,不赘述了
        if self._rerun_strategy and self._reruns:
            if self._reruns + 1 >= len(self._rerun_strategy):
                self._init_rerun_strategy = [""] * (1 + self._reruns - len(self._rerun_strategy)) + self._rerun_strategy
            else:
                self._init_rerun_strategy = self._rerun_strategy[:self._reruns + 1]

            rerun_round = request.node.execution_count - 1
            self._round_rerun_strategy = self._init_rerun_strategy[rerun_round]

            #  以下为判断log策略内容和参数的方法,注意,如果没有则设置为off
            if "screenshot" in self._round_rerun_strategy:
                self._screenshot_option = self._round_rerun_strategy.split("=")[-1]
            else:
                self._screenshot_option = "off"
            if "video" in self._round_rerun_strategy:
                self._video_option = self._round_rerun_strategy.split("=")[-1]
            else:
                self._video_option = "off"
            if "tracing" in self._round_rerun_strategy:
                self._tracing_option = self._round_rerun_strategy.split("=")[-1]
            else:
                self._tracing_option = "off"
            self._capture_trace = self._tracing_option in ["on", "retain-on-failure"]
        else:
            #  没有重试log策略和重试次数,自然取原始的log策略
            self._screenshot_option = self._pytestconfig.getoption("--screenshot")
            self._video_option = self._pytestconfig.getoption("--video")
            self._tracing_option = pytestconfig.getoption("--tracing")
            self._capture_trace = self._tracing_option in ["on", "retain-on-failure"]

    def did_finish_test(self, failed: bool) -> None:
        #  获取当前轮次并初始化一个字符串,给保存文件做前缀
        round_prefix = f"round{self._request.node.execution_count}-"
        #  这里可以学习一下组合的布尔逻辑
        capture_screenshot = self._screenshot_option == "on" or (
                failed and self._screenshot_option == "only-on-failure"
        )
        if capture_screenshot:
            for index, screenshot in enumerate(self._screenshots):
                human_readable_status = "failed" if failed else "finished"
                screenshot_path = _build_artifact_test_folder(
                    self._pytestconfig,
                    self._request,
                    #  原始为 f"test-{human_readable_status}-{index + 1}.png",
                    f"{round_prefix}{index + 1}-{human_readable_status}-{screenshot.split(os.sep)[-1]}.png",
                )
                #  这里这种写法注意下,如果自己需要放log,用这个方式创建很好
                os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
                # 路径问题
                try:
                    shutil.move(screenshot, screenshot_path)
                    print(f"File not found: {screenshot} and {screenshot_path}")
                except FileNotFoundError:
                    pass
                # allure附加图片文件的方法
                try:
                    allure.attach.file(screenshot_path, f"{round_prefix}{index + 1}-{human_readable_status}-{screenshot.split(os.sep)[-1]}.png")
                except FileNotFoundError:
                    pass
        else:
            for screenshot in self._screenshots:
                if os.path.isfile(screenshot):
                    # 路径问题
                    try:
                        os.remove(screenshot)
                    except FileNotFoundError:
                        print(f"File not found: {screenshot}")
                else:
                    print(f"File does not exist: {screenshot}")

        if self._tracing_option == "on" or (
                failed and self._tracing_option == "retain-on-failure"
        ):
            for index, trace in enumerate(self._traces):
                trace_file_name = (
                    f"{round_prefix}trace.zip" if len(self._traces) == 1 else f"{round_prefix}trace-{index + 1}.zip"
                )
                trace_path = _build_artifact_test_folder(
                    self._pytestconfig, self._request, trace_file_name
                )
                os.makedirs(os.path.dirname(trace_path), exist_ok=True)
                shutil.move(trace, trace_path)
                # allure附加zip文件的方法
                allure.attach.file(trace_path, "trace.playwright.dev", extension="zip")
        else:
            for trace in self._traces:
                os.remove(trace)

        preserve_video = self._video_option == "on" or (
                failed and self._video_option == "retain-on-failure"
        )
        if preserve_video:
            for index, page in enumerate(self._all_pages):
                video = page.video
                if not video:
                    continue
                try:
                    # 获取页面标题并处理非法字符
                    page_title = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', get_page_title(page))[:30] or "untitled"
                    video_file_name = (
                        f"{round_prefix}video.webm"
                        if len(self._all_pages) == 1
                        else f"{round_prefix}video-{index + 1}-{page_title}.webm"
                    )
                    # video_file_name = (
                    #     f"{round_prefix}video.webm"
                    #     if len(self._all_pages) == 1
                    #     else f"{round_prefix}video-{index + 1}.webm"
                    # )
                    video_path = _build_artifact_test_folder(
                        self._pytestconfig, self._request, video_file_name
                    )
                    if not _save_video_with_retry(video, video_path):
                        continue
                    # allure附加webm录像的方法
                    allure.attach.file(video_path, page_title, allure.attachment_type.WEBM)
                except Error:
                    logger.warning(f"failed to save page video, title={page_title}")
        else:
            for page in self._all_pages:
                # Can be changed to "if page.video" without try/except once https://github.com/microsoft/playwright-python/pull/2410 is released and widely adopted.
                if self._video_option in ["on", "retain-on-failure"]:
                    try:
                        page.video.delete()
                    except Error:
                        pass

    def on_did_create_browser_context(self, context: BrowserContext) -> None:
        #  上下文里监听,有新的page就添加到列表中
        base_url = GlobalMap().get("baseurl")
        context.on("page", lambda page: self._all_pages.append(page))
        global api_Count
        global _api_perf_records
        request_start_times: Dict[int, Dict[str, Any]] = {}
        # request id -> metadata，避免闭包中丢失开始时间
        request_start_times: Dict[int, Dict[str, Any]] = {}

        def on_page(page: Page):
            def on_clear(my_page: Page):
                try:
                    api_Count.clear()
                    my_page.wait_for_timeout(500)
                except:
                    pass

            # pages.append(page)
            page.on("close", on_clear)
            page.on("load", on_clear)

        def on_add_request(req):
            if any(fix in req.url for fix in [base_url]):
                api_Count.append(req.url)
            # 仅统计业务价值更高的接口请求
            if req.resource_type in ["xhr", "fetch"]:
                request_start_times[id(req)] = {
                    "start_perf": time.perf_counter(),
                    "method": req.method,
                    "url": req.url,
                    "resource_type": req.resource_type,
                    "test_nodeid": getattr(self._request.node, "nodeid", self._request.node.name),
                    "execution_count": getattr(self._request.node, "execution_count", 1),
                }

        def on_remove_request(req):
            try:
                api_Count.remove(req.url)
            except:
                pass
            start_meta = request_start_times.pop(id(req), None)
            if not start_meta:
                return

            duration_ms = round((time.perf_counter() - start_meta["start_perf"]) * 1000, 2)
            parsed_url = urlsplit(start_meta["url"])
            response = req.response()
            status_code = response.status if response else None
            endpoint = f'{start_meta["method"]} {parsed_url.path or "/"}'
            _api_perf_records.append({
                "method": start_meta["method"],
                "url": start_meta["url"],
                "endpoint": endpoint,
                "path": parsed_url.path or "/",
                "query": parsed_url.query,
                "status": status_code,
                "resource_type": start_meta["resource_type"],
                "duration_ms": duration_ms,
                "test_nodeid": start_meta["test_nodeid"],
                "execution_count": start_meta["execution_count"],
                "recorded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        context.on("page", on_page)
        context.on("request", on_add_request)
        context.on("requestfinished", on_remove_request)
        context.on("requestfailed", on_remove_request)
        #  判断是否需要trace,如果需要,就开始录制
        if self._request and self._capture_trace:
            context.tracing.start(
                title=slugify(self._request.node.name),
                screenshots=True,
                snapshots=True,
                sources=True,
            )

    def on_will_close_browser_context(self, context: BrowserContext) -> None:
        #  判断是否需要trace,如果需要,就结束录制
        if self._capture_trace:
            trace_path = Path(self._pw_artifacts_folder.name) / create_guid()
            try:
                context.tracing.stop(path=trace_path)
                self._traces.append(str(trace_path))
            except Error as e:
                logger.warning(f"stop tracing failed: {e}")

        #  如果需要截图,就在关闭page前,获取截图
        if self._screenshot_option in ["on", "only-on-failure"]:
            for page in context.pages:
                #  这里用try是因为有可能page已经关闭了
                try:
                    # 修改路径在这个错误中，OSError: [Errno 22] Invalid argument 指出了在尝试打开文件进行写入时出现了问题。具体来说，文件名
                    # page.title = re.sub(r'\s+\d{2}:\d{2}:\d{2}$', '', page.title())
                    # 完整清理（可配置）
                    page.title = clean_title(
                        get_page_title(page),
                        preserve_brackets=False  # 是否保留括号内容
                    )
                    # page.title = "".join([page.title(), str(time.time_ns())])
                    page.title = "".join([page.title, str(time.time_ns())])
                    screenshot_path = (
                        # Path(self._pw_artifacts_folder.name) / create_guid()
                        # Path(self._pw_artifacts_folder.name) / "".join([page.title(), str(time.time_ns())])
                            Path(self._pw_artifacts_folder.name) /  page.title
                    )
                    page.screenshot(
                        timeout=5000,
                        path=screenshot_path,
                        full_page=self._pytestconfig.getoption(
                            "--full-page-screenshot"
                        ),
                    )
                    self._screenshots.append(str(screenshot_path))
                except Error:
                    pass


def create_guid() -> str:
    return hashlib.sha256(os.urandom(16)).hexdigest()



 # --- 关键步骤：覆盖不安全的 delete_output_dir fixture ---
# 这个空的 fixture 会“遮蔽”原始的 delete_output_dir，
# 防止它在 worker 进程中执行不安全的删除操作。
@pytest.fixture(scope="session", autouse=True)
def delete_output_dir(pytestconfig: Any) -> None:
    """
    **OVERRIDE**: 覆盖原始的 delete_output_dir fixture。
    我们不在此处执行任何删除操作，将其交给主进程在安全时机处理。
    """
    logger.info(" 原始的 delete_output_dir fixture 已被覆盖，跳过。")
    yield  # 简单地提供一个空的 fixture
    # teardown 阶段也什么都不做


# --- 安全的主进程清理逻辑 ---
def pytest_configure(config: pytest.Config) -> None:
    """
    在测试配置阶段，由主进程执行。
    此时所有 worker 进程尚未启动，是清理和准备输出目录的最佳时机。
    """
    # 只在主进程执行
    current_pid = os.getpid()
    if hasattr(config, 'workerinput'):
        logger.info(f" Worker进程 {config.workerinput['workerid']}-进程号-{current_pid} 跳过清理。")
        return

    output_dir = config.getoption("--output")
    allure_report_dir = config.getoption("--alluredir")
    if not output_dir:
        logger.warning("--output 参数未指定，跳过输出目录清理。")
        return

    if not allure_report_dir:
        logger.warning("--output 参数未指定，跳过输出目录清理。")
        return

    # 检查是否由 run.py 启动
    is_run_by_script = os.environ.get("TEST_CLEANUP_DONE") == "1"

    # ---- 清理禅道bug相关的临时文件 ----
    try:
        bug_temp_dir = BugHelper.get_bug_temp_dir()
        if not is_run_by_script:
            # 直接运行时清理禅道bug相关的临时文件
            if os.path.exists(bug_temp_dir):
                shutil.rmtree(bug_temp_dir)
                logger.info(f" 清理bug临时目录: {bug_temp_dir}")
            os.makedirs(bug_temp_dir, exist_ok=True)
            logger.info(f" 创建bug临时目录: {bug_temp_dir}")
    except Exception as e:
        logger.warning(f" 清理bug临时文件时出错: {e}")


    # 检查环境变量
    # ---- 处理 output_dir (你的业务输出目录) ----
    if output_dir:
        if is_run_by_script:
            # 信任 run.py 已清理，只确保存在
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f" (信任 run.py) 输出目录已就绪: {output_dir}")
        else:
            # 直接运行，必须自己清理
            logger.info(f" 主进程-进程号-{current_pid} 进行清理。")
            logger.info(f" (直接调用) 清理业务输出目录: {output_dir}")
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f" (直接调用) 业务输出目录已清理: {output_dir}")

    # ---- 处理 allure_dir (Allure 报告目录) ----
    if allure_report_dir:
        # 关键：直接运行时才清理 allure_dir！
        if is_run_by_script:
            # 由 run.py 启动，相信它已清理，只确保存在
            os.makedirs(allure_report_dir, exist_ok=True)
            logger.info(f" (信任 run.py) Allure 目录已就绪: {allure_report_dir}")
        else:
            # 直接运行 pytest test_xxx.py，需要清理 allure_dir
            logger.info(f" 主进程-进程号-{current_pid} 进行清理。")
            logger.info(f" (直接调用) 清理 Allure 报告目录: {allure_report_dir}")
            if os.path.exists(allure_report_dir):
                shutil.rmtree(allure_report_dir)
            os.makedirs(allure_report_dir, exist_ok=True)
            logger.info(f" (直接调用) Allure 目录已清理: {allure_report_dir}")


class Locator(_Locator):
    # 这是一个私有属性，用于存储上一步操作的名称
    __last_step = None

    # @property装饰器：这个装饰器用于将一个方法转换为属性，这样可以通过简单的属性访问来调用方法。
    @property
    def selector(self):
        _repr = self.__repr__()
        if "selector" in _repr:
            __selector = []
            for _ in _repr.split("selector=")[1][1:-2].split(" >> "):
                if r"\\u" not in _:
                    __selector.append(_)
                    continue
                __selector.append(
                    _.encode("utf8")
                    .decode("unicode_escape")
                    .encode("utf8")
                    .decode("unicode_escape")
                )
            return " >> ".join(__selector)

    def __getattribute__(self, attr):
        # 这个方法在访问对象的属性时被调用。它拦截了对属性的访问，并在属性是可调用的（即方法）时，返回一个包装器函数。这个包装器函数会在执行原始方法之前等待页面加载完成，并处理一些额外的逻辑，比如记录操作步骤和处理超时情况。
        global api_Count
        global time_out
        try:
            orig_attr = super().__getattribute__(attr)
            if callable(orig_attr):

                def wrapped(*args, **kwargs):
                    # 这是一个内部函数，它是__getattribute__方法返回的包装器函数。它在执行原始方法之前等待页面加载完成，并处理一些额外的逻辑，比如记录操作步骤和处理超时情况。
                    step_title = None
                    if attr == "_sync" and self.__last_step:
                        step_title = self.__last_step
                    else:
                        self.__last_step = attr
                    start_time = time.time()
                    while True:
                        self.page.wait_for_load_state()
                        if time.time() - start_time < int(time_out / 1333):
                            try:
                                if attr in ["click", "fill", "hover", "check", "blur", "focus", "input_value"]:
                                    self.page.wait_for_timeout(100)
                                    api_length = len(api_Count)
                                    if api_Count:
                                        self.page.wait_for_timeout(200)
                                        self.page.evaluate('''() => {
                                               const spanToRemove = document.getElementById('ainotestgogogo');
                                               if (spanToRemove) {
                                                   spanToRemove.remove();
                                               }
                                           }''')
                                        self.page.evaluate(f'''() => {{
                                                const span = document.createElement('span');
                                                span.textContent = '{attr}:{api_length}';
                                                span.style.position = 'absolute';
                                                span.style.top = '0';
                                                span.style.left = '50%';
                                                span.style.transform = 'translateX(-50%)';
                                                span.style.backgroundColor = 'yellow'; // 设置背景色以便更容易看到
                                                span.style.zIndex = '9999';
                                                span.id = 'ainotestgogogo';
                                                document.body.appendChild(span);
                                            }}''')
                                    else:
                                        # 在这里可以添加自己需要等待或者处理的动作,比如等待转圈,关闭弹窗等等(当然,弹窗最好单独做个监听)
                                        self.page.locator("//*[contains(@class, 'spin-dot-spin')]").locator("visible=true").last.wait_for(state="hidden", timeout=30_000)
                                        self.page.locator('//div[@class="el-loading-mask"]/div[@class="el-loading-spinner"]/p[text()="保存数据中..."]').locator("visible=true").last.wait_for(state="hidden", timeout=30_000)
                                        self.page.locator("//*[contains(@class, 'el-icon-loading')]").locator("visible=true").last.wait_for(state="hidden", timeout=30_000)
                                        self.page.locator("//*[contains(@class, 'el-loading-mask')]").locator("visible=true").last.wait_for(state="hidden", timeout=60_000)
                                        self.page.locator('//p[contains(@class, "el-loading-text") and text()="Loading..."]').locator("visible=true").last.wait_for(state="hidden", timeout=60_000)
                                        self.page.locator('//div[@class="el-loading-mask"]/div[@class="el-loading-spinner"]').locator("visible=true").last.wait_for(state="hidden", timeout=60_000)
                                        if self.page.locator('//div[@class="antHcbm_routesDashboardCardsHcbmCards_down"][text()="关闭"]').locator("visible=true").or_(self.page.locator(".driver-close-btn").filter(has_text="关闭").locator("visible-true")).count():
                                            self.page.locator('//div[@class="antHcbm_routesDashboardCardsHcbmCards_down"][text()="关闭"]').locator("visible=true").or_(self.page.locator(".driver-close-btn").filter(has_text="关闭").locator("visible-true")).last.evaluate("node => node.click()")
                                        self.page.evaluate('''() => {
                                                const spanToRemove = document.getElementById('ainotestgogogo');
                                                if (spanToRemove) {
                                                    spanToRemove.remove();
                                                }
                                            }''')
                                        self.page.evaluate(f'''() => {{
                                                const span = document.createElement('span');
                                                span.textContent = '{attr}:{api_length}';
                                                span.style.position = 'absolute';
                                                span.style.top = '0';
                                                span.style.left = '50%';
                                                span.style.transform = 'translateX(-50%)';
                                                span.style.backgroundColor = 'green'; // 设置背景色以便更容易看到
                                                span.style.zIndex = '9999';
                                                span.id = 'ainotestgogogo';
                                                document.body.appendChild(span);
                                            }}''')
                                        break
                                else:
                                    break
                            except:
                                self.page.evaluate('''() => {
                                        const spanToRemove = document.getElementById('ainotestgogogo');
                                        if (spanToRemove) {
                                            spanToRemove.remove();
                                        }
                                    }''')
                                self.page.evaluate(f'''() => {{
                                        const span = document.createElement('span');
                                        span.textContent = '操作等待中.....';
                                        span.style.position = 'absolute';
                                        span.style.top = '0';
                                        span.style.left = '50%';
                                        span.style.transform = 'translateX(-50%)';
                                        span.style.backgroundColor = 'red'; // 设置背景色以便更容易看到
                                        span.style.zIndex = '9999';
                                        span.id = 'ainotestgogogo';
                                        document.body.appendChild(span);
                                    }}''')
                                break
                        else:
                            self.page.evaluate('''() => {
                                    const spanToRemove = document.getElementById('ainotestgogogo');
                                    if (spanToRemove) {
                                        spanToRemove.remove();
                                    }
                                }''')
                            escaped_api_count = json.dumps(api_Count)
                            self.page.evaluate(f'''() => {{
                                    const span = document.createElement('span');
                                    span.textContent = `当前列表内容为: {escaped_api_count}`;
                                    span.style.position = 'absolute';
                                    span.style.top = '0';
                                    span.style.left = '50%';
                                    span.style.transform = 'translateX(-50%)';
                                    span.style.backgroundColor = 'red'; // 设置背景色以便更容易看到
                                    span.style.zIndex = '9999';
                                    span.id = 'ainotestgogogo';
                                    document.body.appendChild(span);
                                }}''')
                            if sys.platform != "linux":
                                print("接口卡超时了,暂时放行,需要查看超时接口或调整接口监听范围:")
                                print(escaped_api_count)
                                pass
                            api_Count.clear()
                            break

                    # if step_title amd step_title != "wait_for" and step_title != "count":  #可以出去allure报告里 wait和count的sub-steps
                    if step_title and step_title != "wait_for" and step_title != "count":
                        with step(f"{step_title}: {self.selector}"):
                            return orig_attr(*args, **kwargs)
                    return orig_attr(*args, **kwargs)

                return wrapped
            return orig_attr
        except AttributeError:
            ...


mapping.register(LocatorImpl, Locator)
# 这行代码注册了一个映射，将LocatorImpl类与Locator类关联起来。这可能是用于依赖注入或其他类似的模式，以便在需要定位器的地方使用Locator类的实例。
# 总的来说，这段代码是一个自定义的定位器类，它提供了一种更灵活和强大的方式来处理页面元素的定位和操作。它通过拦截属性访问和包装方法调用来实现这些功能，并且它还处理了一些常见的问题，比如等待页面加载和处理超时情况。


# 统计用例执行情况的数据 - 按环境分离
_test_results_by_env = {
    'jenkins': {
        'total': 0,
        'passed': 0,
        'failed': 0,
        'skipped': 0,
        'failed_tests': [],
        'by_file': {},
        'by_module': {}
    },
    'local': {
        'total': 0,
        'passed': 0,
        'failed': 0,
        'skipped': 0,
        'failed_tests': [],
        'by_file': {},
        'by_module': {}
    }
}
# 当前运行环境
_current_env = None


def detect_run_environment():
    """
    检测运行环境
    返回：'jenkins' 或 'local'
    """
    jenkins_indicators = [
        os.getenv('BUILD_NUMBER'),
        os.getenv('JOB_NAME'),
        os.getenv('JENKINS_URL'),
        os.getenv('WORKSPACE'),
        os.getenv('NODE_NAME')
    ]

    if any(indicator for indicator in jenkins_indicators):
        return 'jenkins'
    else:
        return 'local'


def collect_run_info(environment):
    """
    收集运行环境信息
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if environment == 'jenkins':
        return {
            'environment': 'jenkins',
            'timestamp': timestamp,
            'build_number': os.getenv('BUILD_NUMBER', 'unknown'),
            'job_name': os.getenv('JOB_NAME', 'unknown'),
            'jenkins_url': os.getenv('JENKINS_URL', ''),
            'workspace': os.getenv('WORKSPACE', ''),
            'node_name': os.getenv('NODE_NAME', ''),
            'executor_number': os.getenv('EXECUTOR_NUMBER', ''),
            'git_commit': os.getenv('GIT_COMMIT', ''),
            'git_branch': os.getenv('GIT_BRANCH', '')
        }
    else:
        # 本地运行信息
        ide_name = detect_ide()
        return {
            'environment': 'local',
            'timestamp': timestamp,
            'user': os.getenv('USERNAME', os.getenv('USER', 'unknown')),
            'hostname': os.getenv('COMPUTERNAME', os.getenv('HOSTNAME', 'unknown')),
            'ide': ide_name,
            'python_version': sys.version,
            'working_directory': os.getcwd()
        }


def detect_ide():
    """
    检测当前使用的 IDE
    """
    # PyCharm
    if os.getenv('PYCHARM_HOSTED') == '1':
        return 'PyCharm'

    # VSCode
    if os.getenv('TERM_PROGRAM') == 'vscode':
        return 'VSCode'

    # IntelliJ IDEA
    if os.getenv('IDEA_INITIAL_DIRECTORY'):
        return 'IntelliJ IDEA'

    # 默认
    return 'Unknown/Command Line'


def save_statistics(statistics_data):
    """
    根据环境保存到不同位置，并合并历史数据
    """
    env = statistics_data['environment']
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')

    # 基础目录 - logs 目录
    base_dir = '../logs'

    # 按环境分目录
    if env == 'jenkins':
        env_dir = os.path.join(base_dir, 'jenkins_stats')
        build_num = statistics_data['run_info'].get('build_number', 'unknown')
        filename_prefix = f'summary_build_{build_num}_{timestamp}'
        history_file = os.path.join(env_dir, 'history_total.json')
    else:
        env_dir = os.path.join(base_dir, 'local_stats')
        filename_prefix = f'summary_{timestamp}'
        history_file = os.path.join(env_dir, 'history_total.json')

    # 确保目录存在
    os.makedirs(env_dir, exist_ok=True)

    # ---- 保存当次运行结果 ----
    json_file = os.path.join(env_dir, f'{filename_prefix}.json')
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(statistics_data, f, ensure_ascii=False, indent=2)

    logger.info(f"当次统计数据已保存到：{json_file}")

    # ---- 合并历史数据 ----
    historical_data = load_historical_data(history_file, env)

    # 更新历史总计
    updated_history = merge_historical_data(historical_data, statistics_data)

    # 保存更新后的历史数据
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(updated_history, f, ensure_ascii=False, indent=2)

    logger.info(f"历史统计数据已更新到：{history_file}")

    # ---- 更新最新结果文件 ----
    latest_file = os.path.join(env_dir, 'latest_summary.json')
    with open(latest_file, 'w', encoding='utf-8') as f:
        # 包含当次数据和历史总计
        combined_data = {
            'current_run': statistics_data,
            'historical_total': updated_history
        }
        json.dump(combined_data, f, ensure_ascii=False, indent=2)

    logger.info(f"最新统计已更新到：{latest_file}")


def load_historical_data(history_file, env):
    """
    加载历史数据
    """
    default_history = {
        'environment': env,
        'first_run_time': None,
        'last_run_time': None,
        'total_runs': 0,
        'runs_detail': [],  # 记录每次运行的简要信息
        'cumulative': {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'skipped': 0,
            'success_rate': 0.0
        },
        'by_file': {},
        'by_module': {}
    }

    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except Exception as e:
            logger.warning(f"加载历史数据失败：{e}，使用默认值")
            return default_history
    else:
        return default_history


def merge_historical_data(historical_data, current_data):
    """
    合并历史数据和当前运行数据
    """
    from datetime import datetime

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 更新基本信息
    if not historical_data['first_run_time']:
        historical_data['first_run_time'] = current_time
    historical_data['last_run_time'] = current_time
    historical_data['total_runs'] += 1

    # 记录本次运行的简要信息
    run_detail = {
        'timestamp': current_time,
        'total': current_data['results']['total'],
        'passed': current_data['results']['passed'],
        'failed': current_data['results']['failed'],
        'skipped': current_data['results']['skipped'],
        'success_rate': current_data['success_rate']
    }

    # 如果是 Jenkins，添加构建号
    if current_data['environment'] == 'jenkins':
        run_detail['build_number'] = current_data['run_info'].get('build_number', 'unknown')
    else:
        run_detail['ide'] = current_data['run_info'].get('ide', 'unknown')

    historical_data['runs_detail'].append(run_detail)

    # 只保留最近 100 次的详细信息
    if len(historical_data['runs_detail']) > 100:
        historical_data['runs_detail'] = historical_data['runs_detail'][-100:]

    # 累加总计数据
    historical_data['cumulative']['total'] += current_data['results']['total']
    historical_data['cumulative']['passed'] += current_data['results']['passed']
    historical_data['cumulative']['failed'] += current_data['results']['failed']
    historical_data['cumulative']['skipped'] += current_data['results']['skipped']

    # 重新计算历史成功率
    total = historical_data['cumulative']['total']
    passed = historical_data['cumulative']['passed']
    historical_data['cumulative']['success_rate'] = (passed / total * 100) if total > 0 else 0

    # 合并按文件统计的数据
    for file_name, stats in current_data['results'].get('by_file', {}).items():
        if file_name not in historical_data['by_file']:
            historical_data['by_file'][file_name] = {
                'total': 0,
                'passed': 0,
                'failed': 0,
                'skipped': 0,
                'success_rate': 0.0
            }

        hist_file = historical_data['by_file'][file_name]
        hist_file['total'] += stats['total']
        hist_file['passed'] += stats['passed']
        hist_file['failed'] += stats['failed']
        hist_file['skipped'] += stats['skipped']
        hist_file['success_rate'] = (hist_file['passed'] / hist_file['total'] * 100) if hist_file['total'] > 0 else 0

    # 合并按模块统计的数据
    for module_name, stats in current_data['results'].get('by_module', {}).items():
        if module_name not in historical_data['by_module']:
            historical_data['by_module'][module_name] = {
                'total': 0,
                'passed': 0,
                'failed': 0,
                'skipped': 0,
                'success_rate': 0.0
            }

        hist_module = historical_data['by_module'][module_name]
        hist_module['total'] += stats['total']
        hist_module['passed'] += stats['passed']
        hist_module['failed'] += stats['failed']
        hist_module['skipped'] += stats['skipped']
        hist_module['success_rate'] = (hist_module['passed'] / hist_module['total'] * 100) if hist_module[
                                                                                                  'total'] > 0 else 0

    return historical_data


def print_statistics_report(statistics_data):
    """
    打印统计报告到控制台（包含历史和当次）
    """
    env = statistics_data['environment']
    run_info = statistics_data['run_info']
    results = statistics_data['results']

    total = results['total']
    passed = results['passed']
    failed = results['failed']
    skipped = results['skipped']

    success_rate = (passed / total * 100) if total > 0 else 0

    print("\n" + "=" * 60)
    if env == 'jenkins':
        print(" " * 18 + "Jenkins 运行统计")
        print("=" * 60)
        print(f"构建号：#{run_info.get('build_number', 'N/A')}")
        print(f"任务名：{run_info.get('job_name', 'N/A')}")
        if run_info.get('git_commit'):
            print(f"Commit: {run_info.get('git_commit', '')[:7]}")
    else:
        print(" " * 20 + "本地运行统计")
        print("=" * 60)
        print(f"用户：{run_info.get('user', 'N/A')}@{run_info.get('hostname', 'N/A')}")
        print(f"IDE: {run_info.get('ide', 'N/A')}")

    print("-" * 60)
    print(f"时间：{run_info.get('timestamp', 'N/A')}")
    print("-" * 60)
    print("【当次运行情况】")
    print(f"总用例数：{total}")
    print(f"成功：{passed} | 失败：{failed} | 跳过：{skipped}")
    print(f"成功率：{success_rate:.2f}%")

    # 加载并显示历史数据
    if env == 'jenkins':
        history_file = os.path.join('../logs', 'jenkins_stats', 'history_total.json')
    else:
        history_file = os.path.join('../logs', 'local_stats', 'history_total.json')

    historical_data = load_historical_data(history_file, env)

    if historical_data['total_runs'] > 0:
        print("\n【历史运行统计】")
        print(f"运行次数：{historical_data['total_runs']}")
        print(f"首次运行：{historical_data['first_run_time']}")
        print(f"最后运行：{historical_data['last_run_time']}")
        print(f"累计总用例：{historical_data['cumulative']['total']}")
        print(f"累计成功：{historical_data['cumulative']['passed']}")
        print(f"累计失败：{historical_data['cumulative']['failed']}")
        print(f"历史成功率：{historical_data['cumulative']['success_rate']:.2f}%")

        # 显示最近 5 次的成功率趋势
        if historical_data['runs_detail']:
            print("\n【最近 5 次成功率趋势】")
            recent_runs = historical_data['runs_detail'][-5:]
            for i, run in enumerate(recent_runs):
                trend_icon = "↑" if run['success_rate'] >= 95 else ("↓" if run['success_rate'] < 90 else "→")
                run_identifier = run.get('build_number', run.get('ide', f'Run {i + 1}'))
                print(
                    f"  {trend_icon} 第 {len(historical_data['runs_detail']) - len(recent_runs) + i + 1} 次：{run['success_rate']:.2f}% ({run['passed']}/{run['total']}) - {run_identifier}")

    # 按文件统计 Top 5
    if results.get('by_file'):
        print("\n【当次按文件统计 Top 5】")
        sorted_files = sorted(
            results['by_file'].items(),
            key=lambda x: x[1]['passed'] / max(x[1]['total'], 1),
            reverse=True
        )[:5]

        for file, stats in sorted_files:
            file_success_rate = stats['passed'] / max(stats['total'], 1) * 100
            file_name = Path(file).name
            print(f"  {file_name}: {stats['passed']}/{stats['total']} ({file_success_rate:.1f}%)")

    # 显示失败用例
    if results.get('failed_tests'):
        print(f"\n【失败用例】({len(results['failed_tests'])})")
        for i, test in enumerate(results['failed_tests'][:5], 1):
            test_name = test.get('name', '').split('::')[-1][:50]
            print(f"  {i}. {test_name}")
        if len(results['failed_tests']) > 5:
            print(f"  ... 还有 {len(results['failed_tests']) - 5} 个失败用例")

    print("=" * 60 + "\n")


def _calc_percentile(values: List[float], percentile: float) -> float:
    """计算百分位数（线性插值），percentile 取值范围 0~1。"""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * percentile
    floor_index = math.floor(k)
    ceil_index = math.ceil(k)
    if floor_index == ceil_index:
        return float(sorted_values[int(k)])
    floor_value = sorted_values[floor_index]
    ceil_value = sorted_values[ceil_index]
    return float(floor_value + (ceil_value - floor_value) * (k - floor_index))


def _build_slow_api_report(
        records: List[Dict[str, Any]],
        environment: str,
        min_slow_ms: float = 1000.0,
        percentile_threshold: float = 0.95,
        min_samples_for_percentile: int = 20,
) -> Dict[str, Any]:
    """构建慢接口报告（固定阈值 + P95 动态阈值）。"""
    by_endpoint: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        endpoint_key = record.get("endpoint", "UNKNOWN")
        by_endpoint.setdefault(endpoint_key, []).append(record)

    endpoint_baseline: Dict[str, Dict[str, Any]] = {}
    for endpoint_key, endpoint_records in by_endpoint.items():
        durations = [float(item["duration_ms"]) for item in endpoint_records if item.get("duration_ms") is not None]
        p95 = _calc_percentile(durations, percentile_threshold) if durations else 0.0
        p50 = _calc_percentile(durations, 0.50) if durations else 0.0
        dynamic_threshold = p95 if len(durations) >= min_samples_for_percentile else 0.0
        final_threshold = max(min_slow_ms, dynamic_threshold)
        endpoint_baseline[endpoint_key] = {
            "samples": len(durations),
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "dynamic_threshold_ms": round(dynamic_threshold, 2),
            "final_threshold_ms": round(final_threshold, 2),
        }

    slow_records: List[Dict[str, Any]] = []
    for record in records:
        endpoint_key = record.get("endpoint", "UNKNOWN")
        duration_ms = float(record.get("duration_ms") or 0.0)
        threshold_ms = float(endpoint_baseline.get(endpoint_key, {}).get("final_threshold_ms", min_slow_ms))
        if duration_ms >= threshold_ms:
            item = dict(record)
            item["slow_threshold_ms"] = threshold_ms
            slow_records.append(item)

    slow_records.sort(key=lambda item: float(item.get("duration_ms", 0.0)), reverse=True)
    return {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "environment": environment,
        "rules": {
            "absolute_threshold_ms": min_slow_ms,
            "percentile_threshold": percentile_threshold,
            "min_samples_for_percentile": min_samples_for_percentile,
            "final_threshold_formula": "max(absolute_threshold_ms, endpoint_p95_if_enough_samples_else_0)",
            "resource_types": ["xhr", "fetch"],
        },
        "summary": {
            "total_api_calls": len(records),
            "slow_api_calls": len(slow_records),
            "slow_ratio_percent": round((len(slow_records) / len(records) * 100), 2) if records else 0.0,
        },
        "endpoint_baseline": endpoint_baseline,
        "slow_apis": slow_records,
    }


def _load_recent_slow_api_reports(base_dir: str, current_report_file: str, lookback_runs: int = 20) -> List[Dict[str, Any]]:
    """读取历史慢接口报告（不含当前 run），用于构建性能基线。"""
    if not os.path.isdir(base_dir):
        return []
    current_abs = os.path.abspath(current_report_file)
    files: List[str] = []
    for name in os.listdir(base_dir):
        if not (name.startswith("slow_apis_") and name.endswith(".json")):
            continue
        full_path = os.path.abspath(os.path.join(base_dir, name))
        if full_path == current_abs:
            continue
        files.append(full_path)
    files.sort(key=lambda fp: os.path.getmtime(fp), reverse=True)

    reports: List[Dict[str, Any]] = []
    for file_path in files[:lookback_runs]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                reports.append(data)
        except Exception as e:
            logger.warning(f"读取历史慢接口报告失败，已跳过: {file_path}, err={e}")
    return reports


def _build_baseline_regression_alerts(
        current_report: Dict[str, Any],
        history_reports: List[Dict[str, Any]],
        min_history_points: int = 3,
        min_current_samples: int = 5,
) -> Dict[str, Any]:
    """生成基线回归告警（历史中位 P95 对比当前 P95）。"""
    current_baseline = current_report.get("endpoint_baseline", {})
    history_by_endpoint: Dict[str, List[float]] = {}

    for history in history_reports:
        baseline = history.get("endpoint_baseline", {})
        if not isinstance(baseline, dict):
            continue
        for endpoint, metrics in baseline.items():
            if not isinstance(metrics, dict):
                continue
            p95 = metrics.get("p95_ms")
            samples = int(metrics.get("samples", 0))
            if p95 is None or samples <= 0:
                continue
            history_by_endpoint.setdefault(endpoint, []).append(float(p95))

    alerts: List[Dict[str, Any]] = []
    for endpoint, metrics in current_baseline.items():
        if not isinstance(metrics, dict):
            continue
        current_samples = int(metrics.get("samples", 0))
        current_p95 = float(metrics.get("p95_ms", 0.0))
        if current_samples < min_current_samples:
            continue

        history_p95 = history_by_endpoint.get(endpoint, [])
        if len(history_p95) < min_history_points:
            continue

        baseline_p95 = _calc_percentile(history_p95, 0.50)
        if baseline_p95 <= 0:
            continue

        ratio = current_p95 / baseline_p95
        delta_ms = current_p95 - baseline_p95
        severity = None
        if ratio >= 1.5 and delta_ms >= 500:
            severity = "critical"
        elif ratio >= 1.2 and delta_ms >= 200:
            severity = "warning"

        if severity:
            alerts.append({
                "severity": severity,
                "endpoint": endpoint,
                "current_p95_ms": round(current_p95, 2),
                "baseline_p95_ms": round(baseline_p95, 2),
                "delta_ms": round(delta_ms, 2),
                "ratio": round(ratio, 3),
                "current_samples": current_samples,
                "history_points": len(history_p95),
            })

    alerts.sort(key=lambda item: (0 if item["severity"] == "critical" else 1, -item["delta_ms"]))
    return {
        "rules": {
            "current_min_samples": min_current_samples,
            "history_min_points": min_history_points,
            "baseline_metric": "median_of_historical_p95",
            "warning": "ratio>=1.2 and delta_ms>=200",
            "critical": "ratio>=1.5 and delta_ms>=500",
        },
        "summary": {
            "history_reports_used": len(history_reports),
            "endpoints_compared": len(current_baseline),
            "alerts_total": len(alerts),
            "critical": sum(1 for item in alerts if item["severity"] == "critical"),
            "warning": sum(1 for item in alerts if item["severity"] == "warning"),
        },
        "alerts": alerts,
    }


def save_slow_api_report(environment: str, records: List[Dict[str, Any]]) -> Optional[str]:
    """保存慢接口报告，并同时输出基线对比告警 JSON。"""
    if not records:
        logger.info("未采集到 xhr/fetch 接口调用，跳过慢接口报告生成。")
        return None

    report_data = _build_slow_api_report(records, environment)
    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    base_dir = os.path.join("../logs", "api_perf", environment)
    os.makedirs(base_dir, exist_ok=True)

    report_file = os.path.join(base_dir, f"slow_apis_{timestamp}.json")
    latest_file = os.path.join(base_dir, "latest_slow_apis.json")

    # 基线回归：使用当前 run 之前的历史慢接口报告构建基线
    history_reports = _load_recent_slow_api_reports(base_dir, report_file, lookback_runs=20)
    regression_data = _build_baseline_regression_alerts(report_data, history_reports)
    report_data["baseline_regression"] = regression_data

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    baseline_file = os.path.join(base_dir, f"baseline_alerts_{timestamp}.json")
    latest_baseline_file = os.path.join(base_dir, "latest_baseline_alerts.json")
    with open(baseline_file, "w", encoding="utf-8") as f:
        json.dump(regression_data, f, ensure_ascii=False, indent=2)
    with open(latest_baseline_file, "w", encoding="utf-8") as f:
        json.dump(regression_data, f, ensure_ascii=False, indent=2)

    logger.info(f"慢接口报告已保存: {report_file}")
    logger.info(f"基线告警报告已保存: {baseline_file}, alerts={regression_data.get('summary', {}).get('alerts_total', 0)}")
    return report_file

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report):
    """增强的错误信息收集 - 按环境分离"""
    global _test_results_by_env

    # 获取当前运行环境
    env = detect_run_environment()
    current_results = _test_results_by_env[env]

    # 只在 call 阶段统计（真正的测试执行阶段）
    if report.when == 'call':
        current_results['total'] += 1

        # 按文件统计
        test_file = report.nodeid.split('::')[0]
        if test_file not in current_results['by_file']:
            current_results['by_file'][test_file] = {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0}
        current_results['by_file'][test_file]['total'] += 1

        # 按模块统计
        test_parts = test_file.replace('\\', '/').split('/')
        if len(test_parts) > 1:
            module_name = test_parts[-2]  # 获取 testcases 作为模块
            if module_name not in current_results['by_module']:
                current_results['by_module'][module_name] = {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0}
            current_results['by_module'][module_name]['total'] += 1

        if report.passed:
            current_results['passed'] += 1
            if test_file in current_results['by_file']:
                current_results['by_file'][test_file]['passed'] += 1
            if 'module_name' in locals() and module_name in current_results['by_module']:
                current_results['by_module'][module_name]['passed'] += 1

        elif report.failed:
            error_msg = str(report.longrepr) if hasattr(report, 'longrepr') else None
            # 提取更详细的错误信息
            if error_msg and 'AssertionError' in error_msg:
                error_msg = error_msg.split('AssertionError:')[-1].strip()
            failed_test_info = {
                "name": report.nodeid,
                "nodeid": report.nodeid,
                "error_message": error_msg
            }
            current_results['failed'] += 1
            current_results['failed_tests'].append(failed_test_info)

            if test_file in current_results['by_file']:
                current_results['by_file'][test_file]['failed'] += 1
            if 'module_name' in locals() and module_name in current_results['by_module']:
                current_results['by_module'][module_name]['failed'] += 1

        elif report.skipped:
            current_results['skipped'] += 1
            if test_file in current_results['by_file']:
                current_results['by_file'][test_file]['skipped'] += 1
            if 'module_name' in locals() and module_name in current_results['by_module']:
                current_results['by_module'][module_name]['skipped'] += 1

    # setup 和 teardown 阶段的失败也需要记录（如 fixture 失败）
    elif report.failed and report.when in ['setup', 'teardown']:
        # setup/teardown 失败通常意味着测试无法执行，应该记录为失败
        current_results['total'] += 1
        current_results['failed'] += 1

        error_msg = str(report.longrepr) if hasattr(report, 'longrepr') else None
        failed_test_info = {
            "name": report.nodeid,
            "nodeid": report.nodeid,
            "error_message": f"[{report.when}] {error_msg}" if error_msg else f"{report.when} failed"
        }
        current_results['failed_tests'].append(failed_test_info)


# 要自动打开报告就放开，不自动打开就注释
@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session):
    global _api_perf_records
    # 只在主进程执行(非 worker进程)
    if not hasattr(session.config, 'workerinput'):

        # 检测当前运行环境
        env = detect_run_environment()

        # 获取该环境的统计结果
        results = _test_results_by_env[env]

        # 计算成功率
        total = results['total']
        passed = results['passed']
        success_rate = (passed / total * 100) if total > 0 else 0

        # 收集运行信息
        run_info = collect_run_info(env)

        # 构建完整的统计对象
        statistics = {
            'environment': env,
            'run_info': run_info,
            'results': results,
            'success_rate': success_rate,
            'summary': {
                'total': total,
                'passed': passed,
                'failed': results['failed'],
                'skipped': results['skipped'],
                'success_rate': success_rate
            }
        }

        # 保存统计结果
        save_statistics(statistics)

        # 打印统计报告
        print_statistics_report(statistics)
        # 输出接口慢请求与基线回归告警报告
        save_slow_api_report(env, _api_perf_records)

        if ENABLE_ALLURE_REPORT:
            # 自动打开allure报告
            allure_report_auto_open_config = session.config.getoption("--allure_report_auto_open")
            if session.config.getoption("--allure_report_auto_open") != "off":
                if sys.platform != "linux":
                    import subprocess
                    import psutil
                    import time

                    def kill_existing_allure_processes():
                        '''关闭所有运行中的 allure serve 进程'''
                        killed = False
                        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                            try:
                                cmdline = proc.info['cmdline']
                                # 确保是 allure serve 进程
                                if cmdline and 'allure' in ' '.join(cmdline) and 'serve' in ' '.join(cmdline):
                                    print(f"\nFound existing Allure process (PID: {proc.info['pid']}), terminating...")
                                    psutil.Process(proc.info['pid']).terminate()
                                    killed = True
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                continue

                        if killed:
                            # 给进程一点时间来完全终止
                            time.sleep(3)
                        return killed

                    try:
                        # 先尝试关闭已经存在的 allure 进程
                        kill_existing_allure_processes()

                        # 启动新的 allure 服务
                        allure_report_dir = allure_report_auto_open_config
                        allure_command = f'allure serve {allure_report_dir}'
                        print(f"Starting new Allure server: {allure_command}")
                        subprocess.Popen(allure_command, shell=True)

                    except Exception as e:
                        print(f"Error managing Allure process: {str(e)}")

        if ENABLE_BUG_CREATION:
            bug_results = BugHelper.create_bugs_for_failed_tests(
                results['failed_tests'],
                defectType="BUG",
                projectId=259,
                moduleId=None,
                defectLevel="middle",
                caseStepId=0,
                srcHost=BUG_URL,
            )
            success_count = sum(1 for result in bug_results if result["success"])
            logger.info(
                f"缺陷创建完成，共处理 {len(bug_results)} 条失败记录，成功 {success_count} 条，失败 {len(bug_results) - success_count} 条"
            )
