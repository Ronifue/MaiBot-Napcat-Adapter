import asyncio
from maim_message import Router, RouteConfig, TargetConfig
from .config import global_config
from .logger import logger, custom_logger
from .send_handler.main_send_handler import send_handler

route_config = RouteConfig(
    route_config={
        global_config.maibot_server.platform_name: TargetConfig(
            url=f"ws://{global_config.maibot_server.host}:{global_config.maibot_server.port}/ws",
            token=None,
        )
    }
)
router = Router(route_config, custom_logger)
_mmc_stop_lock = asyncio.Lock()
_mmc_stopped = False


async def mmc_start_com():
    logger.info("正在连接MaiBot")
    router.register_class_handler(send_handler.handle_message)
    await router.run()


async def mmc_stop_com():
    global _mmc_stopped
    async with _mmc_stop_lock:
        if not _mmc_stopped:
            _mmc_stopped = True
            logger.info("正在停止 MMC com layer...")
            await router.stop()
