"""
船舶远程控制 - 船上设备客户端
运行在船上NanoPi Zero2 (ARM64 Linux)上

功能：
- 开机自启，主动连接中继服务器
- 自动重连（4G网络不稳定）
- 接收SSH/Web端口转发请求
- 接收远程命令执行
- 心跳保活
"""

import asyncio
import base64
import json
import logging
import os
import subprocess
import sys
import time
import signal
import urllib.request
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("ship_client")

# 配置文件路径
CONFIG_PATH = Path("/opt/zero2_device/remote_client.json")
# 默认配置
DEFAULT_CONFIG = {
    "device_id": "",           # 设备ID，留空则自动从config.json读取
    "relay_host": "",          # 中继地址，如: 6.tcp.cpolar.cn（单个地址，兼容旧配置）
    "relay_port": 10799,       # 中继端口（relay_host对应的端口）
    "relay_addresses": [],     # 优先连接地址列表 [{"host":"14.23.87.117","port":7000}, ...]，按顺序尝试
    "config_url": "https://cdn.jsdelivr.net/gh/wmlsyd/ship-relay-config@main/relay_address.json",  # 地址发现URL，自动获取最新中继地址（作为最终回退）
    "heartbeat_interval": 30,  # 心跳间隔(秒)
    "reconnect_delay": 10,     # 重连延迟(秒)
    "max_reconnect_delay": 300  # [M16 fix] increased from 60 to 300, avoid rapid retry on persistent network outage # 最大重连延迟(秒)
}


def load_config() -> dict:
    """加载配置"""
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                user_cfg = json.load(f)
                config.update(user_cfg)
        except Exception as e:
            logger.error(f"配置加载失败: {e}")

    # 如果device_id为空，尝试从主配置读取
    if not config.get("device_id"):
        for path in ["/opt/zero2_device/config.json", "/home/pi/zero2_device/config.json"]:
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        main_cfg = json.load(f)
                    config["device_id"] = main_cfg.get("device_id", "")
                    if config["device_id"]:
                        break
                except Exception:
                    pass

    return config


class ShipClient:
    """船上设备客户端"""

    def __init__(self, config: dict):
        self.config = config
        self.device_id = config.get("device_id", "unknown")
        self.config_url = config.get("config_url", "")
        self.heartbeat_interval = config.get("heartbeat_interval", 30)
        self.reconnect_delay = config.get("reconnect_delay", 10)
        self.max_reconnect_delay = config.get("max_reconnect_delay", 60)
        # 本地服务端口（注册时上报，中继服务器据此转发）
        self.local_ssh_port = config.get("local_ssh_port", 22)
        self.local_web_port = config.get("local_web_port", 20801)

        # 构建连接地址列表（优先级从高到低）
        self._relay_addresses = []
        # 1. relay_addresses 列表（最高优先级）
        for addr in config.get("relay_addresses", []):
            host = addr.get("host", "")
            port = addr.get("port", 0)
            if host and port:
                self._relay_addresses.append((host, int(port)))
        # 2. relay_host 单地址（兼容旧配置）
        relay_host = config.get("relay_host", "")
        relay_port = config.get("relay_port", 10799)
        if relay_host and relay_port:
            if (relay_host, relay_port) not in self._relay_addresses:
                self._relay_addresses.append((relay_host, int(relay_port)))
        # 3. CDN地址（回退，首次启动时获取，重连时也会刷新）
        self._cdn_address = None  # (host, port) 从config_url获取

        # 当前使用的地址
        self.relay_host = ""
        self.relay_port = 0

        self._reader: asyncio.StreamReader = None
        self._writer: asyncio.StreamWriter = None
        self._running = False
        self._connected: bool = False  # whether currently connected
        self._channels: dict = {}  # channel_id -> (reader, writer) pair
        self._config_mtime: float = 0  # 配置文件修改时间，用于热加载
        self.probe_interval = config.get("probe_interval", 60)
        self.probe_timeout = config.get("probe_timeout", 5)

    def _reload_config_if_changed(self) -> bool:
        """检查配置文件是否变化，如有变化则热加载地址列表。返回True表示配置已更新"""
        if not CONFIG_PATH.exists():
            return False

        try:
            mtime = CONFIG_PATH.stat().st_mtime
            if mtime == self._config_mtime:
                return False

            logger.info(f"检测到配置文件变化，重新加载...")
            new_config = load_config()

            # 重建地址列表
            old_addresses = list(self._relay_addresses)
            self._relay_addresses = []
            for addr in new_config.get("relay_addresses", []):
                host = addr.get("host", "")
                port = addr.get("port", 0)
                if host and port:
                    self._relay_addresses.append((host, int(port)))

            relay_host = new_config.get("relay_host", "")
            relay_port = new_config.get("relay_port", 0)
            if relay_host and relay_port:
                if (relay_host, int(relay_port)) not in self._relay_addresses:
                    self._relay_addresses.append((relay_host, int(relay_port)))

            # 更新其他配置
            if new_config.get("config_url"):
                self.config_url = new_config["config_url"]

            self._config_mtime = mtime

            if old_addresses != self._relay_addresses:
                logger.info(f"地址列表已更新: {old_addresses} → {self._relay_addresses}")
            else:
                logger.info("配置已重载，地址列表未变")

            return old_addresses != self._relay_addresses
        except Exception as e:
            logger.warning(f"配置热加载失败: {e}")
            return False

    async def start(self):
        """启动客户端"""
        # 记录初始配置文件修改时间
        if CONFIG_PATH.exists():
            self._config_mtime = CONFIG_PATH.stat().st_mtime

        # 首次启动时尝试从config_url获取回退地址
        if self.config_url:
            self._fetch_relay_address()

        if not self._relay_addresses and not self._cdn_address:
            logger.error("未配置任何中继地址，无法连接中继服务器")
            return

        self._running = True
        addr_list = ", ".join(f"{h}:{p}" for h, p in self._relay_addresses)
        if self._cdn_address:
            addr_list += f", CDN回退:{self._cdn_address[0]}:{self._cdn_address[1]}"
        logger.info(f"船上客户端启动 | 设备: {self.device_id} | 中继地址: {addr_list}")

        delay = self.reconnect_delay
        while self._running:
            try:
                await self._try_connect()
                delay = self.reconnect_delay  # 连接成功后重置延迟
            except Exception as e:
                logger.error(f"连接异常: {e}")

            if self._running:
                # 重连前：热加载配置 + 刷新CDN地址
                config_changed = self._reload_config_if_changed()
                if config_changed:
                    delay = self.reconnect_delay  # 配置变化时重置退避延迟
                self._fetch_relay_address()
                # [M16 fix] 退避上限从60秒提到300秒，避免4G断网时频繁重试占CPU
                actual_delay = min(delay, self.max_reconnect_delay)
                logger.info(f"将在 {actual_delay} 秒后重连...")
                await asyncio.sleep(actual_delay)
                delay = min(delay * 2, self.max_reconnect_delay)  # 指数退避

    async def _try_connect(self):
        """按优先级尝试所有中继地址，第一个成功即用"""
        # 构建本次尝试的地址列表
        addresses = list(self._relay_addresses)
        if self._cdn_address and self._cdn_address not in addresses:
            addresses.append(self._cdn_address)

        for host, port in addresses:
            try:
                logger.info(f"尝试连接: {host}:{port}")
                self.relay_host = host
                self.relay_port = port
                await self._connect_and_run()
                return  # 连接成功并正常断开，不再尝试
            except Exception as e:
                logger.warning(f"连接 {host}:{port} 失败: {e}")
                continue

        # 所有地址都失败
        raise ConnectionError("所有中继地址均连接失败")

    def _fetch_relay_address(self):
        """从config_url获取最新的中继服务器地址（作为回退地址，cpolar隧道地址可能已变）"""
        if not self.config_url:
            return

        try:
            req = urllib.request.Request(self.config_url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            new_host = data.get('relay_host', '')
            new_port = data.get('relay_port', 0)

            if new_host and new_port:
                if self._cdn_address != (new_host, new_port):
                    logger.info(f"CDN回退地址更新: {new_host}:{new_port}")
                    self._cdn_address = (new_host, int(new_port))
                else:
                    logger.debug(f"CDN回退地址未变: {new_host}:{new_port}")
        except Exception as e:
            logger.warning(f"获取地址发现失败: {e}")

    async def _connect_and_run(self):
        """连接中继服务器并保持"""
        logger.info(f"连接中继: {self.relay_host}:{self.relay_port}")

        self._reader, self._writer = await asyncio.open_connection(
            self.relay_host, self.relay_port
        )
        logger.info("TCP连接建立")

        # 发送注册消息（包含本地端口信息）
        reg_msg = {
            'type': 'register',
            'device_id': self.device_id,
            'info': {
                'hostname': os.uname().nodename if hasattr(os, 'uname') else os.getenv('COMPUTERNAME', ''),
                'platform': sys.platform,
                'pid': os.getpid(),
                'local_ssh_port': self.local_ssh_port,
                'local_web_port': self.local_web_port
            }
        }
        self._writer.write((json.dumps(reg_msg) + '\n').encode())
        await self._writer.drain()
        logger.info(f"注册消息已发送: {self.device_id}")

        # 等待注册确认
        line = await asyncio.wait_for(self._reader.readline(), timeout=15)
        resp = json.loads(line.decode().strip())
        if resp.get('type') == 'register_ack' and resp.get('status') == 'ok':
            ssh_port = resp.get('ssh_port', 0)
            web_port = resp.get('web_port', 0)
            logger.info(f"注册成功! SSH: localhost:{ssh_port} | Web: localhost:{web_port}")
        else:
            logger.error(f"注册失败: {resp}")
            return

        # 并行运行：心跳 + 消息接收 + 探测回切
        # [M16 fix] 使用显式任务管理：任意loop退出时取消其余任务，避免gather卡死
        self._connected = True
        tasks = []
        try:
            tasks = [
                asyncio.ensure_future(self._heartbeat_loop()),
                asyncio.ensure_future(self._receive_loop()),
                asyncio.ensure_future(self._probe_loop()),
            ]
            # 等待第一个任务完成（正常退出或异常）
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            # 取消所有未完成的任务
            for t in pending:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            # 检查是否有异常
            for t in done:
                if t.exception():
                    logger.warning(f"连接任务异常退出: {t.exception()}")
        finally:
            self._connected = False
            # 确保连接断开时清理资源
            if self._writer:
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except Exception:
                    pass
            logger.info("连接已关闭，将自动重连")

    async def _probe_loop(self):
        """定时探测优先地址是否可达，如果当前连在非优先地址上则切换回去"""
        if not self._relay_addresses:
            return

        await asyncio.sleep(self.probe_interval)  # 首次探测前等待

        while self._running and self._connected:
            try:
                # 最高优先级地址
                preferred = self._relay_addresses[0]
                current = (self.relay_host, self.relay_port)

                # 已连在优先地址上，无需探测
                if current == preferred:
                    await asyncio.sleep(self.probe_interval)
                    continue

                # 探测优先地址
                reachable = await self._probe_relay(preferred[0], preferred[1])

                if reachable:
                    logger.info(
                        f"优先地址 {preferred[0]}:{preferred[1]} 已可达，"
                        f"从 {current[0]}:{current[1]} 切换"
                    )
                    # 关闭当前连接，触发重连到优先地址
                    if self._writer:
                        try:
                            self._writer.close()
                        except Exception:
                            pass
                    return  # 退出探测循环，触发重连
                else:
                    logger.debug(
                        f"优先地址 {preferred[0]}:{preferred[1]} 不可达，"
                        f"保持当前连接 {current[0]}:{current[1]}"
                    )

            except Exception as e:
                logger.debug(f"探测异常: {e}")

            await asyncio.sleep(self.probe_interval)

    async def _probe_relay(self, host: str, port: int) -> bool:
        """TCP探测指定中继地址是否可达"""
        try:
            # [M16 fix] 用线程池执行DNS解析，避免阻塞事件循环
            loop = asyncio.get_event_loop()
            conn_coro = asyncio.open_connection(host, port)
            _, writer = await asyncio.wait_for(conn_coro, timeout=self.probe_timeout)
            writer.close()
            return True
        except Exception:
            return False

    async def _heartbeat_loop(self):
        """心跳保活"""
        while self._running and self._writer:
            try:
                msg = {'type': 'heartbeat', 'device_id': self.device_id}
                self._writer.write((json.dumps(msg) + '\n').encode())
                await self._writer.drain()
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"心跳发送失败: {e}")
                # [M16 fix] 关闭writer让receive_loop也退出
                if self._writer:
                    try:
                        self._writer.close()
                    except Exception:
                        pass
                break

    async def _receive_loop(self):
        """接收中继服务器消息"""
        while self._running:
            try:
                line = await asyncio.wait_for(self._reader.readline(), timeout=60)
                if not line:
                    logger.warning("连接断开")
                    # 关闭 writer 让心跳 loop 也退出
                    if self._writer:
                        try:
                            self._writer.close()
                        except Exception:
                            pass
                    break

                msg = json.loads(line.decode().strip())
                msg_type = msg.get('type')

                if msg_type == 'heartbeat_ack':
                    pass  # 心跳回复，忽略

                elif msg_type == 'open_channel':
                    # 中继请求打开一个端口转发通道
                    await self._handle_open_channel(msg)

                elif msg_type == 'channel_data':
                    # 转发数据到本地端口
                    await self._handle_channel_data(msg)

                elif msg_type == 'channel_close':
                    # 关闭通道
                    await self._handle_channel_close(msg)

                elif msg_type == 'command':
                    # 远程命令
                    await self._handle_command(msg)

                else:
                    logger.warning(f"未知消息类型: {msg_type}")

            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error(f"消息接收异常: {e}")
                break

    async def _handle_open_channel(self, msg: dict):
        """处理打开通道请求 - 连接本地端口"""
        channel_id = msg.get('channel_id')
        remote_port = msg.get('remote_port', 22)

        try:
            # 连接本地端口
            reader, writer = await asyncio.open_connection('127.0.0.1', remote_port)
            self._channels[channel_id] = (reader, writer)
            logger.info(f"通道打开: {channel_id} → localhost:{remote_port}")

            # 启动数据读取循环（从本地端口读，发回中继）
            asyncio.create_task(self._channel_read_loop(channel_id, reader))

        except Exception as e:
            logger.error(f"通道打开失败 {channel_id} → localhost:{remote_port}: {e}")
            # 通知中继通道失败
            try:
                resp = {'type': 'channel_close', 'channel_id': channel_id}
                self._writer.write((json.dumps(resp) + '\n').encode())
                await self._writer.drain()
            except Exception:
                pass

    async def _channel_read_loop(self, channel_id: str, reader: asyncio.StreamReader):
        """从本地端口读取数据，发回中继"""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                msg = {
                    'type': 'channel_data',
                    'channel_id': channel_id,
                    'payload': base64.b64encode(data).decode()
                }
                self._writer.write((json.dumps(msg) + '\n').encode())
                await self._writer.drain()
        except Exception:
            pass
        finally:
            # 通知中继通道关闭
            try:
                msg = {'type': 'channel_close', 'channel_id': channel_id}
                self._writer.write((json.dumps(msg) + '\n').encode())
                await self._writer.drain()
            except Exception:
                pass
            self._channels.pop(channel_id, None)

    async def _handle_channel_data(self, msg: dict):
        """处理转发数据 - 写入本地端口"""
        channel_id = msg.get('channel_id')
        payload = msg.get('payload', '')

        if channel_id not in self._channels:
            return

        _, writer = self._channels[channel_id]
        try:
            data = base64.b64decode(payload)
            writer.write(data)
            await writer.drain()
        except Exception as e:
            logger.debug(f"通道数据写入失败 {channel_id}: {e}")

    async def _handle_channel_close(self, msg: dict):
        """处理通道关闭"""
        channel_id = msg.get('channel_id')
        if channel_id in self._channels:
            _, writer = self._channels.pop(channel_id)
            try:
                writer.close()
            except Exception:
                pass

    async def _handle_command(self, msg: dict):
        """处理远程命令执行"""
        cmd = msg.get('cmd', '')
        request_id = msg.get('request_id', '')

        logger.info(f"收到远程命令: {cmd}")

        try:
            # 执行命令
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

            result = {
                'type': 'command_result',
                'request_id': request_id,
                'exit_code': proc.returncode,
                'output': stdout.decode(errors='replace')[:4096],
                'error': stderr.decode(errors='replace')[:4096]
            }
        except asyncio.TimeoutError:
            result = {
                'type': 'command_result',
                'request_id': request_id,
                'exit_code': -1,
                'error': '命令执行超时(30秒)'
            }
        except Exception as e:
            result = {
                'type': 'command_result',
                'request_id': request_id,
                'exit_code': -1,
                'error': str(e)
            }

        try:
            self._writer.write((json.dumps(result) + '\n').encode())
            await self._writer.drain()
        except Exception as e:
            logger.error(f"命令结果发送失败: {e}")

    async def stop(self):
        """停止客户端"""
        self._running = False
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
        logger.info("客户端已停止")


def main():
    config = load_config()

    # 检查是否配置了任何连接地址
    has_relay = config.get("relay_host") or config.get("relay_addresses")
    has_cdn = config.get("config_url")
    if not has_relay and not has_cdn:
        print("错误: 未配置relay_host/relay_addresses且无config_url，请编辑 /opt/zero2_device/remote_client.json")
        print(f"示例: {json.dumps(DEFAULT_CONFIG, indent=2, ensure_ascii=False)}")
        sys.exit(1)

    client = ShipClient(config)

    def _signal_handler(sig, frame):
        logger.info(f"收到信号 {sig}，正在停止...")
        client._running = False

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # [M16 fix] 添加看门狗：如果start()卡住超过10分钟，强制退出让systemd重启
    import threading
    _watchdog_stop = threading.Event()

    def _watchdog():
        # 每60秒检查一次，如果连接状态长时间没变化则强制退出
        last_connected = False
        stale_count = 0
        while not _watchdog_stop.wait(60):
            currently_connected = client._connected
            if currently_connected:
                stale_count = 0
                last_connected = True
            else:
                if last_connected:
                    # 刚断开，重置
                    stale_count = 0
                    last_connected = False
                else:
                    # 持续未连接
                    stale_count += 1
                    if stale_count >= 10:  # 10分钟一直未连接
                        logger.error("[看门狗] 连续10分钟未连上中继，强制退出让systemd重启")
                        os._exit(1)
    t = threading.Thread(target=_watchdog, daemon=True)
    t.start()

    try:
        asyncio.run(client.start())
    finally:
        _watchdog_stop.set()


if __name__ == '__main__':
    main()
