import json
import logging
import threading
import ssl
import websocket

from .bot import bot
from .constant import LANGUAGE
from .info import QQInfo

logging.getLogger("websocket").setLevel(logging.WARNING)

class QQWebSocketConnector:
    def __init__(self, server, config):
        self.config = config
        self.server = server
        self.ws = None
        self.listener_thread = None
        self.language = config.get("language", "zh")
        
        if self.language not in LANGUAGE:
            server.logger.warning(LANGUAGE["en"]["language_not_found"].format(self.language))
            self.language = "en"
        
        # determine scheme using single parameter 'use_ssl' (boolean)
        use_ssl = config.get('use_ssl', False)
        self.scheme = "wss" if use_ssl else "ws"
        self.url = self._build_url(config)

        token = config.get("token")
        # WebSocketApp accepts headers as list of strings; convert dict to list for compatibility
        if token:
            self.headers = [f"Authorization: Bearer {token}"]
        else:
            self.headers = None

        self.bot = bot(self.send_message, max_wait_time=self._get_max_wait_time(config))

    def _get_max_wait_time(self, config):
        max_wait_time = config.get("max_wait_time", 5)
        if max_wait_time >= 10:
            self.server.logger.warning(LANGUAGE[self.language]["max_wait_time_warning"])
            return 9
        return max_wait_time

    def _build_url(self, config):
        host = config.get("host")
        port = config.get("port")
        post_path = config.get("post_path")
        scheme = getattr(self, "scheme", "ws") or "ws"
        url = f"{scheme}://{host}:{port}"
        if post_path:
            url += f"/{post_path}"

        return url

    def on_open(self, ws):
        self.server.logger.info(LANGUAGE[self.language]["connect_success"])

    def connect(self):
        self.server.logger.info(LANGUAGE[self.language]["try_connect"].format(self.url))
        self.ws = websocket.WebSocketApp(
            self.url,
            header=self.headers,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )

        # prepare run_forever kwargs (support ssl for wss)
        run_kwargs = {}
        # reconnect value is optional in config, default to 5 if supported by websocket-client
        run_kwargs['reconnect'] = self.config.get('reconnect', 5)

        if self.scheme and self.scheme.lower().startswith('wss'):
            # Configure SSL options
            verify = self.config.get('verify', True)
            sslopt = {}
            if not verify:
                sslopt['cert_reqs'] = ssl.CERT_NONE
            else:
                sslopt['cert_reqs'] = ssl.CERT_REQUIRED
                ca_certs = self.config.get('ca_certs')
                if ca_certs:
                    sslopt['ca_certs'] = ca_certs

            # allow passing additional ssl options from config
            extra_sslopt = self.config.get('sslopt') or {}
            sslopt.update(extra_sslopt)

            run_kwargs['sslopt'] = sslopt

        self.listener_thread = threading.Thread(target=self.ws.run_forever, kwargs=run_kwargs)
        self.listener_thread.start()
        self.server.logger.info(LANGUAGE[self.language]["start_connect"])

    def on_message(self, ws, message):
        self.server.logger.debug(LANGUAGE[self.language]["received_message"].format(message))
        message = json.loads(message)

        if "echo" in message:
            self.bot.function_return[message["echo"]] = message
        else:
            QQInfo(message, self.server, self.bot)

    def on_error(self, ws, error):
        self.server.logger.error(LANGUAGE[self.language]["error_connect"].format(error))

    def on_close(self, ws, close_status_code, close_msg):
        self.server.logger.debug(LANGUAGE[self.language]["close_connect"])
        self.close()

    def send_message(self, message):
        if self.is_connected():
            self.ws.send(json.dumps(message))
            self.server.logger.debug(LANGUAGE[self.language]["send_message"].format(message))
        else:
            self.server.logger.warning(LANGUAGE[self.language]["retry_connect"])

    def close(self):
        try:
            if self.ws:
                self.ws.close()
            if self.listener_thread:
                self.listener_thread.join()
            self.server.logger.info(LANGUAGE[self.language]["close_info"])
        except Exception as e:
            self.server.logger.warning(LANGUAGE[self.language]["error_close"].format(e))

    def is_connected(self):
        return self.ws and self.ws.sock and self.ws.sock.connected
