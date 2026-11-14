"""WAS -Plugin

this plugin reads emergency messages from the german emergency warning system and
sends new messages to the meshcore plugin for forwarding to a mesh network

"""

# Standard module
import time
import threading
import json
import requests
import logging

# Non standard modules (install with pip)


# own local modules
import msg_ids

from was import defaults
from was.splthread import SplThread
from was.common.jsonstorage import JsonStorage

logger = logging.getLogger("nmg.dwd_listen")


class SplPlugin(SplThread):
    """Instance of the base SplThread class"""

    plugin_id = "dwd_listen"  # must be unique, used for message routing
    plugin_names = [
        "DWD Message Listener"
    ]  # list of names, used for display in the web interface

    def __init__(self, modref):
        """inits the plugin"""
        self.modref = modref

        # do the plugin specific initialisation first

        self.config = JsonStorage(
            self.plugin_id,
            "backup",
            "dwd_listen.json",
            {
                "url": "https://www.dwd.de/DWD/warnungen/warnapp/json/warnings.json",
                "states": {"niedersachsen": ["wesermarsch"]},
                "poll_interval": 60,
                "info_fields": [
                    "state",
                    "regionName",
                    "headline",
                    "description",
                    "instruction",
                ],
            },
        )  # set defaults

        self.lock = threading.Lock()  # create a lock, only if necessary

        # at last announce the own plugin
        super().__init__(modref.message_handler, self)
        modref.message_handler.add_event_handler(self.plugin_id, 0, self.event_listener)
        modref.message_handler.add_query_handler(self.plugin_id, 0, self.query_handler)
        self.run_flag = True
        # plugin specific attributes
        self.messages = {}
        self.SEV_MAP = {
            1: "🟢",
            2: "🟡",
            3: "🟠",
            4: "🔴",
            5: "🟡",
        }

    def event_listener(self, queue_event):
        """try to send simulated answers"""
        # print("uihandler event handler", queue_event.type, queue_event.user)
        if queue_event.type == defaults.MSG_SYSTEM_READY:
            print(f"plugin {self.plugin_id} received system ready event")
            # self.modref.message_handler.queue_event(
            #     None,
            #     msg_ids.MSG_NMG_WARNING_TEXT,
            #     {
            #         "source": "dwd",
            #         "text": "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.",
            #     },
            # )
        if queue_event.type == "_join":
            print("a web client has connected", queue_event.data)
        # for further pocessing, do not forget to return the queue event
        return queue_event

    def query_handler(self, queue_event, max_result_count):
        # print("myplugin handler query handler",
        # queue_event.type,  queue_event.user, max_result_count)
        if queue_event.type == defaults.MSG_SOCKET_XXX:  # wait for defined messages
            pass
        return []

    def plugin_run(self):
        """starts the server"""
        # https://stackoverflow.com/a/36499538/1927807
        t = threading.current_thread()
        while getattr(t, "run_flag", True):
            new_messages = self.fetch_dwd()
            new_messages = self.filter_messages_by_state(new_messages)
            if new_messages:
                with self.lock:
                    diff = self.new_dict_elements(self.messages, new_messages)
                    if diff:
                        msg_fields = self.config.read("info_fields", [])
                        for msg in diff:
                            sev_type = msg.get("level", 0)
                            msg_text = self.SEV_MAP.get(sev_type, "⚪") + " | ".join(
                                [msg.get(f, "") for f in msg_fields]
                            )
                            logger.info("New DWD message: %s", msg_text)
                            self.messages = new_messages
                            self.modref.message_handler.queue_event(
                                None,
                                msg_ids.MSG_NMG_WARNING_TEXT,
                                {
                                    "source": "dwd",
                                    "text": msg_text,
                                },
                            )
            time.sleep(self.config.read("poll_interval", 60))


    def plugin_stop(self):
        self.run_flag = False

    # ------ plugin specific routines

    def new_dict_elements(self, old_dict, new_dict):
        """
        Returns a dictionary containing the elements that are present in new_dict but not in old_dict.
        """
        result = {}
        if type(old_dict) is not type(new_dict):
            return new_dict
        if isinstance(new_dict, dict):
            for k, v in new_dict.items():
                if k not in old_dict:
                    result[k] = v
                elif isinstance(v, dict) and isinstance(old_dict[k], dict):
                    nested_result = self.new_dict_elements(old_dict[k], v)
                    if nested_result:
                        result[k] = nested_result
        elif isinstance(new_dict, list):
            for i, v in enumerate(new_dict):
                if i >= len(old_dict):
                    result[i] = v
                elif isinstance(v, dict) and isinstance(old_dict[i], dict):
                    nested_result = self.new_dict_elements(old_dict[i], v)
                    if nested_result:
                        result[i] = nested_result
        return result

    def filter_messages_by_state(self, messages):
        """filters the messages by state and region"""
        states = self.config.read("states", {})
        filtered_messages = []
        for message in messages:
            state_name = message.get("state", "").lower()
            region_name = message.get("regionName", "").lower()
            if state_name in states:
                if any(region.lower() in region_name for region in states[state_name]):
                    filtered_messages.append(message)

        return filtered_messages

    def fetch_dwd(self):
        """fetches the current warnings from the DWD website"""
        try:
            r = requests.get(
                self.config.read("url", "no_url"),
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
        except requests.RequestException as e:
            logger.error("Error fetching DWD warnings: %s", e)
            return []
        text = r.text
        if text.startswith("warnWetter.loadWarnings("):
            text = text[len("warnWetter.loadWarnings(") : -2]
        d = json.loads(text)
        msgs = []
        for _, wlist in d.get("warnings", {}).items():
            msgs.extend(wlist)
        for _, wlist in d.get("vorabInformation", {}).items():
            msgs.extend(wlist)
        return msgs
