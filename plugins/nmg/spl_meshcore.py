"""WAS -Plugin

this plugin waits for messages from the emergency warning system and forwards them to a mesh network

"""

# Standard module
import time
import threading

# Non standard modules (install with pip)


# own local modules
import msg_ids

from was import defaults
from was.splthread import SplThread
from was.common.jsonstorage import JsonStorage


class SplPlugin(SplThread):
    """Instance of the base SplThread class"""

    plugin_id = "meshcore"  # must be unique, used for message routing
    plugin_names = [
        "MeshCore Interface"
    ]  # list of names, used for display in the web interface

    def __init__(self, modref):
        """inits the plugin"""
        self.modref = modref

        # do the plugin specific initialisation first

        self.movielist_storage = JsonStorage(
            self.plugin_id,
            "backup",
            "meshcore.json",
            {
                "channels": {
                    "nina": "https://warnung.bund.de/bbk.mowas/gefahrendurchsagen/atom.xml"
                },
                "users": {
                    "testuser": "6d1d1956a7f193aaa789ad5847b2565add244636af38e2ed49c72cd57343ed99"
                },
            },
        )  # set defaults

        self.lock = threading.Lock()  # create a lock, only if necessary

        # at last announce the own plugin
        super().__init__(modref.message_handler, self)
        modref.message_handler.add_event_handler(self.plugin_id, 0, self.event_listener)
        modref.message_handler.add_query_handler(self.plugin_id, 0, self.query_handler)
        self.run_flag = True

    def event_listener(self, queue_event):
        """try to send simulated answers"""
        # print("uihandler event handler", queue_event.type, queue_event.user)
        if queue_event.type == defaults.MSG_SYSTEM_READY:
            print(f"plugin {self.plugin_id} received system ready event")
        if queue_event.type == msg_ids.MSG_NMG_WARNING_TEXT:
            print(f"plugin {self.plugin_id} received NMG warning text event")
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
            time.sleep(10)
            with self.lock:
                pass

    def plugin_stop(self):
        self.run_flag = False

    # ------ plugin specific routines
