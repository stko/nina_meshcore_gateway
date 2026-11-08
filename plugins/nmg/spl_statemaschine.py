"""State machine plugin for the NINA Meshcore Gateway.

this plugin is just a preparation for the state machine, it does not implement any logic yet

"""

# Standard module
import time
import threading

# Non standard modules (install with pip)
from statemachine import StateMachine, State

# own local modules
import msg_ids

from was import defaults
from was.splthread import SplThread
from was.common.jsonstorage import JsonStorage


class Statemachine(StateMachine):
    # States
    Initial = State(initial=True)
    dummy = State("dummy")

    # Transitions
    transision_start = Initial.to(dummy)
    transision_reinit = dummy.to(Initial)

    def __init__(self):
        self.modref = None  # Will be set later
        StateMachine.__init__(self)

    def set_modref(self, modref):
        """Explicitly assign modref after initialization."""
        self.modref = modref
        print("Module reference assigned.")

    def on_enter_Initial(self):
        """Start at Welcome screen and wait for user input."""
        print("Entered state: Initial (Welcome Screen)")

    def on_enter_dummy(self):
        print("Entered state: dummy")


class SplPlugin(SplThread):
    plugin_id = "statemachine_plugin"
    plugin_names = ["StateMachine Plugin"]

    def __init__(self, modref):
        self.modref = modref
        self.menu_storage = JsonStorage(
            self.plugin_id, "runtime", "statemachine.json", {"state_options": []}
        )
        self.lock = threading.Lock()
        super().__init__(modref.message_handler, self)
        modref.message_handler.add_event_handler(self.plugin_id, 0, self.event_listener)
        modref.message_handler.add_query_handler(self.plugin_id, 0, self.query_handler)
        self.runFlag = True

        # Initialize StateMachine without auto-starting states
        self.state_machine = Statemachine()

        # Assign modref before starting the state machine
        print("Assigning modref to StateMachine...")
        self.state_machine.set_modref(self.modref)

    def event_listener(self, queue_event):
        # print( "Event received in StatemachinePlugin:", queue_event.type, queue_event.user )

        # Handle module connection
        if queue_event.type == defaults.MSG_SYSTEM_READY:
            print(f"plugin {self.plugin_id} received system ready event")
        return queue_event

    def query_handler(self, queue_event, max_result_count):
        if queue_event.type == defaults.MSG_SYSTEM_READY:
            pass
        return []

    def plugin_run(self):
        print("StateMachine plugin running...")
        while self.runFlag:
            time.sleep(5)  # Check every 5 seconds

    def plugin_stop(self):
        print("Stopping StateMachine plugin...")
        self.runFlag = False
