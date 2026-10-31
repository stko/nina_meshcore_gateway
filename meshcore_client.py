"""
MeshCore Client Module
Handles async serial communication with MeshCore devices (e.g., WIO Pro P1 Tracker).
Uses the meshcore Python library for the companion radio protocol.


This source code is licensed under the MIT License.,
originally copied from https://github.com/gunpowder100/openclaw-meshcore/blob/main/meshcore_client.py
but it seems as if that sample used an older meshcore interface,
so these calls are an ongoing re-write

"""

import asyncio
import logging
from typing import Callable, Optional, List
from dataclasses import dataclass, field
from datetime import datetime

from meshcore import MeshCore
from meshcore.events import EventType

logger = logging.getLogger("nmg.meshcore")


@dataclass
class MeshMessage:
    """Represents an incoming or outgoing mesh message."""

    sender_id: str
    sender_name: str
    text: str
    timestamp: datetime = field(default_factory=datetime.now)
    is_group: bool = False
    channel: str = ""
    contact: object = None  # Reference to meshcore Contact object


# MeshCore text message limit
MESHCORE_CHAR_LIMIT = 133


def chunk_message(text: str, limit: int = MESHCORE_CHAR_LIMIT) -> List[str]:
    """
    Split a long AI response into MeshCore-compatible chunks.
    Tries to split on sentence boundaries, then word boundaries.
    Each chunk is prefixed with [n/N] if multiple chunks.
    """
    text = text.strip()
    if len(text) <= limit:
        return [text]

    # Reserve space for chunk prefix like "[1/3] "
    chunks = []
    # First pass: estimate chunk count for prefix sizing
    est_chunks = (len(text) // (limit - 8)) + 1
    prefix_len = len(f"[{est_chunks}/{est_chunks}] ")
    effective_limit = limit - prefix_len

    remaining = text
    while remaining:
        if len(remaining) <= effective_limit:
            chunks.append(remaining)
            break

        # Try to split at sentence boundary
        split_pos = effective_limit
        for sep in [". ", "! ", "? ", "\n"]:
            pos = remaining[:effective_limit].rfind(sep)
            if pos > effective_limit // 3:  # Don't split too early
                split_pos = pos + len(sep)
                break
        else:
            # Fall back to word boundary
            pos = remaining[:effective_limit].rfind(" ")
            if pos > effective_limit // 3:
                split_pos = pos + 1

        chunks.append(remaining[:split_pos].rstrip())
        remaining = remaining[split_pos:].lstrip()

    # Add chunk prefixes
    if len(chunks) > 1:
        total = len(chunks)
        chunks = [f"[{i+1}/{total}] {c}" for i, c in enumerate(chunks)]

    return chunks


class MeshCoreClient:
    """
    Async client for MeshCore companion radio protocol.
    Wraps the meshcore Python library for serial/BLE/TCP connections.
    """

    def __init__(
        self,
        port: str = "/dev/ttyACM0",
        baud: int = 115200,
        debug: bool = False,
        redact_logs: bool = True,
    ):
        self.port = port
        self.baud = baud
        self.debug = debug
        self.redact_logs = redact_logs
        self._mc: Optional[MeshCore] = None
        self._running = False
        self._on_message: Optional[Callable] = None
        self._contacts_cache: dict = {}

    async def connect(self):
        """Establish serial connection to MeshCore device."""
        try:

            logger.info(
                "Connecting to MeshCore device on %s @ %d...", self.port, self.port
            )
            self._mc = await MeshCore.create_serial(
                self.port, self.baud, debug=self.debug
            )
            logger.info("MeshCore serial connection established.")

            # Fetch initial contacts
            await self._refresh_contacts()

            return True

        except Exception:
            logger.exception("Failed to connect to MeshCore device:")
            raise

    async def _refresh_contacts(self):
        """Refresh the local contacts cache from the device."""
        try:
            if not self._mc:
                raise RuntimeError("Not connected. Call connect() first.")
            result = await self._mc.commands.get_contacts()
            if result.type == EventType.ERROR:
                print(f"Error getting contacts: {result.payload}")
                return

            # ok = await self._mc.ensure_contacts()
            contacts = result.payload or {}
            self._contacts_cache = {}
            for key, contact in contacts.items():
                pk = contact.get("public_key", key)
                prefix = pk[:12]
                self._contacts_cache[prefix] = contact
                self._contacts_cache[pk] = contact
                name = contact.get("adv_name", "")
                if name:
                    self._contacts_cache[f"name:{name}"] = contact
            logger.info("Loaded %d contacts.", len(contacts))

        except Exception:
            logger.exception("Could not refresh contacts:")

    def on_message(self, callback: Callable):
        """Register a callback for incoming mesh messages: async def handler(msg: MeshMessage)"""
        self._on_message = callback

    def _resolve_sender(self, pubkey_prefix: str):
        """Look up a contact by pubkey_prefix, return (contact_dict, display_name)."""
        contact = self._contacts_cache.get(pubkey_prefix, None)
        if contact:
            name = contact.get("adv_name", "") or pubkey_prefix[:4]
            return contact, name
        for key, c in self._contacts_cache.items():
            if key.startswith(pubkey_prefix) or pubkey_prefix.startswith(key[:12]):
                name = c.get("adv_name", "") or pubkey_prefix[:4]
                return c, name
        return None, pubkey_prefix[:4]

    async def _handle_contact_msg(self, event):
        print("msg")
        try:
            payload = event.payload
            pubkey_prefix = payload.get("pubkey_prefix", "unknown")
            text = payload.get("text", "")
            contact, sender_name = self._resolve_sender(pubkey_prefix)
            msg = MeshMessage(
                sender_id=pubkey_prefix,
                sender_name=sender_name,
                text=text,
                is_group=False,
                channel="",
                contact=contact,
            )
            if self.redact_logs:
                logger.info(
                    "[RX DM] %s (%s): <redacted>", msg.sender_name, msg.sender_id
                )
            else:
                logger.info(
                    "[RX DM] %s (%s): %s", msg.sender_name, msg.sender_id, msg.text
                )
            if self._on_message:
                await self._on_message(msg)
        except Exception:
            logger.exception("Error handling contact message:")

    async def _handle_channel_msg(self, event):
        try:
            payload = event.payload
            channel_idx = payload.get("channel_idx", 0)
            text = payload.get("text", "")
            pubkey_prefix = payload.get("pubkey_prefix", "channel")
            contact, sender_name = self._resolve_sender(pubkey_prefix)
            msg = MeshMessage(
                sender_id=pubkey_prefix,
                sender_name=sender_name,
                text=text,
                is_group=True,
                channel=str(channel_idx),
                contact=contact,
            )
            if self.redact_logs:
                logger.info("[RX CH%s] %s: <redacted>", channel_idx, msg.sender_name)
            else:
                logger.info("[RX CH%s] %s: %s", channel_idx, msg.sender_name, msg.text)
            if self._on_message:
                await self._on_message(msg)
        except Exception:
            logger.exception("Error handling channel message:")

    async def start_listening(self):
        """Start the event loop that listens for incoming messages."""

        if not self._mc:
            raise RuntimeError("Not connected. Call connect() first.")

        self._running = True
        logger.info("Listening for incoming MeshCore messages...")

        self._mc.subscribe(EventType.CONTACT_MSG_RECV, self._handle_contact_msg)
        self._mc.subscribe(EventType.CHANNEL_MSG_RECV, self._handle_channel_msg)

        await self._mc.start_auto_message_fetching()
        logger.info("Auto message fetching started.")

        while self._running:
            await asyncio.sleep(0.1)

    async def send_message(self, contact, text: str) -> bool:
        """
        Send a text message to a contact dict or pubkey string.
        Automatically chunks if the text exceeds 133 chars.
        Returns True if all chunks sent successfully.
        """
        if not self._mc:
            raise RuntimeError("Not connected. Call connect() first.")

        dst = contact
        if isinstance(contact, dict):
            dst = contact

        chunks = chunk_message(text)
        logger.info("[TX] Sending %d chunk(s)...", len(chunks))

        for i, chunk in enumerate(chunks):
            try:
                result = await self._mc.commands.send_msg(dst, chunk)
                if result.type == EventType.ERROR:
                    logger.error("Failed to send chunk %d: %s", i + 1, result.payload)
                    return False
                """
                expected_ack = result.payload["expected_ack"].hex()
                print(f"Message sent, waiting for ack with code: {expected_ack}")

                # Wait specifically for this acknowledgment
                result = await self._mc.wait_for_event(
                    EventType.ACK,
                    attribute_filters={"code": expected_ack},
                    timeout=10.0,
                )

                if not result:
                    print("Message delivery confirmation timed out")
                    return False
                print("Message confirmed delivered!")
                """
                print("[TX] Chunk %d/%d sent.", i + 1, len(chunks))
                logger.debug("[TX] Chunk %d/%d sent.", i + 1, len(chunks))
                if i < len(chunks) - 1:
                    await asyncio.sleep(3)
                    pass
            except Exception:
                logger.exception("Error sending chunk %d:", i + 1)
                return False

        return True

    async def send_channel_message(self, channel_idx: int, text: str) -> bool:
        """
        Send a text message to a channel by its index.
        Automatically chunks if the text exceeds 133 chars.
        Returns True if all chunks sent successfully.
        """

        if not self._mc:
            raise RuntimeError("Not connected. Call connect() first.")

        chunks = chunk_message(text)
        logger.info("[TX CH%s] Sending %d chunk(s)...", channel_idx, len(chunks))

        for i, chunk in enumerate(chunks):
            try:
                result = await self._mc.commands.send_chan_msg(channel_idx, chunk)
                if result.type == EventType.ERROR:
                    logger.error(
                        "Failed to send channel chunk %d: %s", i + 1, result.payload
                    )
                    return False
                logger.debug(
                    "[TX CH%d] Chunk %d/%d sent.", channel_idx, i + 1, len(chunks)
                )
                if i < len(chunks) - 1:
                    await asyncio.sleep(1.5)
            except Exception:
                logger.exception("Error sending channel chunk %d: ", i + 1)
                return False

        return True

    async def get_device_info(self) -> dict:
        """Get basic device info from the MeshCore radio."""
        if not self._mc:
            return {}
        result = await self._mc.commands.send_device_query()
        if result.type == EventType.ERROR:
            pass
        else:
            if hasattr(result, "payload"):
                return {"self": str(result.payload)}
        return {}

    async def disconnect(self):
        """Gracefully disconnect from the MeshCore device."""
        self._running = False
        if self._mc:
            try:
                await self._mc.disconnect()
                logger.info("Disconnected from MeshCore device.")
            except Exception as e:
                logger.warning("Error during disconnect: %s", e)
            self._mc = None


async def msg_handler(msg: MeshMessage):
    print(msg)


async def init_main(mc):
    mc.on_message(msg_handler)
    await mc.connect()
    device_info = await mc.get_device_info()
    print(device_info)
    # asyncio.create_task(mc.start_listening())
    # mc._mc.subscribe(EventType.CONTACT_MSG_RECV, mc._handle_contact_msg)
    # mc._mc.subscribe(EventType.CHANNEL_MSG_RECV, mc._handle_channel_msg)


async def main(mc):
    mc.on_message(msg_handler)
    await mc.connect()
    device_info = await mc.get_device_info()
    print(device_info)
    print("start to send")
    await mc.send_message(
        "6d1d1956a7f193aaa789ad5847b2565add244636af38e2ed49c72cd57343ed99",
        "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.",
    )
    print("sleep")
    await asyncio.sleep(20)
    print("disconnect")
    await mc.disconnect()
    print("end main")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-p",
        "--port",
        help="port where the device is connected to",
        default="/dev/ttyACM0",
    )

    parser.add_argument(
        "-b", "--baudrate", help="baudrate of the serial connection", default=115200
    )

    parser.add_argument(
        "-d", "--debug", help="enable debug outputs", default=False, action="store_true"
    )

    parser.add_argument(
        "-r", "--reduct", help="reduct logs", default=False, action="store_true"
    )

    args = parser.parse_args()
    mc = MeshCoreClient(args.port, args.baudrate, args.debug, args.reduct)
    # print("start")
    # asyncio.run(init_main(mc))
    print("run main")
    asyncio.run(main(mc))
    print("end")
