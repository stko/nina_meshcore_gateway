"""Nina Mashcore Gateway
reads emergency messages from the german emergency warning system and forwards them to a mesh network
"""

import os

from was.was import Was

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-p", "--plugins", help="directory to load plugins from", default="plugins"
    )

    args = parser.parse_args()
    was = Was()
    was.run(custom_plugins_dir=[os.path.abspath(args.plugins)])
