# -*- coding: utf-8 -*-
# 2023/5/4
# create by: snower

import os
import argparse
import asyncio
from .server import Server


def check_path(value):
    config_path = os.path.abspath(value)
    if not os.path.exists(config_path) or not os.path.isdir(config_path):
        raise FileNotFoundError(value)
    return config_path


def main():
    parser = argparse.ArgumentParser(
        description="Simple and easy to use SQL scripts to build virtual database tables MySQL Server"
    )
    parser.add_argument('-b', "--bind", dest='bind', default="0.0.0.0", help='bind host (default: 0.0.0.0)')
    parser.add_argument('-p', "--port", dest='port', default=3306, type=int, help='bind port (default: 3306)')
    parser.add_argument('-c', "--config_path", dest='config_path', default=".", type=check_path,
                        help='The directory where the configuration file is located, '
                             'the default current directory (default: .)')
    parser.add_argument('-U', "--username", dest='username', default="root", type=str,
                        help='Login account name, invalid when configured with user.json'
                             ' configuration file (default: root)')
    parser.add_argument('-P', "--password", dest='password', default="", type=str,
                        help='Login account password, invalid when configured with user.json'
                             ' configuration file (default: )')
    parser.add_argument('-w', "--executor_max_workers", dest='executor_max_workers', default=5, type=int,
                        help='Maximum number of worker threads for ThreadPoolExecutor executing SQL '
                             'queries (default: 5)')
    parser.add_argument('-W', "--executor_wait_timeout", dest='executor_wait_timeout', default=120, type=int,
                        help='The maximum waiting seconds time before the SQL query task is submitted to '
                             'the ThreadPoolExecutor for execution (default: 120 seconds)')
    args = parser.parse_args()
    if args.config_path:
        os.chdir(os.path.abspath(args.config_path))
    asyncio.run(Server(args.bind, args.port, os.path.abspath(args.config_path),
                       args.username, args.password,
                       args.executor_max_workers, args.executor_wait_timeout)
                .serve_forever())


if __name__ == "__main__":
    main()
