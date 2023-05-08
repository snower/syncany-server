# -*- coding: utf-8 -*-
# 2023/5/4
# create by: snower

import asyncio
from .server import Server


def main():
    asyncio.run(Server().serve_forever())


if __name__ == "__main__":
    main()
