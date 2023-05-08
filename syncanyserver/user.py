# -*- coding: utf-8 -*-
# 2023/5/4
# create by: snower

import os
from mysql_mimic import IdentityProvider, NativePasswordAuthPlugin, User
from syncany.taskers.config import load_config


class UserIdentityProvider(IdentityProvider):
    def __init__(self):
        self.users = None

    def get_plugins(self):
        return [NativePasswordAuthPlugin()]

    async def get_user(self, username):
        self.load_users()

        user = self.users.get(username)
        if user:
            return User(
                name=username,
                auth_string=NativePasswordAuthPlugin.create_auth_string(user["password"]),
                auth_plugin=NativePasswordAuthPlugin.name,
            )
        return None

    async def get_databases(self, username):
        self.load_users()

        user = self.users.get(username)
        return user["databases"] if user and "databases" in user else None

    def load_users(self):
        if self.users is not None:
            return
        self.users = {}
        for filename in ("user.json", "user.yaml"):
            if not os.path.exists(filename):
                continue
            config = load_config(filename)
            if not config or "users" not in config or not isinstance(config["users"], list):
                continue
            for user in config["users"]:
                if "username" not in user or "password" not in user:
                    continue
                self.users[user["username"]] = user
