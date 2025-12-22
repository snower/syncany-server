# -*- coding: utf-8 -*-
# 2023/5/4
# create by: snower

import os
from syncany.logger import get_logger
from mysql_mimic import IdentityProvider, NativePasswordAuthPlugin, User
from syncany.taskers.config import load_config


class UserIdentityProvider(IdentityProvider):
    def __init__(self, config_path=".", username=None, password=None, is_readonly=True):
        self.config_path = config_path
        self.username = username
        self.password = password
        self.is_readonly = is_readonly
        self.users = None

    def get_plugins(self):
        return [NativePasswordAuthPlugin()]

    async def get_user(self, username):
        if self.users is None:
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
        if self.users is None:
            self.load_users()
        user = self.users.get(username)
        return user["databases"] if user and "databases" in user else None

    def is_readonly(self, username):
        if self.users is None:
            self.load_users()
        user = self.users.get(username)
        return user["readonly"] if user and "readonly" in user else self.is_readonly

    def has_permission(self, username, permission):
        if self.users is None:
            self.load_users()
        user = self.users.get(username)
        if not user:
            return False
        if permission == "temporary_memory_table":
            return (permission in user["permissions"]) if "permissions" in user else True
        return (permission in user["permissions"]) if "permissions" in user else False

    def load_users(self):
        users = {}
        for filename in (os.path.join(self.config_path, "user.json"), os.path.join(self.config_path, "user.yaml")):
            if not os.path.exists(filename):
                continue
            config = load_config(filename)
            if not config or not isinstance(config, (dict, list)):
                continue
            if isinstance(config, dict):
                config_users = config["users"] if "users" in config and isinstance(config["users"], list) else None
            else:
                config_users = config
            if not config_users:
                continue
            for user in config_users:
                if not isinstance(user, dict) or "username" not in user or "password" not in user:
                    continue
                users[user["username"]] = user
        if not users and self.username:
            users[self.username] = {"username": self.username, "password": self.password}
        self.users = users
        get_logger().info("load users finish, users: %s", ",".join(list(users.keys())))
