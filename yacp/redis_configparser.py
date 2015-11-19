#!/usr/bin/env python
# encoding: utf-8

from collections import Mapping
from ConfigParser import ConfigParser as _ConfigParser

from yacp.configparser import (
    NoSectionError, NoOptionError,
    DataStructureMixin, DeclareOptionMixin,
)


class RedisDictMixin(object):
    def update(self, dct):
        for k, v in dct.items():
            self[k] = v

    def copy(self):
        dict_type = getattr(self, "DictType", dict)
        return dict_type(self)


class RedisHashDict(Mapping, RedisDictMixin):
    def __init__(self, connection, name, *args, **kwargs):
        self.connection = connection
        self.name = name
        super(RedisHashDict, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        self.connection.hset(self.name, key, value)

    def __getitem__(self, key):
        value = self.connection.hget(self.name, key)
        if value is None:
            raise KeyError("%s not found" % key)
        return value

    def __iter__(self):
        return iter(self.connection.hkeys(self.name))

    def __len__(self):
        return self.connection.hlen(self.name)

    def __contains__(self, key):
        return self.connection.hexists(self.name, key)

    def __delitem__(self, key):
        if self.connection.hdel(self.name, key) == 0:
            raise KeyError("%s not found" % key)

    def clear(self):
        self.connection.delete(self.name)


class RedisPrefixDict(Mapping, RedisDictMixin):
    def __init__(self, connection, prefix, *args, **kwargs):
        self.connection = connection
        self.prefix = prefix
        super(RedisPrefixDict, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        hash_dict = RedisHashDict(self.connection, self.real_key(key))
        hash_dict.update(value)

    def __getitem__(self, key):
        return RedisHashDict(self.connection, self.real_key(key))

    def __iter__(self):
        keys = [i for i in self.connection.keys(self.real_key("*"))]
        pipe = self.connection.pipeline()
        for i in keys:
            pipe.type(i)

        for t, k in zip(pipe.execute(), keys):
            if t == "hash":
                yield self.natural_key(k)

    def __len__(self):
        return len(list(iter(self)))

    def __contains__(self, key):
        for k in iter(self):
            if k == key:
                return True
        return False

    def __delitem__(self, key):
        if self.connection.delete(self.real_key(key)) == 0:
            raise KeyError("%s not found" % key)

    def natural_key(self, key):
        return key[len(self.prefix):]

    def real_key(self, key):
        return "%s%s" % (self.prefix, key)


class RedisConfigParser(_ConfigParser, DataStructureMixin, DeclareOptionMixin):
    ConfigPrefix = "__config__"

    def __init__(self, connection, *args, **kwargs):
        super(RedisConfigParser, self).__init__(*args, **kwargs)
        self._sections = RedisPrefixDict(connection, self.ConfigPrefix)
