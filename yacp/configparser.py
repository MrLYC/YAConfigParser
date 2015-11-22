#!/usr/bin/env python
# encoding: utf-8

import json
import re
from datetime import timedelta
from ConfigParser import ConfigParser as _ConfigParser
from ConfigParser import NoSectionError, NoOptionError


class DataStructureMixin(object):
    _GetDictRex = re.compile(r"\s*(?P<key>\w+)\s*:\s*(?P<val>[^,$]+?)\s*")
        
    def getdict(self, section, option, factory=None):
        result = {}
        for i in self._GetDictRex.finditer(self.get(section, option)):
            matched = i.groupdict()
            result[matched["key"]] = (
                factory(matched["val"])
                if factory else matched["val"]
            )
        return result
    
    def getlist(self, section, option, sep=","):
        return [i.strip() for i in self.get(section, option).split(sep)]

    def getjson(self, section, option):
        return json.loads(self.get(section, option))

    def gettimedelta(self, section, option):
        return timedelta(seconds=self.getint(section, option))

    def getregex(self, section, option, flags=0):
        return re.compile(self.get(section, option), flags)


class DeclareOptionMixin(object):
    def declare(self, section, option, defaults=NotImplemented):
        if self.has_option(section, option):
            return

        if defaults is NotImplemented:
            raise NoOptionError("%s::%s" % (section, option))

        if not self.has_section(section):
            self.add_section(section)
        self.set(section, option, defaults)


class ConfigParser(_ConfigParser, DataStructureMixin, DeclareOptionMixin):
    pass
