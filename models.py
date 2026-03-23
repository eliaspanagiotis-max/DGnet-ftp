from typing import List, Dict


class SiteConfig:
    def __init__(self, name, host, protocol, user="", password="", path="", pattern="", frequency="daily",
                 network="NOA", rate="30s", external_clock=False, use_letter_hour=False, output_dir=None,
                 station_code="", format="Topcon", port=None):
        self.name = name
        self.host = host
        self.protocol = protocol.lower()
        self.port = port
        self.user = user
        self.password = password
        self.path = path
        self.pattern = pattern
        self.frequency = frequency.lower()
        self.network = network
        self.rate = rate
        self.external_clock = external_clock
        self.use_letter_hour = use_letter_hour
        self.output_dir = output_dir or f"./downloads/{name}"
        self.station_code = station_code
        self.format = format

    def to_dict(self):
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


class MissingFilesLog:
    def __init__(self):
        self.log: Dict[str, List[Dict]] = {}

    def clear(self):
        self.log.clear()

    def add(self, site_name: str, items: List[Dict]):
        self.log[site_name] = items
