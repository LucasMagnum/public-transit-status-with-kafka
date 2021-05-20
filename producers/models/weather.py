"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests
from confluent_kafka import avro

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/weather_value.json"
    )

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        self.topic_name = "org.chicago.cta.weather.v1"
        super().__init__(
            self.topic_name,
            num_partitions=3,
            num_replicas=1,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
            self.key_schema_json = json.load(f)

        with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
            self.value_schema_json = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(
                {
                    "key_schema": json.dumps(self.key_schema_json),
                    "value_schema": json.dumps(self.value_schema_json),
                    "records": [{
                        "key": {"timestamp": self.time_millis()},
                        "value": {"temperature": self.temp, "status": self.status.name},
                    }]
                }
            ),
        )
        print(resp.content)
        resp.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
