"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str



app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("org.chicago.cta.kafka_connect.stations", value_type=Station)

out_topic = app.topic("org.chicago.cta.stations.table", value_type=TransformedStation, partitions=1)

table = app.Table(
   #"faust_stations",
   "org.chicago.cta.stations.table.faust",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def transformed_station(stations):
    async for station in stations:
        station_color = ""
        if station.red:
            station_color = "red"
        elif station.blue:
            station_color = "blue"
        elif station.green:
            station_color = "green"
        
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station_color
        )
        #await out_topic.send(key=station.station_name, value=transformed_station)
        table[station.station_id] = transformed_station




if __name__ == "__main__":
    app.main()
