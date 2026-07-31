import os
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
from opensky_api import OpenSkyApi
from confluent_kafka import Producer

from utils.token_manager import TokenManager
from utils.delivery_report import delivery_report


def get_flights_from_iterval_send_dict_to_kafka(unix_begin: int, unix_end: int, network: str) -> int | None:
    """Get flights from interval with OpenSky api and sends it to kafka.

        Difference between begin and end is strictly bounded in 2h.

    Args:
        unix_begin (int): begin interval in second from unix start epoch.
        unix_end (int): end interval in second from unix start epoch.
        network (str): network or ip to kafka. TODO: proper way to get ip/network

    Returns:
        int | None: number of recieved records or None in the case of recieve nothing.

    Raises:
        TypeError: if the unix_begin or unix_end is not an int.
    """
    if not isinstance(unix_begin, int) or not isinstance(unix_end, int):
        raise TypeError(
            f"Expected int, but got {type(unix_begin).__name__} for begin"
            f"and {type(unix_end).__name__} for end!"
        )

    token_manager = TokenManager()
    with OpenSkyApi(token_manager=token_manager) as api:
        # The following exceptions will be processed: begin > end, auth error
        states = api.get_flights_from_interval(unix_begin, unix_end)

    # May occur in the case of no flights in interval
    if states is None:
        return None

    conf = {
        'bootstrap.servers': f'{network}:9092',  # container name in docker network
        'linger.ms': 50,
        'batch.size': 32768,
        'compression.type': 'lz4'
    }
    producer = Producer(conf)

    for i, s in enumerate(states):
        return_attributes = {
            "icao24": s.icao24,
            "first_seen": s.firstSeen,
            "est_departure_airport": s.estDepartureAirport,
            "last_seen": s.lastSeen,
            "est_arrival_airport": s.estArrivalAirport,
            "callsign": s.callsign,
            "est_departure_airport_horiz_distance": s.estDepartureAirportHorizDistance,
            "est_departure_airport_vert_distance": s.estDepartureAirportVertDistance,
            "est_arrival_airport_horiz_distance": s.estArrivalAirportHorizDistance,
            "est_arrival_airport_vert_distance": s.estArrivalAirportVertDistance,
            "departure_airport_candidates_count": s.departureAirportCandidatesCount,
            "arrival_airport_candidates_count": s.arrivalAirportCandidatesCount,
        }

        producer.produce(
            topic="raw_opensky_telemetry",
            value=json.dumps(return_attributes),
            callback=delivery_report
        )

        if i % 1000 == 0:
            producer.poll(0)

    producer.flush()

    return len(states)


if __name__ == "__main__":
    # TODO: Add logging
    parser = argparse.ArgumentParser()
    parser.add_argument('--begin', default='')
    parser.add_argument('--end', default='')
    args = parser.parse_args()

    load_dotenv()

    ip = os.getenv('KAFKA_EXTERNAL_IP')

    # 2026-07-29 20:30:00.0
    # "%d/%m/%Y, %H:%M:%S"

    get_flights_from_iterval_send_dict_to_kafka(
        int(datetime.strptime(args.begin, "%Y-%m-%d %H:%M:%S").timestamp()),
        int(datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S").timestamp()),
        ip
    )
