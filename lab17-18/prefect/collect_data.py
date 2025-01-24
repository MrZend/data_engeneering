from kafka import KafkaProducer
import json
import requests


def fetch_photos(sol, base_url, rover, api_key):
    url = f"{base_url}/{rover}/photos"
    params = {
        'api_key': api_key,
        'sol': sol
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()['photos']
    else:
        print(f"Error: {response.status_code} on sol {sol}")
        return []


def collect_and_send_data(start_sol, end_sol, base_url, rover, api_key, producer):
    start = int(start_sol)
    end = int(end_sol)

    while start <= end:
        current_sol = start
        print(f"Fetching data for sol {current_sol}")

        photos = fetch_photos(current_sol, base_url, rover, api_key)

        for photo in photos:
            producer.send('curiosity-topic', value=photo, key=photo['id'])
            print(f"Sent: {photo}")
        print(start)
        start += 1


if __name__ == "__main__":
    producer = KafkaProducer(
                    bootstrap_servers='kafka:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8')
                    )

    API_KEY = "5TcwCaxXohcxAeUxjVDS9hEQ2BK8UHpfhEXJSOXG"
    START_SOL = 0
    END_SOL = 300
    ROVER = 'Curiosity'
    BASE_URL = 'https://api.nasa.gov/mars-photos/api/v1/rovers'

    collect_and_send_data(START_SOL, END_SOL, BASE_URL, ROVER, API_KEY, producer)

    producer.flush()
    producer.close()

