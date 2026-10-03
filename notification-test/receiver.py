from flask import Flask, request
import requests
import signal

def signal_handler(sig, frame):
    print("EXITING SAFELY!")
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

app = Flask(__name__)

@app.route("/receive", methods = ["POST"])
def receive():
    data = request.json

    required_fields = [
        "notification_type",
        "researcher",
        "experiment_id",
        "measurement_id",
        "cipher_data"
    ]
    missing = set(required_fields) - data.keys()

    if missing:
        return f"Missing fields: {missing}"
    elif( data["notification_type"]=="OutOfRange" or data["notification_type"]=="Stabilized"):
        notification_data = {
            "notification_type": data["notification_type"],
            "researcher": data["researcher"],
            "experiment_id": data["experiment_id"],
            "measurement_id": data["measurement_id"],
            "cipher_data": data["cipher_data"]
        }
        print(notification_data)

        ntf_url = 'http://notifications-service:3000/api/notify'

        response = requests.post(url = ntf_url, json = notification_data)

        if response.status_code == 200:
           print("Notification Sent!")
           print("Payload:", response.text)
           return "OK", 200
        else:
           return f"{response.text}"
    else:
        return f"Bad Request", 400

app.run(host = "0.0.0.0", port = 3000) 
