import time
import requests
import signal

def signal_handler(sig, frame):
    print('EXITING SAFELY!')
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

data = {
  "notification_type": "OutOfRange",
  "researcher": "d.landau@uu.nl",
  "experiment_id": "5678",
  "measurement_id": "1234",
  "cipher_data": "D5qnEHeIrTYmLwYX.hSZNb3xxQ9MtGhRP7E52yv2seWo4tUxYe28ATJVHUi0J++SFyfq5LQc0sTmiS4ILiM0/YsPHgp5fQKuRuuHLSyLA1WR9YIRS6nYrokZ68u4OLC4j26JW/QpiGmAydGKPIvV2ImD8t1NOUrejbnp/cmbMDUKO1hbXGPfD7oTvvk6JQVBAxSPVB96jDv7C4sGTmuEDZPoIpojcTBFP2xA"
}

start_t = time.time()
message = requests.post( url = "http://receiver:3000/receive", json = data)
end_t = time.time()
latency = end_t - start_t

print("Message: ", message.text) 
print("Response Time: ", latency)
