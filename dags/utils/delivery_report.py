

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
