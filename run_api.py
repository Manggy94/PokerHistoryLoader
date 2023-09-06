import requests

response = requests.post("http://localhost:8000/process-files/")
print(response.json())
