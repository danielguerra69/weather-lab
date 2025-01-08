class FlowiseAIStorage:
    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    def send_data(self, data):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        response = requests.post(self.api_url, json=data, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to send data to FlowiseAI: {response.status_code}, {response.text}")

    def send_batch_data(self, data_list):
        for data in data_list:
            self.send_data(data)