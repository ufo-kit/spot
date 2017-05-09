import requests


class Spot(object):
    def __init__(self, host='http://localhost:5000'):
        self.host = host

    def url(self, *rest):
        return self.host + '/' + '/'.join(rest)

    def submit_runner(self, data):
        r = requests.post(self.url('api/workflows'), json=data)

    def submit_fact(self, runner, fact):
        r = requests.post(self.url('api/workflows', runner.uid, 'facts'), json=fact.to_dict())
