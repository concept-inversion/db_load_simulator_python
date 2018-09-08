from locust import HttpLocust, TaskSet

def login(insta):
    pass

def logout():
    pass

def home(insta):
    insta.client.get("http://127.0.0.1:8000/")

def profile(insta):
    insta.client.get("http://127.0.0.1:8000/")

class patterns(TaskSet):
    tasks = {home:2, profile: 1}

    def starting(self):
        # login
        pass

    def ending(self):
        # logout at the end automatically
        pass

class WebsiteUser(HttpLocust):
    task_set=patterns
    min_wait = 5000
    max_wait = 9000