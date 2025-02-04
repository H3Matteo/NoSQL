import threading
import redis
from redis import ConnectionPool

pool = ConnectionPool(host='localhost', port=6379, db=0)

def worker():
    r = redis.Redis(connection_pool=pool)
    # Perform Redis operations with 'r'
    r.set('nom', 'Alice')
    nom = r.get('nom')
    print(f"Nom récupéré depuis Redis : {nom.decode('utf-8')}")

threads = [threading.Thread(target=worker) for _ in range(5)]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
