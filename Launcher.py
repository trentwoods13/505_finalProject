import time

from PayloadGen import getpayload, init, getrandpayload
from Publisher import pub



init()

while True:

    payload = getrandpayload()
    pub(payload)
    time.sleep(2)