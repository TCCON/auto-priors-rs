from datetime import datetime
from time import sleep, time

def fact(n):
    total = 1
    for i in range(2, n+1):
        total *= i
    return total    


print('Will start calculating factorials in:')
for c in range(30):
    print(f'   {30-c} s')
    sleep(1)


for i in range(1, 100000):
    start = time()
    f = fact(i)
    end = time()
    print(f'{datetime.now()}: {i}! required {end - start} seconds')