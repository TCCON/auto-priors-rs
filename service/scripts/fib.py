from datetime import datetime
from time import sleep, time

min_n = 1000
max_n = 100000

def fib(n):
    a = 1
    b = 1
    for i in range(n-1):
        # if i % 100 == 0:
            # print('*', end='')
        a, b = b, a + b
    return a


def fib_sum(nmax):
    start = time()
    total = 0
    for n in range(1, nmax+1):
        total += fib(n)
    end = time()
    print(f'{datetime.now()}: Sum for {nmax} fibonacci terms required {end - start} seconds')

print('Will start calculating fibonacci sums in:')
for c in range(30):
    print(f'   {30-c} s')
    sleep(1)

for nm in range(min_n, max_n+1):
    fib_sum(nm)