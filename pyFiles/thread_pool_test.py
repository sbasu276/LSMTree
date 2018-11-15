from thread_pool import ThreadPool
from time import sleep

def foo(x, y):
    print("foo called...\n")
    sleep(1)
    return x*y

def bar(x, y):
    print("bar called...\n")
    sleep(1)
    return x+y

def test(x):
    print("test called...\n")
    #sleep(1)
    return x*x

if __name__ == '__main__':
    with ThreadPool(workers=10) as pool:
        f1 = pool.submit(fn=foo, args=(10, 20))
        f2 = pool.submit(fn=bar, args=("Hello", "world"))
        f3 = pool.submit(fn=test, args=(5,))

    print(f1.get(), f2.get(), f3.get())

    #with ThreadPool(workers=10) as pool:
    #    result = pool.map(fn=square, data=range(100))
    #    print("result =", result)

