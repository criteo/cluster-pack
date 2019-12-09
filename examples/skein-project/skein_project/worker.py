import numpy as np


if __name__ == "__main__":
    # compute intersection of 2 arrays on the worker
    # numpy dependency and this code have been shipped by cluster-pack to the worker
    a = np.random.random_integers(0, 100, 100)
    b = np.random.random_integers(0, 100, 100)
    print("Computed intersection of two arrays:")
    print(np.intersect1d(a, b))
